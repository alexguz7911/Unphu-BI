from typing import Any, Dict, List, Optional
from src.api.services.unphu_api import UnphuApiService
from src.api.services.student_transformer import calculate_credits_evaluated, parse_prerequisites, build_history_by_period, deduplicate_history
from src.db.data_warehouse import DataWareHouseSync
from src.db.connection import DBConnection
from concurrent.futures import ThreadPoolExecutor, as_completed, TimeoutError as FuturesTimeoutError
import re
import datetime

PERIOD_LABELS = {1: 'ENE-ABR', 2: 'MAY-AGO', 3: 'SEP-DIC'}

class StudentSyncService:
    """
    Servicio centralizado para orquestar la extracción de datos desde la API de la UNPHU
    y su posterior sincronización con el Data Warehouse (PostgreSQL).
    """

    @staticmethod
    def _get_enrollment_year(matricula: str) -> int:
        """Extrae el año de ingreso desde la matrícula. Ej: 'aj20-1205' -> 2020"""
        try:
            m = re.search(r'[a-zA-Z]+(\d{2})-', matricula)
            if m:
                return 2000 + int(m.group(1))
        except Exception:
            pass
        return 2020

    @staticmethod
    def _fetch_index_history_parallel(careers: list, id_persona: str, matricula: str) -> list:
        """
        Obtiene el historial de índices por período usando llamadas paralelas.
        Drásticamente más rápido que el enfoque secuencial anterior (60+ llamadas → ~5s).
        """
        start_year = StudentSyncService._get_enrollment_year(matricula)
        current_year = datetime.datetime.now().year

        all_periods = [
            (yr, per)
            for yr in range(start_year, current_year + 1)
            for per in [1, 2, 3]
        ]

        def fetch_one(yr: int, per: int, car_id: str):
            try:
                grades = UnphuApiService.get_semester_grades(yr, per, str(id_persona), car_id)
                if grades:
                    cum = grades[0].get('cumulativeIndex')
                    if cum and float(cum) > 0.1:
                        return (yr, per, grades[0])
            except Exception:
                pass
            return None

        seen_labels = set()
        index_history = []

        with ThreadPoolExecutor(max_workers=20) as executor:
            futures = {
                executor.submit(fetch_one, yr, per, str(car.get('IdCarrera'))): (yr, per)
                for yr, per in all_periods
                for car in careers
            }

            try:
                for future in as_completed(futures, timeout=30):
                    result = future.result()
                    if result:
                        yr, per, grade_data = result
                        label = f"{PERIOD_LABELS.get(per, f'PER-{per}')}-{yr}"
                        if label not in seen_labels:
                            seen_labels.add(label)
                            cum_f = float(grade_data.get('cumulativeIndex', 0))
                            sem_raw = grade_data.get('semesterIndex') or grade_data.get('periodIndex')
                            sem_f = float(sem_raw) if sem_raw else cum_f
                            index_history.append({
                                'label': label,
                                'cumulativeIndex': round(cum_f, 2),
                                'semesterIndex': round(sem_f, 2),
                                'year': yr,
                                'period': per
                            })
            except FuturesTimeoutError:
                print(f"[SYNC] Timeout parcial al obtener historial de índices para {matricula}. Se usan los datos disponibles.")

        # Ordenar cronológicamente
        index_history.sort(key=lambda x: (x['year'], x['period']))
        return index_history

    @staticmethod
    def fetch_and_sync_all(matricula: str, nombre_google: str = None) -> Dict[str, Any]:
        """
        Extrae toda la información disponible para un estudiante y la guarda en el DW.
        Retorna el objeto api_data listo para ser usado por el frontend.
        """
        try:
            api_data: Dict[str, Any] = {}

            # 1. Datos Básicos del Estudiante
            data_est = UnphuApiService.get_student_data(matricula)
            if not data_est:
                print(f"[SYNC] No data found for student {matricula} in API")
                return {}

            id_persona = data_est.get('id')
            if not id_persona:
                id_persona_match = re.search(r'\d+', matricula.replace('-', ''))
                id_persona = int(id_persona_match.group()) if id_persona_match else 0

            nombre_final = nombre_google or data_est.get('names') or 'Estudiante'
            api_data['matricula'] = data_est.get('username')
            api_data['carrera'] = data_est.get('career')
            api_data['nombre'] = nombre_final

            # 2. Carrera(s)
            careers = UnphuApiService.get_student_careers(str(id_persona))
            active_career = careers[0] if careers else {}
            id_carrera = str(active_career.get('IdCarrera', '0'))
            api_data['id_carrera'] = id_carrera

            # 3. Historial (Consolidado de todas las carreras/pensa)
            all_history = []
            for car in careers:
                car_id = str(car.get('IdCarrera'))
                if car_id:
                    hist = UnphuApiService.get_pending_grades(str(id_persona), car_id)
                    if hist:
                        hist = [
                            h for h in hist
                            if str(h.get('codeSubject', '')).strip() != 'INF-158-L'
                            and str(h.get('semester', '')).strip() != '99'
                        ]
                        all_history.extend(hist)

            # Deduplicar historial
            historial = deduplicate_history(all_history)

            if len(historial) > 0:
                creditos_evaluados = calculate_credits_evaluated(all_history)

                max_approved = 0
                max_pensum = 0
                try:
                    def safe_get_int(d, key):
                        v = d.get(key, 0)
                        try:
                            return int(float(v)) if v is not None else 0
                        except Exception:
                            return 0

                    for h in historial:
                        let = str(h.get('lyrics', '')).strip()
                        if let in ['A', 'B', 'C', 'D', 'EX', 'AP']:
                            max_approved += safe_get_int(h, 'credits')

                    max_pensum = max((safe_get_int(h, 'pensumCredit') for h in historial), default=0)
                except Exception:
                    pass

                api_data['stats'] = {
                    'creditosAprobados': max_approved,
                    'creditosEvaluados': creditos_evaluados,
                    'totalCreditos': max_pensum,
                    'materiasAprobadas': max_approved // 3 if max_approved else 0
                }

                pending_list = [
                    h for h in historial
                    if not (str(h.get('lyrics', '')).strip() or
                            str(h.get('number', '')).strip() or
                            str(h.get('observations', '')).strip())
                ]

                api_data['pending_subjects'] = parse_prerequisites(pending_list)
                api_data['history'] = build_history_by_period(historial)

            # 4. Índices: obtener historial real por período (EN PARALELO)
            index_history = StudentSyncService._fetch_index_history_parallel(
                careers, str(id_persona), matricula
            )

            real_index = index_history[-1]['cumulativeIndex'] if index_history else 0.0

            print(f"[SYNC] Index history for {matricula}: {len(index_history)} periods. Latest: {real_index}")

            # Fallback a DB si no se obtuvo nada de la API
            if real_index <= 0:
                conn = DBConnection.get_connection()
                if conn:
                    try:
                        cursor = conn.cursor()
                        id_p_num = int(re.search(r'\d+', matricula.replace('-', '')).group()) if re.search(r'\d+', matricula.replace('-', '')) else 0
                        cursor.execute("""
                            SELECT IndiceAcumulado
                            FROM Fact_Calificaciones
                            WHERE IdPersona = %s
                            ORDER BY IdPeriodo DESC
                            LIMIT 1
                        """, (id_p_num,))
                        row = cursor.fetchone()
                        if row and row[0] is not None:
                            real_index = float(row[0])
                    except Exception as db_e:
                        print(f"[SYNC] Error fallback DB index {matricula}: {db_e}")
                    finally:
                        conn.close()

            api_data['index_history'] = index_history

            last_sem_index = index_history[-1]['semesterIndex'] if index_history else real_index
            api_data['indices'] = {
                'semesterIndex': round(last_sem_index, 2),
                'cumulativeIndex': round(real_index, 2)
            }

            # 5. Período Actual y Selección
            periodo_actual = UnphuApiService.get_current_period()
            ano_actual = datetime.datetime.now().year
            num_periodo = 1

            enrolled = UnphuApiService.get_officially_enrolled(ano_actual, num_periodo, str(id_persona), id_carrera)
            selected = UnphuApiService.get_unofficial_selected(ano_actual, num_periodo, str(id_persona), id_carrera)

            api_data['current_period'] = [periodo_actual] if periodo_actual else []
            api_data['registered_subjects'] = enrolled
            api_data['selected_subjects'] = selected

            # 6. SINCRONIZACIÓN CON DATA WAREHOUSE (DW)
            try:
                DataWareHouseSync.sync_student_login(api_data, matricula, nombre_final, id_carrera)
            except Exception as sync_e:
                print(f"[SYNC] Non-critical error in sync_student_login for {matricula}: {sync_e}")

            return api_data

        except Exception as global_e:
            import traceback
            print(f"[SYNC] CRITICAL ERROR in fetch_and_sync_all for {matricula}:")
            traceback.print_exc()
            return api_data if 'api_data' in locals() and api_data else {}
