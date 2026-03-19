from typing import Any, Dict, List, Optional
from src.api.services.unphu_api import UnphuApiService
from src.api.services.student_transformer import calculate_credits_evaluated, parse_prerequisites, build_history_by_period, deduplicate_history
from src.db.data_warehouse import DataWareHouseSync
from src.db.connection import DBConnection
import re

class StudentSyncService:
    """
    Servicio centralizado para orquestar la extracción de datos desde la API de la UNPHU
    y su posterior sincronización con el Data Warehouse (PostgreSQL).
    """

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
                id_persona_match = re.search(r'\d+', matricula.replace('-',''))
                id_persona = int(id_persona_match.group()) if id_persona_match else 0

            nombre_final = nombre_google or data_est.get('names') or 'Estudiante'
            api_data['matricula'] = data_est.get('username')
            api_data['carrera'] = data_est.get('career')
            api_data['nombre'] = nombre_final
            
            # 2. Carrera(s)
            careers = UnphuApiService.get_student_careers(str(id_persona))
            # Usamos la primera carrera como la "activa" para temas de UI, pero procesamos todas para el historial
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
                        # Filtrar materias basura: INF-158-L y periodos fantasmas como '99'
                        # El usuario indica que la carrera solo llega al 12 y INF-158-L no va.
                        hist = [
                            h for h in hist 
                            if str(h.get('codeSubject', '')).strip() != 'INF-158-L' 
                            and str(h.get('semester', '')).strip() != '99'
                        ]
                        all_history.extend(hist)
            
            # Deduplicar historial para evitar contar doble materias convalidadas o repetidas
            historial = deduplicate_history(all_history)
            
            if len(historial) > 0:
                # Créditos evaluados = TODOS los intentos (incluyendo F/FI repeats)
                # Se calcula sobre all_history (pre-dedup) para reflejar todos los créditos
                # que el estudiante cursó y fueron evaluados académicamente.
                creditos_evaluados = calculate_credits_evaluated(all_history)
                
                # Extracción robusta de créditos
                max_approved = 0
                max_pensum = 0
                try:
                    def safe_get_int(d, key):
                        v = d.get(key, 0)
                        try: return int(float(v)) if v is not None else 0
                        except: return 0
                        
                    # Sumamos creditos aprobados de la lista deduplicada
                    for h in historial:
                        let = str(h.get('lyrics', '')).strip()
                        if let in ['A', 'B', 'C', 'D', 'EX', 'AP']:
                            max_approved += safe_get_int(h, 'credits')
                    
                    # El total del pensum sí suele ser un valor informativo en cada registro, tomamos el mayor
                    max_pensum = max((safe_get_int(h, 'pensumCredit') for h in historial), default=0)
                except: pass

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

            # 4. Índices: recorrer TODOS los períodos históricos para construir el historial real
            real_index = 0.0
            index_history: list = []  # Lista: [{label, cumulativeIndex, semesterIndex}]
            
            try:
                # Etiquetas de período UNPHU: 1=ENE-ABR, 2=MAY-AGO, 3=SEP-DIC
                PERIOD_LABELS = {1: 'ENE-ABR', 2: 'MAY-AGO', 3: 'SEP-DIC'}
                all_periods_to_check = [
                    (yr, per)
                    for yr in range(2020, 2027)
                    for per in [1, 2, 3]
                ]
                
                seen_labels = set()
                for yr, per in all_periods_to_check:
                    for car in careers:
                        c_id = str(car.get('IdCarrera'))
                        grades_list = UnphuApiService.get_semester_grades(yr, per, str(id_persona), c_id)
                        if grades_list and len(grades_list) > 0:
                            val_cum = grades_list[0].get('cumulativeIndex')
                            val_sem = grades_list[0].get('semesterIndex') or grades_list[0].get('periodIndex')
                            if val_cum and float(val_cum) > 0.1:
                                label = f"{PERIOD_LABELS.get(per, 'PER-' + str(per))}-{yr}"
                                if label not in seen_labels:
                                    seen_labels.add(label)
                                    cum_f = float(val_cum)
                                    sem_f = float(val_sem) if val_sem else cum_f
                                    index_history.append({
                                        'label': label,
                                        'cumulativeIndex': round(cum_f, 2),
                                        'semesterIndex': round(sem_f, 2),
                                        'year': yr,
                                        'period': per
                                    })
                                    real_index = cum_f
                                break
                
                print(f"[SYNC] Index history for {matricula}: {len(index_history)} periods. Latest: {real_index}")
                
                # Fallback a DB si no se obtuvo nada de la API
                if real_index <= 0:
                    conn = DBConnection.get_connection()
                    if conn:
                        try:
                            cursor = conn.cursor()
                            id_p_num = int(re.search(r'\d+', matricula.replace('-','')).group()) if re.search(r'\d+', matricula.replace('-','')).group() else 0
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
            except Exception as e_ind:
                print(f"[SYNC] Error extracting indices for {matricula}: {e_ind}")
            
            api_data['index_history'] = index_history  # Historial real por periodo para la gráfica
            
            # semesterIndex = GPA del ÚLTIMO período (no el acumulado)
            # cumulativeIndex = GPA acumulado total
            last_sem_index = index_history[-1]['semesterIndex'] if index_history else real_index
            api_data['indices'] = {'semesterIndex': round(last_sem_index, 2), 'cumulativeIndex': round(real_index, 2)}


            # 5. Periodo Actual y Selección
            periodo_actual = UnphuApiService.get_current_period()
            ano_actual = 2026
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
