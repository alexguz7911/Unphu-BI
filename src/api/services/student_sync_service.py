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
                        all_history.extend(hist)
            
            # Deduplicar historial para evitar contar doble materias convalidadas o repetidas
            historial = deduplicate_history(all_history)
            
            if len(historial) > 0:
                creditos_evaluados = calculate_credits_evaluated(historial)
                
                # Extracción robusta de créditos
                max_approved = 0
                max_pensum = 0
                try:
                    def safe_get_int(d, key):
                        v = d.get(key, 0)
                        try: return int(float(v)) if v is not None else 0
                        except: return 0
                        
                    # Sumamos creditos aprobados de la lista deduplicada
                    # No usamos MAX porque queremos el total real de materias con nota literal válida
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

            # 4. Índices (Búsqueda exhaustiva del índice más reciente)
            real_index = 0.0
            try:
                # Intentamos en varios periodos recientes y para todas las carreras
                # Buscamos de lo más nuevo a lo más antiguo. En cuanto encontramos un índice válido, PARAMOS.
                periods_to_check = [(2026, 1), (2025, 3), (2025, 2)]
                
                found = False
                for yr, per in periods_to_check:
                    if found: break
                    for car in careers:
                        c_id = str(car.get('IdCarrera'))
                        grades_list = UnphuApiService.get_semester_grades(yr, per, str(id_persona), c_id)
                        if grades_list and len(grades_list) > 0:
                            val = grades_list[0].get('cumulativeIndex')
                            if val and float(val) > 0.1:
                                real_index = float(val)
                                found = True
                                break
                
                # Si sigue en 0, intentamos fallback a DB local (DW) - Solo el más reciente
                if real_index <= 0:
                    conn = DBConnection.get_connection()
                    if conn:
                        try:
                            cursor = conn.cursor()
                            id_p_num = int(re.search(r'\d+', matricula.replace('-','')).group()) if re.search(r'\d+', matricula.replace('-','')).group() else 0
                            # Obtenemos el índice del periodo más reciente registrado en DB
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
            
            api_data['indices'] = {'semesterIndex': real_index, 'cumulativeIndex': real_index}

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
