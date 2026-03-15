from typing import Any, Dict, List, Optional
from src.api.services.unphu_api import UnphuApiService
from src.api.services.student_transformer import calculate_credits_evaluated, parse_prerequisites, build_history_by_period
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
        api_data: Dict[str, Any] = {}
        
        # 1. Datos Básicos del Estudiante
        data_est = UnphuApiService.get_student_data(matricula)
        if not data_est:
            return {}

        id_persona = data_est.get('id')
        if not id_persona:
            id_persona_match = re.search(r'\d+', matricula.replace('-',''))
            id_persona = int(id_persona_match.group()) if id_persona_match else 0

        nombre_final = nombre_google or data_est.get('names') or 'Estudiante'
        api_data['matricula'] = data_est.get('username')
        api_data['carrera'] = data_est.get('career')
        api_data['nombre'] = nombre_final
        api_data['id_carrera'] = str(id_carrera) if id_carrera else "0"

        # 2. Carrera
        data_car = UnphuApiService.get_student_career(str(id_persona))
        id_carrera = data_car.get('IdCarrera') if data_car else None

        # 3. Historial (Pending Grades contiene todo el historial en el endpoint de la UNPHU)
        historial = UnphuApiService.get_pending_grades(str(id_persona), str(id_carrera))
        if len(historial) > 0:
            creditos_evaluados = calculate_credits_evaluated(historial)
            max_approved = max((int(h.get('approved', 0)) if isinstance(h.get('approved'), (int, str)) and str(h.get('approved')).isdigit() else 0 for h in historial), default=0)
            max_pensum = max((int(h.get('pensumCredit', 0)) if isinstance(h.get('pensumCredit'), (int, str)) and str(h.get('pensumCredit')).isdigit() else 0 for h in historial), default=0)
            
            api_data['stats'] = {
                'creditosAprobados': max_approved,
                'creditosEvaluados': creditos_evaluados,
                'totalCreditos': max_pensum,
                'materiasAprobadas': max_approved // 3
            }
            
            pending_list = [
                h for h in historial 
                if not (str(h.get('lyrics', '')).strip() or 
                        str(h.get('number', '')).strip() or 
                        str(h.get('observations', '')).strip())
            ]
            
            api_data['pending_subjects'] = parse_prerequisites(pending_list)
            api_data['history'] = build_history_by_period(historial)

        # 4. Índices
        # Usamos el 2025-3 como referencia para obtener el índice acumulado más reciente de la API
        grades_list = UnphuApiService.get_semester_grades(2025, 3, str(id_persona), str(id_carrera))
        real_index = 0
        if len(grades_list) > 0 and grades_list[0].get('cumulativeIndex', 0) > 0:
            real_index = float(grades_list[0].get('cumulativeIndex', 0))
        else:
            # Fallback a DB local (DW) si la API no devuelve índice
            conn = DBConnection.get_connection()
            if conn:
                try:
                    cursor = conn.cursor()
                    id_p_num = int(re.search(r'\d+', matricula.replace('-','')).group()) if re.search(r'\d+', matricula.replace('-','')) else 0
                    cursor.execute("SELECT MAX(IndiceAcumulado) FROM Fact_Calificaciones WHERE IdPersona = %s", (id_p_num,))
                    row = cursor.fetchone()
                    if row and row[0] is not None:
                        real_index = float(row[0])
                except Exception as e:
                    print(f"Error reading index from DB fallback for {matricula}:", e)
                finally:
                    conn.close()
        
        api_data['indices'] = {'semesterIndex': real_index, 'cumulativeIndex': real_index}

        # 5. Periodo Actual y Selección
        periodo_actual = UnphuApiService.get_current_period()
        ano_actual = 2026
        num_periodo = 1
        
        enrolled = UnphuApiService.get_officially_enrolled(ano_actual, num_periodo, str(id_persona), str(id_carrera))
        selected = UnphuApiService.get_unofficial_selected(ano_actual, num_periodo, str(id_persona), str(id_carrera))
        
        api_data['current_period'] = [periodo_actual] if periodo_actual else []
        api_data['registered_subjects'] = enrolled
        api_data['selected_subjects'] = selected

        # 6. SINCRONIZACIÓN CON DATA WAREHOUSE (DW)
        DataWareHouseSync.sync_student_login(api_data, matricula, nombre_final, str(id_carrera))

        return api_data
