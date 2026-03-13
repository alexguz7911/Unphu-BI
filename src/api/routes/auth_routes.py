from flask import Blueprint, request, jsonify # type: ignore
from google.oauth2 import id_token # type: ignore
from google.auth.transport import requests as google_requests # type: ignore
from src.config.settings import GOOGLE_CLIENT_ID
from src.api.services.unphu_api import UnphuApiService
from src.api.services.student_transformer import calculate_credits_evaluated, parse_prerequisites, build_history_by_period
from src.db.data_warehouse import DataWareHouseSync
from typing import Any, Dict

auth_bp = Blueprint('auth_routes', __name__)

@auth_bp.route('/auth/google', methods=['POST', 'OPTIONS'])
def auth_google():
    data = request.json
    if not data: return jsonify({"error": "No data"}), 400
    token = data.get('token')
    
    if not token:
        return jsonify({"error": "Token no proporcionado"}), 400

    try:
        # 1. Validar el token con los servidores de Google (con margen de 10s para el reloj)
        idinfo = id_token.verify_oauth2_token(token, google_requests.Request(), GOOGLE_CLIENT_ID, clock_skew_in_seconds=10)
        
        email = idinfo.get('email')
        nombre = idinfo.get('name')
        
        # 2. FILTRO DE SEGURIDAD INSTITUCIONAL
        if not email.endswith("@unphu.edu.do"):
            print(f"INTENTO DE ACCESO DENEGADO: {email}")
            return jsonify({"error": "Acceso restringido a correos institucionales de la UNPHU"}), 403
            
        # Extraer matrícula del correo
        matricula = email.split('@')[0]
        
        api_data: Dict[str, Any] = {}
        
        # 3. CONSUMIR API SERVICES DE LA UNPHU
        data_est = UnphuApiService.get_student_data(matricula)
        print(f"[DEBUG] data_est keys: {list(data_est.keys()) if data_est else 'EMPTY'}")
        if not data_est:
            return jsonify({"error": "No se pudo conectar con el sistema base de la UNPHU. Intente nuevamente en breves momentos."}), 502
            
        id_persona = data_est.get('id')
        api_data['matricula'] = data_est.get('username')
        api_data['carrera'] = data_est.get('career')
        
        data_car = UnphuApiService.get_student_career(str(id_persona))
        if not data_car:
            return jsonify({"error": "No se pudo obtener la carrera del estudiante."}), 502
            
        id_carrera = data_car.get('IdCarrera')
                
        # Historial y Estadísticas de Créditos Evaluados
        historial = UnphuApiService.get_pending_grades(str(id_persona), str(id_carrera))
        print(f"[DEBUG] historial len: {len(historial)}")
        if len(historial) > 0:
            print(f"[DEBUG] primer item: approved={historial[0].get('approved')}, pensumCredit={historial[0].get('pensumCredit')}, lyrics='{historial[0].get('lyrics')}'")
            print(f"[DEBUG] ultimo item:  approved={historial[-1].get('approved')}, pensumCredit={historial[-1].get('pensumCredit')}, lyrics='{historial[-1].get('lyrics')}'")
            creditos_evaluados = calculate_credits_evaluated(historial)
            
            max_approved = max((int(h.get('approved', 0)) if isinstance(h.get('approved'), (int, str)) and str(h.get('approved')).isdigit() else 0 for h in historial), default=0)
            max_pensum = max((int(h.get('pensumCredit', 0)) if isinstance(h.get('pensumCredit'), (int, str)) and str(h.get('pensumCredit')).isdigit() else 0 for h in historial), default=0)
            print(f"[DEBUG] max_approved={max_approved}, max_pensum={max_pensum}, creditos_evaluados={creditos_evaluados}")
            
            api_data['stats'] = {
                'creditosAprobados': max_approved,
                'creditosEvaluados': creditos_evaluados,
                'totalCreditos': max_pensum,
                'materiasAprobadas': max_approved // 3
            }
            
            # Preparar y Limpiar Prerrequistos
            pending_list = [
                h for h in historial 
                if not (str(h.get('lyrics', '')).strip() or 
                        str(h.get('number', '')).strip() or 
                        str(h.get('observations', '')).strip())
            ]
            
            api_data['pending_subjects'] = parse_prerequisites(pending_list)
            api_data['history'] = build_history_by_period(historial)
        
        # Indices y Semestre Actual 
        # Intentar leer desde API
        grades_list = UnphuApiService.get_semester_grades(2025, 3, str(id_persona), str(id_carrera))
        real_index = 0
        
        if len(grades_list) > 0 and grades_list[0].get('cumulativeIndex', 0) > 0:
            real_index = grades_list[0].get('cumulativeIndex', 0)
        else:
            # Fallback: Extraer de la Base de Datos (SQL Server) si la API nos bloqueó el valor
            from src.db.connection import DBConnection
            conn = DBConnection.get_connection()
            if conn:
                try:
                    cursor = conn.cursor()
                    import re
                    id_p_num = int(re.search(r'\d+', matricula.replace('-','')).group()) if re.search(r'\d+', matricula.replace('-','')) else 0
                    cursor.execute("SELECT MAX(IndiceAcumulado) FROM Fact_Calificaciones WHERE IdPersona = %s", (id_p_num,))
                    row = cursor.fetchone()
                    if row and row[0] is not None:
                        real_index = float(row[0])
                except Exception as e:
                    print("Error reading index from DB fallback:", e)
                finally:
                    conn.close()
        
        api_data['indices'] = {'semesterIndex': real_index, 'cumulativeIndex': real_index}
        
        periodo_actual = UnphuApiService.get_current_period()
        
        ano_actual = 2026 # Override from user url request tests
        numero_periodo = 1
        
        enrolled = UnphuApiService.get_officially_enrolled(ano_actual, numero_periodo, str(id_persona), str(id_carrera))
        selected = UnphuApiService.get_unofficial_selected(ano_actual, numero_periodo, str(id_persona), str(id_carrera))
        
        api_data['current_period'] = [periodo_actual] if periodo_actual else []
        api_data['registered_subjects'] = enrolled
        api_data['selected_subjects'] = selected
        
        # 4. Sincronización Base hacia PostgreSQL de forma síncrona 
        # (Vercel Serverless congela los hilos en background, así que esto debe ser síncrono para el ranking)
        DataWareHouseSync.sync_student_login(api_data, matricula, nombre, str(id_carrera))
        
        # Sincronización PROFUNDA hacia PostgreSQL usando un Worker Thread en 2do plano
        from src.api.services.background_worker import enqueue_student_sync
        enqueue_student_sync(str(id_persona), str(id_carrera), matricula, nombre, api_data)

        # Calcular ranking del DW y añadir a la data para la GUI
        api_data['ranking'] = DataWareHouseSync.get_student_ranking(matricula)

        import json
        print(f"[DEBUG] api_data completo enviado al frontend:\n{json.dumps(api_data, indent=2, default=str)}")
        return jsonify({
            "success": True,
            "message": "Autenticación exitosa",
            "matricula": matricula,
            "name": nombre,
            "api_data": api_data
        })

    except ValueError as e:
        import traceback
        traceback.print_exc()
        return jsonify({"error": "Token de Google inválido o expirado", "details": str(e)}), 401
    except Exception as e:
        import traceback
        traceback.print_exc()
        return jsonify({"error": "Error interno del servidor", "details": str(e)}), 500
