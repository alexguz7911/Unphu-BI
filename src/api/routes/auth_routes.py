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
        
        # 3. CONSUMIR API SERVICES DE LA UNPHU Y SINCRONIZAR (Orquestado por StudentSyncService)
        from src.api.services.student_sync_service import StudentSyncService
        api_data = StudentSyncService.fetch_and_sync_all(matricula, nombre)
        
        if not api_data:
            return jsonify({"error": "No se pudo conectar con el sistema base de la UNPHU. Intente nuevamente en breves momentos."}), 502
            
        # Sincronización PROFUNDA hacia PostgreSQL usando un Worker Thread en 2do plano
        from src.api.services.background_worker import enqueue_student_sync
        import re
        id_persona_val = int(re.search(r'\d+', matricula.replace('-','')).group()) if re.search(r'\d+', matricula.replace('-','')) else 0
        
        enqueue_student_sync(str(id_persona_val), api_data.get('id_carrera', '0'), matricula, nombre, api_data)

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
        error_details = traceback.format_exc()
        print(f"[FATAL] {error_details}")
        return jsonify({
            "error": "Error interno del servidor", 
            "details": str(e),
            "traceback": error_details
        }), 500
