from flask import Blueprint, request, jsonify # type: ignore
from google.oauth2 import id_token # type: ignore
from google.auth.transport import requests as google_requests # type: ignore
from src.config.settings import GOOGLE_CLIENT_ID, UNPHU_API_TOKEN, UNPHU_API_BASE_URL, UNPHU_ACADEMIC_PERSON_ID

auth_bp = Blueprint('auth_routes', __name__)

@auth_bp.route('/auth/google', methods=['POST', 'OPTIONS'])
def auth_google():
    """
    Verifica el token de Google y devuelve las credenciales necesarias
    para que el cliente llame directamente a la API de la UNPHU.
    Las llamadas a la API de la UNPHU se hacen desde el navegador del estudiante
    (en RD) para evitar restricciones de IP en los servidores de Vercel (EE.UU.).
    """
    data = request.json
    if not data: return jsonify({"error": "No data"}), 400
    token = data.get('token')

    if not token:
        return jsonify({"error": "Token no proporcionado"}), 400

    try:
        # Validar el token con los servidores de Google
        idinfo = id_token.verify_oauth2_token(
            token, google_requests.Request(), GOOGLE_CLIENT_ID, clock_skew_in_seconds=60
        )

        email = idinfo.get('email')
        nombre = idinfo.get('name')

        # Filtro de seguridad institucional
        if not email.endswith("@unphu.edu.do"):
            print(f"INTENTO DE ACCESO DENEGADO: {email}")
            return jsonify({"error": "Acceso restringido a correos institucionales de la UNPHU"}), 403

        matricula = email.split('@')[0]

        res = {
            "success": True,
            "message": "Autenticación exitosa",
            "matricula": matricula,
            "name": nombre,
            "unphu_token": UNPHU_API_TOKEN,
            "unphu_api_url": UNPHU_API_BASE_URL
        }
        if UNPHU_ACADEMIC_PERSON_ID:
            res["academic_person_id"] = UNPHU_ACADEMIC_PERSON_ID
        return jsonify(res)

    except ValueError as e:
        import traceback
        traceback.print_exc()
        return jsonify({"error": "Token de Google inválido o expirado", "details": str(e)}), 401
    except Exception as e:
        import traceback
        traceback.print_exc()
        return jsonify({"error": "Error interno del servidor", "details": str(e)}), 500


@auth_bp.route('/api/sync', methods=['POST'])
def sync_student_data():
    """
    Recibe los datos procesados del cliente y los sincroniza con el Data Warehouse (PostgreSQL).
    Llamada opcional/asíncrona desde el navegador después del login.
    """
    data = request.json
    if not data:
        return jsonify({"ok": False}), 400

    try:
        matricula = data.get('matricula', '')
        nombre = data.get('nombre', '')
        id_carrera = data.get('id_carrera', '0')
        api_data = data.get('api_data', {})

        if not matricula or not api_data:
            return jsonify({"ok": False, "error": "Datos insuficientes"}), 400

        from src.db.data_warehouse import DataWareHouseSync
        DataWareHouseSync.sync_student_login(api_data, matricula, nombre, id_carrera)
        ranking = DataWareHouseSync.get_student_ranking(matricula)

        return jsonify({"ok": True, "ranking": ranking})
    except Exception as e:
        import traceback
        traceback.print_exc()
        return jsonify({"ok": False, "error": str(e)}), 500
