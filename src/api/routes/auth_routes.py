from flask import Blueprint, request, jsonify # type: ignore
from google.oauth2 import id_token # type: ignore
from google.auth.transport import requests as google_requests # type: ignore
from src.config.settings import GOOGLE_CLIENT_ID, UNPHU_API_TOKEN, UNPHU_API_BASE_URL

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

    # Interceptar inicio de sesión de usuario mock
    if isinstance(token, str) and token.startswith('mock_'):
        matricula = token.replace('mock_', '').strip()
        from src.config.mock_users import MOCK_USERS
        student = MOCK_USERS.get(matricula)
        if isinstance(student, dict):
            student_data = student.get('student_data')
            nombre = 'Estudiante'
            if isinstance(student_data, dict):
                nombre = student_data.get('names', 'Estudiante')
            return jsonify({
                "success": True,
                "message": "Autenticación exitosa (Mock)",
                "matricula": matricula,
                "name": nombre,
                "unphu_token": f"mock_token_{matricula}",
                "unphu_api_url": f"{request.host_url.rstrip('/')}/api/mock/unphu"
            })
        else:
            return jsonify({"error": f"Usuario mock '{matricula}' no configurado"}), 404

    try:
        # Validar el token con los servidores de Google
        # TEMP: clock_skew aumentado porque el reloj del sistema tiene desfase.
        # Sincroniza el reloj de Windows y vuelve a poner clock_skew_in_seconds=10
        idinfo = id_token.verify_oauth2_token(
            token, google_requests.Request(), GOOGLE_CLIENT_ID, clock_skew_in_seconds=86400
        )

        email = idinfo.get('email')
        nombre = idinfo.get('name')

        if not email:
            return jsonify({"error": "No se pudo obtener el correo de Google"}), 400

        # Filtro de seguridad institucional
        if not email.endswith("@unphu.edu.do"):
            print(f"INTENTO DE ACCESO DENEGADO: {email}")
            return jsonify({"error": "Acceso restringido a correos institucionales de la UNPHU"}), 403

        matricula = email.split('@')[0]

        # Devolver credenciales para que el cliente llame a la API de la UNPHU directamente
        return jsonify({
            "success": True,
            "message": "Autenticación exitosa",
            "matricula": matricula,
            "name": nombre,
            "unphu_token": UNPHU_API_TOKEN,
            "unphu_api_url": UNPHU_API_BASE_URL
        })

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


# ── Proxy local para resolver CORS en desarrollo ──────────────────────────────
# El browser no puede llamar directamente a la API UNPHU desde localhost porque
# la API no envía Access-Control-Allow-Origin en respuestas 200.
# Este proxy recibe la llamada del browser (mismo origen = sin CORS) y la
# reenvía desde Flask, que corre en la máquina del estudiante (IP de RD = sin bloqueo geo).
# En producción (Vercel) el browser llama directo; este endpoint no se usa.
@auth_bp.route('/api/unphu-proxy/<path:subpath>', methods=['GET', 'OPTIONS'])
def unphu_proxy(subpath):
    if request.method == 'OPTIONS':
        from flask import Response
        resp = Response('', status=204)
        resp.headers['Access-Control-Allow-Origin'] = '*'
        resp.headers['Access-Control-Allow-Headers'] = 'Authorization, Content-Type'
        resp.headers['Access-Control-Allow-Methods'] = 'GET, OPTIONS'
        return resp

    import requests as req
    import urllib3
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

    # Reconstruir la URL destino con query string
    target_url = f"{UNPHU_API_BASE_URL}/{subpath}"
    query_string = request.query_string.decode('utf-8')
    if query_string:
        target_url += '?' + query_string

    # Reenviar el Authorization header que mandó el browser
    auth_header = request.headers.get('Authorization', '')
    headers = {}
    if auth_header:
        headers['Authorization'] = auth_header

    # Salvaguarda: Si es un token de prueba mock, re-enrutar a la API mock interna
    if 'mock_token_' in auth_header:
        target_url = f"{request.host_url.rstrip('/')}/api/mock/unphu/{subpath}"
        if query_string:
            target_url += '?' + query_string


    try:
        r = req.get(target_url, headers=headers, verify=False, timeout=10)
        from flask import Response
        resp = Response(r.content, status=r.status_code, content_type=r.headers.get('Content-Type', 'application/json'))
        resp.headers['Access-Control-Allow-Origin'] = '*'
        return resp
    except Exception as e:
        return jsonify({"error": str(e)}), 502

