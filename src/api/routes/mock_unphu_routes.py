# src/api/routes/mock_unphu_routes.py
from flask import Blueprint, request, jsonify  # type: ignore
from src.config.mock_users import MOCK_USERS

mock_unphu_bp = Blueprint('mock_unphu_routes', __name__, url_prefix='/api/mock/unphu')

def get_matricula_by_id_persona(id_persona: str) -> str:
    """Mapea el IdPersona mock de vuelta a su matrícula."""
    id_persona = str(id_persona).strip()
    if id_persona == "242083" or id_persona == "212083": return "ms24-2083"

    if id_persona == "220941": return "cm22-0941"
    if id_persona == "190112": return "hp19-0112"
    if id_persona == "211402": return "mr21-1402"
    return ""

def get_matricula_from_auth() -> str:
    """Extrae la matrícula desde el header de autorización mock."""
    auth_header = request.headers.get('Authorization', '')
    if auth_header.startswith('Bearer mock_token_'):
        return auth_header.replace('Bearer mock_token_', '').strip()
    return ""

def resolve_mock_user() -> dict:
    """Resuelve cuál estudiante mock está realizando la petición actual."""
    # 1. Desde el header de autorización
    matricula = get_matricula_from_auth()
    if matricula in MOCK_USERS:
        return MOCK_USERS[matricula]

    # 2. Desde el parámetro de consulta IdPersona
    id_persona = request.args.get('IdPersona')
    if id_persona:
        matricula = get_matricula_by_id_persona(id_persona)
        if matricula in MOCK_USERS:
            return MOCK_USERS[matricula]

    # 3. Búsqueda secundaria en cualquier parámetro que contenga la matrícula
    for arg_val in request.args.values():
        if arg_val in MOCK_USERS:
            return MOCK_USERS[arg_val]

    return {}

@mock_unphu_bp.route('/student-data/<matricula>', methods=['GET'])
def student_data(matricula):
    """Retorna los datos básicos del estudiante mock."""
    user = MOCK_USERS.get(matricula)
    if user:
        return jsonify({"data": user["student_data"]})
    return jsonify({"error": "Estudiante mock no encontrado"}), 404

@mock_unphu_bp.route('/get-student-careers/', methods=['GET'])
def student_careers():
    """Retorna el listado de carreras del estudiante mock."""
    user = resolve_mock_user()
    if user:
        return jsonify({"data": user["careers"]})
    return jsonify({"data": []})

@mock_unphu_bp.route('/get-current-period/', methods=['GET'])
def current_period():
    """Retorna el período actual simulado."""
    return jsonify({
        "data": {
            "id": 107,
            "year": 2026,
            "numeroPeriodo": 3,
            "periodName": "Septiembre - Diciembre 2026",
            "esPeriodoActual": True
        }
    })


@mock_unphu_bp.route('/pending-grades-students/', methods=['GET'])
def pending_grades():
    """Retorna el historial completo de asignaturas aprobadas y pendientes."""
    user = resolve_mock_user()
    if user:
        return jsonify({"data": user["pending_grades"]})
    return jsonify({"data": []})

@mock_unphu_bp.route('/semester-grades/', methods=['GET'])
def semester_grades():
    """Retorna las notas e índices de un período específico."""
    user = resolve_mock_user()
    if user:
        year = request.args.get('Ano')
        period = request.args.get('IdPeriodo')
        key = f"{year}-{period}"
        
        # Si hay calificaciones guardadas (semestres pasados)
        if key in user["semester_grades"]:
            return jsonify({"data": user["semester_grades"][key]})
            
        # Si es un periodo actual/futuro (2026 en adelante)
        try:
            year_num = int(year or 0)
        except ValueError:
            year_num = 0

        if year_num >= 2026:
            enrolled = user.get("officially_enrolled", [])
            selected = user.get("unofficial_selected", [])
            
            grades = []
            for s in enrolled + selected:
                code = s.get("codeSubject") or s.get("groupSubjectCode") or s.get("code") or "N/A"
                name = s.get("subjectName") or s.get("subject") or "Asignatura"
                
                try:
                    cred = int(float(s.get("credits") or 0))
                except ValueError:
                    cred = 0
                
                # Generar las 5 filas de rubros en curso para cada materia
                for rubro in ["ASIS", "ACUM1", "ACUM2", "ACUM3", "Eval_Final"]:
                    grades.append({
                        "codGroup": code,
                        "course": name,
                        "credits": cred,
                        "letter": "EC",
                        "qualification": 0,
                        "indexPeriod": 0.0,
                        "cumulativeIndex": 0.0,
                        "codRubro": rubro,
                        "assignedPoints": "NR",
                        "points": 1 if rubro == "ASIS" else 10 if rubro == "ACUM3" else 30,
                        "year": year_num,
                        "period": int(period or 1)
                    })
            return jsonify({"data": grades})
            
        return jsonify({"data": []})
    return jsonify({"data": []})

@mock_unphu_bp.route('/officially-enrolled-subjects/', methods=['GET'])
def officially_enrolled():
    """Retorna las asignaturas inscritas oficialmente en el período actual."""
    user = resolve_mock_user()
    if user:
        return jsonify({"data": user["officially_enrolled"]})
    return jsonify({"data": []})

@mock_unphu_bp.route('/unofficial-selected-subjects/', methods=['GET'])
def unofficial_selected():
    """Retorna las asignaturas seleccionadas en prematrícula."""
    user = resolve_mock_user()
    if user:
        return jsonify({"data": user["unofficial_selected"]})
    return jsonify({"data": []})
