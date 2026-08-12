# src/config/mock_users.py
# Genera dinámicamente perfiles mock consistentes, con progreso variado y alertas de ruta crítica.

import math

def generate_mock_student(
    matricula: str,
    names: str,
    career_name: str,
    id_persona: int,
    id_carrera: int,
    base_completed: list,
    pending_subjects: list,
    target_credits: int,
    start_year: int,
    end_year: int,
    officially_enrolled: list,
    unofficial_selected: list,
    gpa_target: float
) -> dict:
    """
    Genera dinámicamente el historial académico completo y las calificaciones detalladas
    por rubro (ASIS, ACUM1, ACUM2, ACUM3, Eval_Final) para asegurar consistencia
    matemática entre la vista del Historial y la vista de Calificaciones del Semestre.
    """
    # 1. Calcular créditos base completados
    base_credits = sum(int(s['credits']) for s in base_completed)
    remaining_to_generate = target_credits - base_credits

    # 2. Pool de materias genéricas según la carrera
    generic_pool = [
        ("MAT-201", "CALCULO III", 4),
        ("MAT-202", "ALGEBRA LINEAL", 3),
        ("FIS-101", "FISICA GENERAL I", 4),
        ("FIS-102", "FISICA GENERAL II", 4),
        ("HUM-101", "ESPANOL I", 3),
        ("HUM-102", "ESPANOL II", 3),
        ("HUM-201", "HISTORIA SOCIAL DOMINICANA", 3),
        ("ING-101", "INGLES TECNICO", 2),
        ("MAT-301", "ESTADISTICA GENERAL", 3),
        ("HUM-202", "SOCIOLOGIA", 3),
        ("HUM-203", "FILOSOFIA Y ETICA", 3),
        ("ADM-101", "INTRODUCCION A LA ADMINISTRACION", 3),
        ("ELECT-1", "ELECTIVA DE AREA I", 3),
        ("ELECT-2", "ELECTIVA DE AREA II", 4),
        ("ELECT-3", "ELECTIVA DE AREA III", 3),
        ("ELECT-4", "ELECTIVA GENERAL I", 2),
        ("ELECT-5", "ELECTIVA GENERAL II", 2),
    ]
    # Ajuste de prefijos según la carrera
    if "MEDICINA" in career_name:
        generic_pool = [(code.replace("MAT", "MED").replace("INF", "MED").replace("FIS", "MED"), name, cred) for code, name, cred in generic_pool]
    elif "ARQUITECTURA" in career_name:
        generic_pool = [(code.replace("MAT", "ARQ").replace("INF", "ARQ").replace("FIS", "ARQ"), name, cred) for code, name, cred in generic_pool]
    elif "DERECHO" in career_name:
        generic_pool = [(code.replace("MAT", "DER").replace("INF", "DER").replace("FIS", "DER"), name, cred) for code, name, cred in generic_pool]

    # Generar materias adicionales
    generated_completed = []
    pool_idx = 0
    generated_credits = 0

    while generated_credits < remaining_to_generate:
        code, name, cred = generic_pool[pool_idx % len(generic_pool)]
        code_suffix = pool_idx // len(generic_pool)
        final_code = f"{code}-{code_suffix}" if code_suffix > 0 else code
        final_name = f"{name} {code_suffix + 1}" if code_suffix > 0 else name
        
        generated_completed.append({
            "codeSubject": final_code,
            "subject": final_name,
            "credits": str(cred),
            "lyrics": "",
            "number": "",
            "semester": "",
            "observations": "",
            "codeRequired": "",
            "pensumCredit": str(target_credits + sum(int(p['credits']) for p in pending_subjects) + 10)
        })
        generated_credits += cred
        pool_idx += 1

    all_completed = base_completed + generated_completed

    # 3. Distribuir materias completadas en los semestres disponibles
    semesters = []
    for yr in range(start_year, end_year + 1):
        for per in [1, 2, 3]:
            semesters.append(f"{yr}-{per}")

    period_labels = {1: 'ENE-ABR', 2: 'MAY-AGO', 3: 'SEP-DIC'}
    def get_semester_label(sem_key):
        yr, per = sem_key.split('-')
        return f"{period_labels[int(per)]} {yr}"

    sem_idx = 0
    for sub in all_completed:
        if not sub.get('semester'):
            target_sem = semesters[sem_idx % len(semesters)]
            sub['semester'] = get_semester_label(target_sem)
            sub['_sem_key'] = target_sem
            sem_idx += 1
        else:
            lbl = sub['semester']
            parts = lbl.split(' ')
            if len(parts) == 2:
                p_num = 1 if 'ENE' in parts[0] else 2 if 'MAY' in parts[0] else 3
                sub['_sem_key'] = f"{parts[1]}-{p_num}"
            else:
                sub['_sem_key'] = semesters[0]

    # 4. Feedback loop para converger al GPA
    points_map = {'A': 4, 'B': 3, 'C': 2}
    total_points = 0
    total_credits = 0

    for sub in all_completed:
        if sub.get('lyrics'):
            lyr = sub['lyrics']
            pts = points_map.get(lyr, 4)
            total_points += pts * int(sub['credits'])
            total_credits += int(sub['credits'])

    for sub in all_completed:
        if not sub.get('lyrics'):
            current_gpa = total_points / total_credits if total_credits > 0 else 0
            if current_gpa < gpa_target:
                lyr = 'A'
                num = 93
            else:
                if current_gpa - gpa_target > 0.15:
                    lyr = 'C'
                    num = 78
                else:
                    lyr = 'B'
                    num = 85
            
            sub['lyrics'] = lyr
            sub['number'] = str(num)
            pts = points_map[lyr]
            total_points += pts * int(sub['credits'])
            total_credits += int(sub['credits'])

    # 5. Agrupar por semestre
    by_semester = {}
    for sem in semesters:
        by_semester[sem] = []

    for sub in all_completed:
        sem_key = sub['_sem_key']
        if sem_key in by_semester:
            by_semester[sem_key].append(sub)

    # Calcular índices y calificaciones detalladas por rubro
    semester_grades = {}
    cum_points = 0
    cum_credits = 0

    for sem_key in semesters:
        subs = by_semester[sem_key]
        if not subs:
            continue
            
        sem_points = 0
        sem_credits = 0
        for sub in subs:
            cred = int(sub['credits'])
            pts = points_map.get(sub['lyrics'], 4)
            sem_points += pts * cred
            sem_credits += cred
            
            cum_points += pts * cred
            cum_credits += cred

        sem_gpa = sem_points / sem_credits if sem_credits > 0 else 0.0
        cum_gpa = cum_points / cum_credits if cum_credits > 0 else 0.0

        yr, per = sem_key.split('-')
        sem_rows = []
        for sub in subs:
            code = sub["codeSubject"]
            name = sub["subject"]
            cred = int(sub["credits"])
            lyr = sub["lyrics"]
            num_grade = int(sub["number"] or 85)

            rubros_list = [
                ("ASIS", 1, 1),
                ("ACUM1", round(num_grade * 0.3, 1), 30),
                ("ACUM2", round(num_grade * 0.3, 1), 30),
                ("ACUM3", round(num_grade * 0.1, 1), 10),
                ("Eval_Final", round(num_grade * 0.3, 1), 30)
            ]
            for r_name, r_assigned, r_max in rubros_list:
                sem_rows.append({
                    "codGroup": code,
                    "course": name,
                    "credits": cred,
                    "letter": lyr,
                    "qualification": num_grade,
                    "indexPeriod": round(sem_gpa, 2),
                    "cumulativeIndex": round(cum_gpa, 2),
                    "codRubro": r_name,
                    "assignedPoints": r_assigned,
                    "points": r_max,
                    "year": int(yr),
                    "period": int(per)
                })

        semester_grades[sem_key] = sem_rows

    for sub in all_completed:
        if '_sem_key' in sub:
            del sub['_sem_key']

    final_pending_grades = all_completed + pending_subjects

    return {
        "student_data": {
            "id": id_persona,
            "names": names,
            "username": matricula,
            "email": f"{matricula}@unphu.edu.do",
            "career": career_name,
            "enclosure": "Santo Domingo",
            "enrollment": matricula.split('-')[-1],
            "userType": "STUDENT"
        },
        "careers": [
            {
                "IdPersona": id_persona,
                "IdCarrera": id_carrera,
                "NombreCarrera": career_name
            }
        ],
        "pending_grades": final_pending_grades,
        "semester_grades": semester_grades,
        "officially_enrolled": officially_enrolled,
        "unofficial_selected": unofficial_selected
    }

# Definición de Estudiantes y sus Materias Clave
MOCK_USERS = {
    # 1. MARIA SANTOS - 95% PROGRESO (Cerca de Graduación)
    "ms21-2083": generate_mock_student(
        matricula="ms21-2083",
        names="MARIA ELENA SANTOS ALMONTE",
        career_name="INGENIERIA EN SISTEMAS COMPUTACIONALES 255/3-15-15",
        id_persona=212083,
        id_carrera=251,
        base_completed=[
            {"codeSubject": "INF-101", "subject": "INTRODUCCION A LA COMPUTACION", "credits": "3", "lyrics": "A", "number": "92", "semester": "ENE-ABR 2024", "observations": "", "codeRequired": "", "pensumCredit": "216"},
            {"codeSubject": "MAT-101", "subject": "MATEMATICA I", "credits": "4", "lyrics": "B", "number": "85", "semester": "ENE-ABR 2024", "observations": "", "codeRequired": "", "pensumCredit": "216"},
            {"codeSubject": "INF-102", "subject": "ALGORITMOS Y ESTRUCTURAS DE DATOS", "credits": "4", "lyrics": "A", "number": "94", "semester": "MAY-AGO 2024", "observations": "", "codeRequired": "INF-101", "pensumCredit": "216"},
            {"codeSubject": "MAT-102", "subject": "MATEMATICA II", "credits": "4", "lyrics": "B", "number": "88", "semester": "MAY-AGO 2024", "observations": "", "codeRequired": "MAT-101", "pensumCredit": "216"},
            {"codeSubject": "INF-201", "subject": "PROGRAMACION I", "credits": "4", "lyrics": "A", "number": "95", "semester": "SEP-DIC 2024", "observations": "", "codeRequired": "INF-102", "pensumCredit": "216"},
            {"codeSubject": "INF-202", "subject": "BASES DE DATOS I", "credits": "4", "lyrics": "A", "number": "91", "semester": "SEP-DIC 2024", "observations": "", "codeRequired": "INF-102", "pensumCredit": "216"},
            {"codeSubject": "INF-301", "subject": "INGENIERIA DE SOFTWARE I", "credits": "3", "lyrics": "B", "number": "87", "semester": "ENE-ABR 2025", "observations": "", "codeRequired": "INF-201", "pensumCredit": "216"},
            {"codeSubject": "INF-302", "subject": "BASES DE DATOS II", "credits": "4", "lyrics": "A", "number": "93", "semester": "ENE-ABR 2025", "observations": "", "codeRequired": "INF-202", "pensumCredit": "216"},
            {"codeSubject": "INF-401", "subject": "REDES DE COMPUTADORAS", "credits": "4", "lyrics": "A", "number": "96", "semester": "MAY-AGO 2025", "observations": "", "codeRequired": "INF-202", "pensumCredit": "216"},
            {"codeSubject": "INF-402", "subject": "SISTEMAS OPERATIVOS", "credits": "4", "lyrics": "C", "number": "77", "semester": "MAY-AGO 2025", "observations": "", "codeRequired": "INF-201", "pensumCredit": "216"},
            {"codeSubject": "INF-501", "subject": "PROGRAMACION WEB", "credits": "4", "lyrics": "A", "number": "95", "semester": "SEP-DIC 2025", "observations": "", "codeRequired": "INF-302", "pensumCredit": "216"}
        ],
        pending_subjects=[
            {"codeSubject": "INF-604", "subject": "MINERIA DE DATOS", "credits": 4, "lyrics": "", "number": "", "semester": "", "observations": "", "codeRequired": "INF-302", "pensumCredit": "216"},
            {"codeSubject": "INF-901", "subject": "TRABAJO DE GRADO", "credits": 6, "lyrics": "", "number": "", "semester": "", "observations": "", "codeRequired": "INF-604", "pensumCredit": "216"}
        ],
        target_credits=206,  # 95% de 216
        start_year=2021,
        end_year=2025,
        officially_enrolled=[
            {"codeSubject": "INF-601", "subjectName": "INTELIGENCIA ARTIFICIAL", "credits": "3", "section": "01", "teacher": "Dr. Alan Turing"},
            {"codeSubject": "INF-602", "subjectName": "SEGURIDAD DE LA INFORMACION", "credits": "3", "section": "02", "teacher": "Ing. Bruce Schneier"}
        ],
        unofficial_selected=[
            {"codeSubject": "INF-601", "subjectName": "INTELIGENCIA ARTIFICIAL", "credits": "3", "section": "01", "teacher": "Dr. Alan Turing"},
            {"codeSubject": "INF-602", "subjectName": "SEGURIDAD DE LA INFORMACION", "credits": "3", "section": "02", "teacher": "Ing. Bruce Schneier"}
        ],
        gpa_target=3.80
    ),

    # 2. CARLOS MENDOZA - 60% PROGRESO (Mitad de Carrera, con bloqueos)
    "cm22-0941": generate_mock_student(
        matricula="cm22-0941",
        names="CARLOS ALBERTO MENDOZA RUIZ",
        career_name="ARQUITECTURA 102/2-22-22",
        id_persona=220941,
        id_carrera=6,
        base_completed=[
            {"codeSubject": "ARQ-101", "subject": "DIBUJO TECNICO", "credits": "3", "lyrics": "A", "number": "90", "semester": "ENE-ABR 2024", "observations": "", "codeRequired": "", "pensumCredit": "230"},
            {"codeSubject": "ARQ-102", "subject": "INTRODUCCION A LA ARQUITECTURA", "credits": "4", "lyrics": "B", "number": "82", "semester": "ENE-ABR 2024", "observations": "", "codeRequired": "", "pensumCredit": "230"},
            {"codeSubject": "ARQ-201", "subject": "DISENO ARQUITECTONICO I", "credits": "5", "lyrics": "A", "number": "91", "semester": "MAY-AGO 2024", "observations": "", "codeRequired": "ARQ-101", "pensumCredit": "230"},
            {"codeSubject": "ARQ-202", "subject": "GEOMETRIA DESCRIPTIVA", "credits": "3", "lyrics": "B", "number": "85", "semester": "MAY-AGO 2024", "observations": "", "codeRequired": "ARQ-101", "pensumCredit": "230"},
            {"codeSubject": "ARQ-301", "subject": "DISENO ARQUITECTONICO II", "credits": "5", "lyrics": "B", "number": "88", "semester": "SEP-DIC 2024", "observations": "", "codeRequired": "ARQ-201", "pensumCredit": "230"},
            {"codeSubject": "ARQ-302", "subject": "HISTORIA DE LA ARQUITECTURA I", "credits": "3", "lyrics": "A", "number": "93", "semester": "SEP-DIC 2024", "observations": "", "codeRequired": "ARQ-102", "pensumCredit": "230"},
            {"codeSubject": "ARQ-303", "subject": "TECNOLOGIA DE CONSTRUCCION I", "credits": "4", "lyrics": "C", "number": "78", "semester": "ENE-ABR 2025", "observations": "", "codeRequired": "ARQ-202", "pensumCredit": "230"},
            {"codeSubject": "ARQ-401", "subject": "DISENO ARQUITECTONICO III", "credits": "5", "lyrics": "A", "number": "92", "semester": "MAY-AGO 2025", "observations": "", "codeRequired": "ARQ-301", "pensumCredit": "230"}
        ],
        pending_subjects=[
            # ARQ-503 es materia clave (bloquea 3 materias pendientes) -> Prioridad Alta (unlocks >= 2)
            {"codeSubject": "ARQ-503", "subject": "DISENO ARQUITECTONICO IV", "credits": 5, "lyrics": "", "number": "", "semester": "", "observations": "", "codeRequired": "ARQ-401", "pensumCredit": "230"},
            {"codeSubject": "ARQ-504", "subject": "URBANISMO I", "credits": 3, "lyrics": "", "number": "", "semester": "", "observations": "", "codeRequired": "ARQ-302", "pensumCredit": "230"},
            {"codeSubject": "ARQ-505", "subject": "TECNOLOGIA DE CONSTRUCCION II", "credits": 4, "lyrics": "", "number": "", "semester": "", "observations": "", "codeRequired": "ARQ-303", "pensumCredit": "230"},
            
            # Materias bloqueadas por ARQ-503
            {"codeSubject": "ARQ-601", "subject": "DISENO ARQUITECTONICO V", "credits": 5, "lyrics": "", "number": "", "semester": "", "observations": "", "codeRequired": "ARQ-503", "pensumCredit": "230"},
            {"codeSubject": "ARQ-602", "subject": "URBANISMO II", "credits": 3, "lyrics": "", "number": "", "semester": "", "observations": "", "codeRequired": "ARQ-503", "pensumCredit": "230"},
            {"codeSubject": "ARQ-603", "subject": "PAISAJISMO", "credits": 3, "lyrics": "", "number": "", "semester": "", "observations": "", "codeRequired": "ARQ-503", "pensumCredit": "230"},
            
            # Materias de término adicionales
            {"codeSubject": "ARQ-701", "subject": "DISENO ARQUITECTONICO VI", "credits": 5, "lyrics": "", "number": "", "semester": "", "observations": "", "codeRequired": "ARQ-601", "pensumCredit": "230"},
            {"codeSubject": "ARQ-702", "subject": "TEORIA DE LA ARQUITECTURA", "credits": 3, "lyrics": "", "number": "", "semester": "", "observations": "", "codeRequired": "", "pensumCredit": "230"},
            {"codeSubject": "ARQ-801", "subject": "DISENO ARQUITECTONICO VII", "credits": 5, "lyrics": "", "number": "", "semester": "", "observations": "", "codeRequired": "ARQ-701", "pensumCredit": "230"},
            {"codeSubject": "ARQ-802", "subject": "ETICA PROFESIONAL ARQ", "credits": 2, "lyrics": "", "number": "", "semester": "", "observations": "", "codeRequired": "", "pensumCredit": "230"},
            {"codeSubject": "ARQ-901", "subject": "TRABAJO DE GRADO ARQ", "credits": 6, "lyrics": "", "number": "", "semester": "", "observations": "", "codeRequired": "ARQ-801 y ARQ-802", "pensumCredit": "230"},
            
            # Relleno para ajustar a los créditos restantes
            {"codeSubject": "ARQ-OPT-1", "subject": "OPTATIVA DE DISENO I", "credits": 4, "lyrics": "", "number": "", "semester": "", "observations": "", "codeRequired": "", "pensumCredit": "230"},
            {"codeSubject": "ARQ-OPT-2", "subject": "OPTATIVA DE CONSTRUCCION", "credits": 4, "lyrics": "", "number": "", "semester": "", "observations": "", "codeRequired": "", "pensumCredit": "230"},
            {"codeSubject": "ARQ-OPT-3", "subject": "OPTATIVA TECNICA", "credits": 3, "lyrics": "", "number": "", "semester": "", "observations": "", "codeRequired": "", "pensumCredit": "230"},
            {"codeSubject": "ARQ-OPT-4", "subject": "OPTATIVA CULTURAL", "credits": 3, "lyrics": "", "number": "", "semester": "", "observations": "", "codeRequired": "", "pensumCredit": "230"},
            {"codeSubject": "ARQ-OPT-5", "subject": "OPTATIVA LIBRE I", "credits": 3, "lyrics": "", "number": "", "semester": "", "observations": "", "codeRequired": "", "pensumCredit": "230"},
            {"codeSubject": "ARQ-OPT-6", "subject": "OPTATIVA LIBRE II", "credits": 3, "lyrics": "", "number": "", "semester": "", "observations": "", "codeRequired": "", "pensumCredit": "230"},
            {"codeSubject": "ARQ-OPT-7", "subject": "OPTATIVA LIBRE III", "credits": 3, "lyrics": "", "number": "", "semester": "", "observations": "", "codeRequired": "", "pensumCredit": "230"},
            {"codeSubject": "ARQ-OPT-8", "subject": "OPTATIVA LIBRE IV", "credits": 3, "lyrics": "", "number": "", "semester": "", "observations": "", "codeRequired": "", "pensumCredit": "230"},
            {"codeSubject": "ARQ-OPT-9", "subject": "OPTATIVA LIBRE V", "credits": 3, "lyrics": "", "number": "", "semester": "", "observations": "", "codeRequired": "", "pensumCredit": "230"},
            {"codeSubject": "ARQ-OPT-A", "subject": "OPTATIVA LIBRE VI", "credits": 3, "lyrics": "", "number": "", "semester": "", "observations": "", "codeRequired": "", "pensumCredit": "230"}
        ],
        target_credits=138,  # ~60% de 230
        start_year=2022,
        end_year=2025,
        officially_enrolled=[
            {"codeSubject": "ARQ-501", "subjectName": "DISENO ARQUITECTONICO IV", "credits": "5", "section": "01", "teacher": "Arq. Frank Lloyd Wright"},
            {"codeSubject": "ARQ-502", "subjectName": "URBANISMO I", "credits": "3", "section": "01", "teacher": "Arq. Le Corbusier"}
        ],
        unofficial_selected=[
            {"codeSubject": "ARQ-501", "subjectName": "DISENO ARQUITECTONICO IV", "credits": "5", "section": "01", "teacher": "Arq. Frank Lloyd Wright"},
            {"codeSubject": "ARQ-502", "subjectName": "URBANISMO I", "credits": "3", "section": "01", "teacher": "Arq. Le Corbusier"}
        ],
        gpa_target=3.42
    ),

    # 3. HILDA PEREZ - 30% PROGRESO (Inicios de Carrera, muchas materias faltantes)
    "hp19-0112": generate_mock_student(
        matricula="hp19-0112",
        names="HILDA PATRICIA PEREZ MEJIA",
        career_name="MEDICINA 301/1-12-12",
        id_persona=190112,
        id_carrera=301112,
        base_completed=[
            {"codeSubject": "MED-101", "subject": "BIOLOGIA BASICA", "credits": "4", "lyrics": "B", "number": "84", "semester": "ENE-ABR 2023", "observations": "", "codeRequired": "", "pensumCredit": "310"},
            {"codeSubject": "MED-102", "subject": "QUIMICA ORGANICA", "credits": "4", "lyrics": "C", "number": "76", "semester": "ENE-ABR 2023", "observations": "", "codeRequired": "", "pensumCredit": "310"},
            {"codeSubject": "MED-201", "subject": "ANATOMIA HUMANA I", "credits": "6", "lyrics": "B", "number": "88", "semester": "MAY-AGO 2023", "observations": "", "codeRequired": "MED-101", "pensumCredit": "310"},
            {"codeSubject": "MED-202", "subject": "HISTOLOGIA", "credits": "4", "lyrics": "B", "number": "85", "semester": "MAY-AGO 2023", "observations": "", "codeRequired": "MED-101", "pensumCredit": "310"},
            {"codeSubject": "MED-301", "subject": "ANATOMIA HUMANA II", "credits": "6", "lyrics": "C", "number": "79", "semester": "SEP-DIC 2023", "observations": "", "codeRequired": "MED-201", "pensumCredit": "310"},
            {"codeSubject": "MED-302", "subject": "EMBRIOLOGIA", "credits": "3", "lyrics": "B", "number": "83", "semester": "SEP-DIC 2023", "observations": "", "codeRequired": "MED-202", "pensumCredit": "310"},
            {"codeSubject": "MED-401", "subject": "BIOQUIMICA MEDICA", "credits": "5", "lyrics": "A", "number": "90", "semester": "ENE-ABR 2024", "observations": "", "codeRequired": "MED-102", "pensumCredit": "310"},
            {"codeSubject": "MED-402", "subject": "FISIOLOGIA I", "credits": "5", "lyrics": "B", "number": "86", "semester": "MAY-AGO 2024", "observations": "", "codeRequired": "MED-301", "pensumCredit": "310"},
            {"codeSubject": "MED-403", "subject": "MICROBIOLOGIA I", "credits": "4", "lyrics": "B", "number": "84", "semester": "SEP-DIC 2024", "observations": "", "codeRequired": "MED-202", "pensumCredit": "310"}
        ],
        pending_subjects=[
            # MED-503 es llave de alta prioridad (destranca 3 materias pendientes)
            {"codeSubject": "MED-503", "subject": "FISIOLOGIA HUMANA II", "credits": 4, "lyrics": "", "number": "", "semester": "", "observations": "", "codeRequired": "MED-402", "pensumCredit": "310"},
            {"codeSubject": "MED-504", "subject": "FARMACOLOGIA MEDICA", "credits": 4, "lyrics": "", "number": "", "semester": "", "observations": "", "codeRequired": "MED-401", "pensumCredit": "310"},
            {"codeSubject": "MED-505", "subject": "MICROBIOLOGIA CLINICA", "credits": 3, "lyrics": "", "number": "", "semester": "", "observations": "", "codeRequired": "MED-403", "pensumCredit": "310"},
            
            # Materias dependientes de MED-503
            {"codeSubject": "MED-601", "subject": "FISIOPATOLOGIA I", "credits": 5, "lyrics": "", "number": "", "semester": "", "observations": "", "codeRequired": "MED-503", "pensumCredit": "310"},
            {"codeSubject": "MED-602", "subject": "FARMACOLOGIA CLINICA", "credits": 4, "lyrics": "", "number": "", "semester": "", "observations": "", "codeRequired": "MED-503", "pensumCredit": "310"},
            {"codeSubject": "MED-603", "subject": "SEMIOLOGIA CLINICA", "credits": 4, "lyrics": "", "number": "", "semester": "", "observations": "", "codeRequired": "MED-503", "pensumCredit": "310"},
            
            # Resto de materias del pensum de medicina (muy largo, representamos como una lista estructurada)
            {"codeSubject": "MED-701", "subject": "PEDIATRIA I", "credits": 5, "lyrics": "", "number": "", "semester": "", "observations": "", "codeRequired": "MED-601", "pensumCredit": "310"},
            {"codeSubject": "MED-702", "subject": "GINECOLOGIA Y OBSTETRICIA I", "credits": 5, "lyrics": "", "number": "", "semester": "", "observations": "", "codeRequired": "MED-601", "pensumCredit": "310"},
            {"codeSubject": "MED-703", "subject": "CIRUGIA GENERAL I", "credits": 5, "lyrics": "", "number": "", "semester": "", "observations": "", "codeRequired": "MED-603", "pensumCredit": "310"},
            {"codeSubject": "MED-801", "subject": "PEDIATRIA II", "credits": 5, "lyrics": "", "number": "", "semester": "", "observations": "", "codeRequired": "MED-701", "pensumCredit": "310"},
            {"codeSubject": "MED-802", "subject": "GINECOLOGIA Y OBSTETRICIA II", "credits": 5, "lyrics": "", "number": "", "semester": "", "observations": "", "codeRequired": "MED-702", "pensumCredit": "310"},
            {"codeSubject": "MED-803", "subject": "CIRUGIA GENERAL II", "credits": 5, "lyrics": "", "number": "", "semester": "", "observations": "", "codeRequired": "MED-703", "pensumCredit": "310"},
            {"codeSubject": "MED-901", "subject": "PSIQUIATRIA", "credits": 4, "lyrics": "", "number": "", "semester": "", "observations": "", "codeRequired": "", "pensumCredit": "310"},
            {"codeSubject": "MED-902", "subject": "SALUD PUBLICA I", "credits": 3, "lyrics": "", "number": "", "semester": "", "observations": "", "codeRequired": "", "pensumCredit": "310"},
            {"codeSubject": "MED-903", "subject": "SALUD PUBLICA II", "credits": 3, "lyrics": "", "number": "", "semester": "", "observations": "", "codeRequired": "MED-902", "pensumCredit": "310"},
            {"codeSubject": "MED-INT-1", "subject": "INTERNADO: MEDICINA INTERNA", "credits": 10, "lyrics": "", "number": "", "semester": "", "observations": "", "codeRequired": "MED-803", "pensumCredit": "310"},
            {"codeSubject": "MED-INT-2", "subject": "INTERNADO: PEDIATRIA", "credits": 10, "lyrics": "", "number": "", "semester": "", "observations": "", "codeRequired": "MED-801", "pensumCredit": "310"},
            {"codeSubject": "MED-INT-3", "subject": "INTERNADO: CIRUGIA", "credits": 10, "lyrics": "", "number": "", "semester": "", "observations": "", "codeRequired": "MED-803", "pensumCredit": "310"},
            {"codeSubject": "MED-INT-4", "subject": "INTERNADO: GINECO-OBSTETRICIA", "credits": 10, "lyrics": "", "number": "", "semester": "", "observations": "", "codeRequired": "MED-802", "pensumCredit": "310"},
            {"codeSubject": "MED-OPT-1", "subject": "OPTATIVA MEDICA I", "credits": 3, "lyrics": "", "number": "", "semester": "", "observations": "", "codeRequired": "", "pensumCredit": "310"},
            {"codeSubject": "MED-OPT-2", "subject": "OPTATIVA MEDICA II", "credits": 3, "lyrics": "", "number": "", "semester": "", "observations": "", "codeRequired": "", "pensumCredit": "310"},
            {"codeSubject": "MED-OPT-3", "subject": "OPTATIVA MEDICA III", "credits": 3, "lyrics": "", "number": "", "semester": "", "observations": "", "codeRequired": "", "pensumCredit": "310"}
        ],
        target_credits=93,  # ~30% de 310
        start_year=2019,
        end_year=2024,
        officially_enrolled=[
            {"codeSubject": "MED-501", "subjectName": "FISIOLOGIA HUMANA II", "credits": "4", "section": "01", "teacher": "Dra. Elizabeth Blackwell"},
            {"codeSubject": "MED-502", "subjectName": "FARMACOLOGIA MEDICA", "credits": "4", "section": "03", "teacher": "Dr. Alexander Fleming"}
        ],
        unofficial_selected=[
            {"codeSubject": "MED-501", "subjectName": "FISIOLOGIA HUMANA II", "credits": "4", "section": "01", "teacher": "Dra. Elizabeth Blackwell"},
            {"codeSubject": "MED-502", "subjectName": "FARMACOLOGIA MEDICA", "credits": "4", "section": "03", "teacher": "Dr. Alexander Fleming"}
        ],
        gpa_target=3.15
    ),

    # 4. MIGUEL RODRIGUEZ - 80% PROGRESO (Avanzado en Derecho, con algunas advertencias)
    "mr21-1402": generate_mock_student(
        matricula="mr21-1402",
        names="MIGUEL ANGEL RODRIGUEZ PEÑA",
        career_name="DERECHO 401/2-10-10",
        id_persona=211402,
        id_carrera=401210,
        base_completed=[
            {"codeSubject": "DER-101", "subject": "INTRODUCCION AL DERECHO I", "credits": "3", "lyrics": "A", "number": "90", "semester": "ENE-ABR 2024", "observations": "", "codeRequired": "", "pensumCredit": "180"},
            {"codeSubject": "DER-102", "subject": "DERECHO ROMANO I", "credits": "3", "lyrics": "A", "number": "92", "semester": "ENE-ABR 2024", "observations": "", "codeRequired": "", "pensumCredit": "180"},
            {"codeSubject": "DER-201", "subject": "DERECHO CIVIL I", "credits": "4", "lyrics": "B", "number": "85", "semester": "MAY-AGO 2024", "observations": "", "codeRequired": "DER-101", "pensumCredit": "180"},
            {"codeSubject": "DER-202", "subject": "DERECHO PENAL GENERAL", "credits": "4", "lyrics": "A", "number": "94", "semester": "MAY-AGO 2024", "observations": "", "codeRequired": "DER-101", "pensumCredit": "180"},
            {"codeSubject": "DER-301", "subject": "DERECHO CONSTITUCIONAL I", "credits": "3", "lyrics": "A", "number": "91", "semester": "SEP-DIC 2024", "observations": "", "codeRequired": "DER-101", "pensumCredit": "180"},
            {"codeSubject": "DER-302", "subject": "DERECHO COMERCIAL I", "credits": "3", "lyrics": "B", "number": "87", "semester": "SEP-DIC 2024", "observations": "", "codeRequired": "DER-201", "pensumCredit": "180"},
            {"codeSubject": "DER-401", "subject": "DERECHO PROCESAL PENAL I", "credits": "4", "lyrics": "A", "number": "93", "semester": "ENE-ABR 2025", "observations": "", "codeRequired": "DER-202", "pensumCredit": "180"}
        ],
        pending_subjects=[
            # DER-503 es prerrequisito clave para el resto
            {"codeSubject": "DER-503", "subject": "DERECHO PROCESAL CIVIL I", "credits": 4, "lyrics": "", "number": "", "semester": "", "observations": "", "codeRequired": "DER-201", "pensumCredit": "180"},
            {"codeSubject": "DER-504", "subject": "DERECHO CONSTITUCIONAL II", "credits": 3, "lyrics": "", "number": "", "semester": "", "observations": "", "codeRequired": "DER-301", "pensumCredit": "180"},
            
            # Materias dependientes
            {"codeSubject": "DER-601", "subject": "DERECHO PROCESAL CIVIL II", "credits": 4, "lyrics": "", "number": "", "semester": "", "observations": "", "codeRequired": "DER-503", "pensumCredit": "180"},
            {"codeSubject": "DER-602", "subject": "DERECHO INTERNACIONAL PUBLICO", "credits": 3, "lyrics": "", "number": "", "semester": "", "observations": "", "codeRequired": "DER-504", "pensumCredit": "180"},
            {"codeSubject": "DER-701", "subject": "FILOSOFIA DEL DERECHO", "credits": 3, "lyrics": "", "number": "", "semester": "", "observations": "", "codeRequired": "", "pensumCredit": "180"},
            {"codeSubject": "DER-801", "subject": "PRACTICA FORENSE I", "credits": 4, "lyrics": "", "number": "", "semester": "", "observations": "", "codeRequired": "DER-601", "pensumCredit": "180"},
            {"codeSubject": "DER-901", "subject": "TRABAJO DE GRADO DER", "credits": 6, "lyrics": "", "number": "", "semester": "", "observations": "", "codeRequired": "DER-801", "pensumCredit": "180"}
        ],
        target_credits=144,  # ~80% de 180
        start_year=2021,
        end_year=2024,
        officially_enrolled=[
            {"codeSubject": "DER-501", "subjectName": "DERECHO CONSTITUCIONAL II", "credits": "3", "section": "02", "teacher": "Dra. Ruth Bader Ginsburg"},
            {"codeSubject": "DER-502", "subjectName": "DERECHO PROCESAL CIVIL I", "credits": "4", "section": "01", "teacher": "Dr. Andrés Bello"}
        ],
        unofficial_selected=[
            {"codeSubject": "DER-501", "subjectName": "DERECHO CONSTITUCIONAL II", "credits": "3", "section": "02", "teacher": "Dra. Ruth Bader Ginsburg"},
            {"codeSubject": "DER-502", "subjectName": "DERECHO PROCESAL CIVIL I", "credits": "4", "section": "01", "teacher": "Dr. Andrés Bello"}
        ],
        gpa_target=3.65
    )
}
