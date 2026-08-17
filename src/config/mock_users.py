# src/config/mock_users.py
# Perfiles mock basados en los pensums reales de la UNPHU (2024-2025).
# Cuatro carreras, cuatro situaciones académicas distintas, incluyendo un
# estudiante en condición crítica (GPA 2.2) para ejercitar las alertas del sistema.

import math


def generate_mock_student(
    matricula: str,
    names: str,
    career_name: str,
    id_persona: int,
    id_carrera: int,
    completed_subjects: list,
    pending_subjects: list,
    total_pensum_credits: int,
    start_year: int,
    end_year: int,
    officially_enrolled: list,
    unofficial_selected: list,
    gpa_target: float,
    facultad: str = "",
) -> dict:
    """
    Genera el historial académico completo y las calificaciones detalladas
    por rubro (ASIS, ACUM1, ACUM2, ACUM3, Eval_Final) para asegurar consistencia
    matemática entre la vista del Historial y la vista de Calificaciones del Semestre.

    completed_subjects: lista de materias ya aprobadas, cada una con
        codeSubject, subject, credits, lyrics, number, semester, observations,
        codeRequired, pensumCredit.
    pending_subjects: lista de materias aún no cursadas (sin lyrics ni number).
    total_pensum_credits: créditos totales del pensum de la carrera.
    """

    # 1. Asignar semestres a materias sin uno asignado (no debería ocurrir
    #    en este refactor, pero se conserva como fallback por seguridad).
    semesters = []
    for yr in range(start_year, end_year + 1):
        for per in [1, 2, 3]:
            semesters.append(f"{yr}-{per}")

    period_labels = {1: 'ENE-ABR', 2: 'MAY-AGO', 3: 'SEP-DIC'}

    def get_semester_label(sem_key: str) -> str:
        yr, per = sem_key.split('-')
        return f"{period_labels[int(per)]} {yr}"

    sem_idx = 0
    for sub in completed_subjects:
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

    # 2. Feedback loop para converger al GPA objetivo
    points_map = {'A': 4, 'B': 3, 'C': 2}
    total_points = 0
    total_credits_acc = 0

    for sub in completed_subjects:
        if sub.get('lyrics'):
            lyr = sub['lyrics']
            pts = points_map.get(lyr, 4)
            total_points += pts * int(sub['credits'])
            total_credits_acc += int(sub['credits'])

    for sub in completed_subjects:
        if not sub.get('lyrics'):
            current_gpa = total_points / total_credits_acc if total_credits_acc > 0 else 0
            if current_gpa < gpa_target:
                lyr = 'A'
                num = 93
            elif current_gpa - gpa_target > 0.15:
                lyr = 'C'
                num = 78
            else:
                lyr = 'B'
                num = 85
            sub['lyrics'] = lyr
            sub['number'] = str(num)
            pts = points_map[lyr]
            total_points += pts * int(sub['credits'])
            total_credits_acc += int(sub['credits'])

    # 3. Agrupar por semestre
    by_semester: dict = {}
    for sem in semesters:
        by_semester[sem] = []

    for sub in completed_subjects:
        sem_key = sub['_sem_key']
        if sem_key in by_semester:
            by_semester[sem_key].append(sub)

    # 4. Calcular índices y calificaciones detalladas por rubro
    semester_grades: dict = {}
    cum_points = 0
    cum_credits = 0

    for sem_key in semesters:
        subs = by_semester[sem_key]
        if not subs:
            continue

        sem_points = 0
        sem_cred = 0
        for sub in subs:
            cred = int(sub['credits'])
            pts = points_map.get(sub['lyrics'], 4)
            sem_points += pts * cred
            sem_cred += cred
            cum_points += pts * cred
            cum_credits += cred

        sem_gpa = sem_points / sem_cred if sem_cred > 0 else 0.0
        cum_gpa = cum_points / cum_credits if cum_credits > 0 else 0.0

        yr, per = sem_key.split('-')
        sem_rows = []
        for sub in subs:
            code = sub["codeSubject"]
            name = sub["subject"]
            cred = int(sub["credits"])
            lyr = sub["lyrics"]
            num_grade = int(sub.get("number") or 85)

            rubros_list = [
                ("ASIS",      1,                          1),
                ("ACUM1",     round(num_grade * 0.3, 1),  30),
                ("ACUM2",     round(num_grade * 0.3, 1),  30),
                ("ACUM3",     round(num_grade * 0.1, 1),  10),
                ("Eval_Final",round(num_grade * 0.3, 1),  30),
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
                    "period": int(per),
                })

        semester_grades[sem_key] = sem_rows

    # Limpiar claves internas antes de entregar
    for sub in completed_subjects:
        sub.pop('_sem_key', None)

    final_pending_grades = completed_subjects + pending_subjects

    return {
        "student_data": {
            "id": id_persona,
            "names": names,
            "username": matricula,
            "email": f"{matricula}@unphu.edu.do",
            "career": career_name,
            "enclosure": "Santo Domingo",
            "enrollment": matricula.split('-')[-1],
            "userType": "STUDENT",
            "facultad": facultad,
        },
        "careers": [
            {
                "IdPersona": id_persona,
                "IdCarrera": id_carrera,
                "NombreCarrera": career_name,
            }
        ],
        "pending_grades": final_pending_grades,
        "semester_grades": semester_grades,
        "officially_enrolled": officially_enrolled,
        "unofficial_selected": unofficial_selected,
    }


# ──────────────────────────────────────────────────────────────────────────────
# PENSUMS REALES
# Total de créditos por carrera:
#   Ing. Sistemas              → 217 CR  (13 períodos)
#   Arquitectura               → 236 CR  (12 períodos)
#   Doctor en Medicina         → 310 CR  (16 períodos)
#   Licenciatura en Derecho    → 249 CR  (13 períodos)
# ──────────────────────────────────────────────────────────────────────────────

# Créditos ya aprobados por período (extraídos directamente del pensum):
# INF: P1=18, P2=18, P3=21, P4=21, P5=19, P6=20, P7=22 → completados hasta P7 parcial

_pensum_credit_inf = "220"
_pensum_credit_arq = "225"
_pensum_credit_med = "418"
_pensum_credit_der = "262"


MOCK_USERS = {

    # ──────────────────────────────────────────────────────────────────────────
    # 1. MARÍA SANTOS — Ing. Sistemas — ~60% completado (período 7-8) — GPA 3.80
    #    Situación: estudiante destacada, en la recta final de la carrera.
    # ──────────────────────────────────────────────────────────────────────────
    "ms21-2083": generate_mock_student(
        matricula="ms21-2083",
        names="MARIA ELENA SANTOS ALMONTE",
        career_name=f"INGENIERIA EN SISTEMAS COMPUTACIONALES {_pensum_credit_inf}/3-15-15",
        id_persona=212083,
        id_carrera=251,
        total_pensum_credits=217,
        start_year=2024,
        end_year=2026,

        # Períodos 1-7 completados (119 CR) + algunas del P8
        completed_subjects=[
            # ── Período 1 ──
            {"codeSubject": "MAT-160", "subject": "ÁLGEBRA SUPERIOR",                              "credits": "5", "lyrics": "A", "number": "95", "semester": "ENE-ABR 2024", "observations": "", "codeRequired": "",                  "pensumCredit": _pensum_credit_inf},
            {"codeSubject": "ELT-001", "subject": "ELECTIVA I (ARTES Y DEPORTES)",                 "credits": "1", "lyrics": "A", "number": "90", "semester": "ENE-ABR 2024", "observations": "", "codeRequired": "",                  "pensumCredit": _pensum_credit_inf},
            {"codeSubject": "INF-158", "subject": "INTRODUCCIÓN A LA INFORMÁTICA",                 "credits": "5", "lyrics": "A", "number": "93", "semester": "ENE-ABR 2024", "observations": "", "codeRequired": "",                  "pensumCredit": _pensum_credit_inf},
            {"codeSubject": "LET-101", "subject": "LENGUA ESPAÑOLA Y TÉCNICA DE LA EXPRESIÓN I",  "credits": "3", "lyrics": "B", "number": "88", "semester": "ENE-ABR 2024", "observations": "", "codeRequired": "",                  "pensumCredit": _pensum_credit_inf},
            {"codeSubject": "ORI-100", "subject": "ORIENTACIÓN UNIVERSITARIA",                     "credits": "1", "lyrics": "A", "number": "92", "semester": "ENE-ABR 2024", "observations": "", "codeRequired": "",                  "pensumCredit": _pensum_credit_inf},
            {"codeSubject": "EDU-107", "subject": "TÉCNICAS DEL ESTUDIO E INVESTIGACIÓN",         "credits": "3", "lyrics": "B", "number": "86", "semester": "ENE-ABR 2024", "observations": "", "codeRequired": "",                  "pensumCredit": _pensum_credit_inf},
            # ── Período 2 ──
            {"codeSubject": "MAT-170", "subject": "CÁLCULO DIFERENCIAL E INTEGRAL I",             "credits": "3", "lyrics": "A", "number": "94", "semester": "MAY-AGO 2024", "observations": "", "codeRequired": "MAT-160",           "pensumCredit": _pensum_credit_inf},
            {"codeSubject": "FIS-211", "subject": "FÍSICA GENERAL I",                             "credits": "3", "lyrics": "B", "number": "87", "semester": "MAY-AGO 2024", "observations": "", "codeRequired": "MAT-160",           "pensumCredit": _pensum_credit_inf},
            {"codeSubject": "INF-160", "subject": "FUNDAMENTOS DE INGENIERÍA DE SISTEMAS",        "credits": "3", "lyrics": "A", "number": "91", "semester": "MAY-AGO 2024", "observations": "", "codeRequired": "EDU-107,INF-158",  "pensumCredit": _pensum_credit_inf},
            {"codeSubject": "LEX-110", "subject": "INGLÉS BÁSICO",                                "credits": "3", "lyrics": "B", "number": "85", "semester": "MAY-AGO 2024", "observations": "", "codeRequired": "",                  "pensumCredit": _pensum_credit_inf},
            {"codeSubject": "LET-102", "subject": "LENGUA ESPAÑOLA Y TÉCNICA DE LA EXPRESIÓN II", "credits": "3", "lyrics": "B", "number": "84", "semester": "MAY-AGO 2024", "observations": "", "codeRequired": "LET-101",          "pensumCredit": _pensum_credit_inf},
            {"codeSubject": "QUI-111", "subject": "QUÍMICA GENERAL I",                            "credits": "3", "lyrics": "C", "number": "79", "semester": "MAY-AGO 2024", "observations": "", "codeRequired": "",                  "pensumCredit": _pensum_credit_inf},
            # ── Período 3 ──
            {"codeSubject": "MAT-271", "subject": "CÁLCULO DIFERENCIAL E INTEGRAL II",            "credits": "3", "lyrics": "A", "number": "96", "semester": "SEP-DIC 2024", "observations": "", "codeRequired": "MAT-170",          "pensumCredit": _pensum_credit_inf},
            {"codeSubject": "FIS-212", "subject": "FÍSICA GENERAL II",                            "credits": "3", "lyrics": "B", "number": "88", "semester": "SEP-DIC 2024", "observations": "", "codeRequired": "FIS-211",           "pensumCredit": _pensum_credit_inf},
            {"codeSubject": "HUM-150", "subject": "HISTORIA DE LA CULTURA UNIVERSAL",             "credits": "3", "lyrics": "B", "number": "83", "semester": "SEP-DIC 2024", "observations": "", "codeRequired": "",                  "pensumCredit": _pensum_credit_inf},
            {"codeSubject": "LEX-125", "subject": "INGLÉS TÉCNICO I",                             "credits": "3", "lyrics": "A", "number": "90", "semester": "SEP-DIC 2024", "observations": "", "codeRequired": "LEX-110",           "pensumCredit": _pensum_credit_inf},
            {"codeSubject": "INF-161", "subject": "INTRODUCCIÓN A LA PROGRAMACIÓN",               "credits": "4", "lyrics": "A", "number": "95", "semester": "SEP-DIC 2024", "observations": "", "codeRequired": "INF-160",          "pensumCredit": _pensum_credit_inf},
            {"codeSubject": "FIS-221", "subject": "LABORATORIO DE FÍSICA GENERAL I",              "credits": "1", "lyrics": "A", "number": "92", "semester": "SEP-DIC 2024", "observations": "", "codeRequired": "FIS-211",           "pensumCredit": _pensum_credit_inf},
            {"codeSubject": "MAT-362", "subject": "MATEMÁTICA DISCRETA I",                        "credits": "4", "lyrics": "A", "number": "93", "semester": "SEP-DIC 2024", "observations": "", "codeRequired": "MAT-160",           "pensumCredit": _pensum_credit_inf},
            # ── Período 4 ──
            {"codeSubject": "INF-241", "subject": "ALGORITMOS Y ESTRUCTURAS DE DATOS",            "credits": "4", "lyrics": "A", "number": "97", "semester": "ENE-ABR 2025", "observations": "", "codeRequired": "INF-161",           "pensumCredit": _pensum_credit_inf},
            {"codeSubject": "INF-271", "subject": "ARQUITECTURA DE COMPUTADORES I",               "credits": "3", "lyrics": "A", "number": "91", "semester": "ENE-ABR 2025", "observations": "", "codeRequired": "INF-161",           "pensumCredit": _pensum_credit_inf},
            {"codeSubject": "MAT-371", "subject": "ECUACIONES DIFERENCIALES",                     "credits": "3", "lyrics": "B", "number": "86", "semester": "ENE-ABR 2025", "observations": "", "codeRequired": "MAT-271",           "pensumCredit": _pensum_credit_inf},
            {"codeSubject": "ING-110", "subject": "GEOMETRÍA DESCRIPTIVA",                        "credits": "3", "lyrics": "B", "number": "84", "semester": "ENE-ABR 2025", "observations": "", "codeRequired": "",                  "pensumCredit": _pensum_credit_inf},
            {"codeSubject": "LEX-126", "subject": "INGLÉS TÉCNICO II",                            "credits": "3", "lyrics": "A", "number": "90", "semester": "ENE-ABR 2025", "observations": "", "codeRequired": "LEX-125",           "pensumCredit": _pensum_credit_inf},
            {"codeSubject": "FIS-222", "subject": "LABORATORIO DE FÍSICA GENERAL II",             "credits": "1", "lyrics": "A", "number": "92", "semester": "ENE-ABR 2025", "observations": "", "codeRequired": "FIS-212,FIS-221",   "pensumCredit": _pensum_credit_inf},
            {"codeSubject": "MAT-363", "subject": "MATEMÁTICA DISCRETA II",                       "credits": "4", "lyrics": "A", "number": "94", "semester": "ENE-ABR 2025", "observations": "", "codeRequired": "MAT-362",           "pensumCredit": _pensum_credit_inf},
            # ── Período 5 ──
            {"codeSubject": "INF-273", "subject": "ARQUITECTURA DE COMPUTADORES II",              "credits": "3", "lyrics": "A", "number": "92", "semester": "MAY-AGO 2025", "observations": "", "codeRequired": "INF-271",           "pensumCredit": _pensum_credit_inf},
            {"codeSubject": "MAT-330", "subject": "ESTADÍSTICA PARA INGENIEROS",                  "credits": "3", "lyrics": "A", "number": "93", "semester": "MAY-AGO 2025", "observations": "", "codeRequired": "MAT-170",           "pensumCredit": _pensum_credit_inf},
            {"codeSubject": "FIS-213", "subject": "FÍSICA GENERAL III",                           "credits": "3", "lyrics": "B", "number": "88", "semester": "MAY-AGO 2025", "observations": "", "codeRequired": "FIS-212",           "pensumCredit": _pensum_credit_inf},
            {"codeSubject": "INF-272", "subject": "INGENIERÍA DE SOFTWARE I",                     "credits": "3", "lyrics": "A", "number": "95", "semester": "MAY-AGO 2025", "observations": "", "codeRequired": "INF-161",           "pensumCredit": _pensum_credit_inf},
            {"codeSubject": "MAT-420", "subject": "MÉTODOS NUMÉRICOS Y PROGRAMACIÓN",             "credits": "3", "lyrics": "B", "number": "86", "semester": "MAY-AGO 2025", "observations": "", "codeRequired": "MAT-271",           "pensumCredit": _pensum_credit_inf},
            {"codeSubject": "INF-274", "subject": "PROGRAMACIÓN WEB I",                           "credits": "4", "lyrics": "A", "number": "96", "semester": "MAY-AGO 2025", "observations": "", "codeRequired": "INF-241",           "pensumCredit": _pensum_credit_inf},
            # ── Período 6 ──
            {"codeSubject": "MAT-260", "subject": "ÁLGEBRA LINEAL",                               "credits": "3", "lyrics": "A", "number": "94", "semester": "SEP-DIC 2025", "observations": "", "codeRequired": "MAT-160",           "pensumCredit": _pensum_credit_inf},
            {"codeSubject": "MAT-339", "subject": "INFERENCIA ESTADÍSTICA",                       "credits": "3", "lyrics": "B", "number": "87", "semester": "SEP-DIC 2025", "observations": "", "codeRequired": "MAT-330",           "pensumCredit": _pensum_credit_inf},
            {"codeSubject": "INF-275", "subject": "INGENIERÍA DE SOFTWARE II",                    "credits": "3", "lyrics": "A", "number": "93", "semester": "SEP-DIC 2025", "observations": "", "codeRequired": "INF-272",           "pensumCredit": _pensum_credit_inf},
            {"codeSubject": "FIS-223", "subject": "LABORATORIO DE FÍSICA GENERAL III",            "credits": "1", "lyrics": "A", "number": "91", "semester": "SEP-DIC 2025", "observations": "", "codeRequired": "FIS-213,FIS-222",   "pensumCredit": _pensum_credit_inf},
            {"codeSubject": "INF-276", "subject": "PROGRAMACIÓN DE APLICACIONES MÓVILES",         "credits": "4", "lyrics": "A", "number": "97", "semester": "SEP-DIC 2025", "observations": "", "codeRequired": "INF-274",           "pensumCredit": _pensum_credit_inf},
            {"codeSubject": "INF-278", "subject": "SISTEMAS CIBERFÍSICOS I",                      "credits": "3", "lyrics": "B", "number": "85", "semester": "SEP-DIC 2025", "observations": "", "codeRequired": "INF-272,INF-273",   "pensumCredit": _pensum_credit_inf},
            {"codeSubject": "INF-277", "subject": "SISTEMAS DE BASE DE DATOS I",                  "credits": "3", "lyrics": "A", "number": "92", "semester": "SEP-DIC 2025", "observations": "", "codeRequired": "INF-272",           "pensumCredit": _pensum_credit_inf},
            # ── Período 7 ──
            {"codeSubject": "INF-374", "subject": "CIBERSEGURIDAD EN SOFTWARE",                   "credits": "3", "lyrics": "A", "number": "95", "semester": "ENE-ABR 2026", "observations": "", "codeRequired": "INF-273,INF-275,MAT-420", "pensumCredit": _pensum_credit_inf},
            {"codeSubject": "INF-373", "subject": "ELECTRÓNICA DE SISTEMAS CIBERFÍSICOS",         "credits": "3", "lyrics": "B", "number": "86", "semester": "ENE-ABR 2026", "observations": "", "codeRequired": "FIS-223,INF-278,MAT-371", "pensumCredit": _pensum_credit_inf},
            {"codeSubject": "INF-370", "subject": "INGENIERÍA DE SOFTWARE III",                   "credits": "3", "lyrics": "A", "number": "94", "semester": "ENE-ABR 2026", "observations": "", "codeRequired": "INF-275",           "pensumCredit": _pensum_credit_inf},
            {"codeSubject": "INF-270", "subject": "PROGRAMACIÓN CON MATRICES",                    "credits": "4", "lyrics": "A", "number": "96", "semester": "ENE-ABR 2026", "observations": "", "codeRequired": "INF-158,MAT-260",   "pensumCredit": _pensum_credit_inf},
            {"codeSubject": "INF-386", "subject": "PROYECTO INTEGRADOR EN SOFTWARE Y BASES DE DATOS", "credits": "3", "lyrics": "A", "number": "98", "semester": "ENE-ABR 2026", "observations": "", "codeRequired": "INF-275,INF-276,INF-277", "pensumCredit": _pensum_credit_inf},
            {"codeSubject": "INF-372", "subject": "REDES DE COMPUTADORAS I",                      "credits": "3", "lyrics": "A", "number": "93", "semester": "ENE-ABR 2026", "observations": "", "codeRequired": "INF-160",           "pensumCredit": _pensum_credit_inf},
            {"codeSubject": "INF-371", "subject": "SISTEMAS DE BASES DE DATOS II",                "credits": "3", "lyrics": "A", "number": "95", "semester": "ENE-ABR 2026", "observations": "", "codeRequired": "INF-277",           "pensumCredit": _pensum_credit_inf},
            # ── Período 8 (MAY-AGO 2026) ──
            {"codeSubject": "INF-379", "subject": "INTRODUCCIÓN A LA CIENCIA DE DATOS",           "credits": "3", "lyrics": "A", "number": "97", "semester": "MAY-AGO 2026", "observations": "", "codeRequired": "INF-270,INF-277,MAT-339", "pensumCredit": _pensum_credit_inf},
            {"codeSubject": "ECO-100", "subject": "INTRODUCCIÓN A LA ECONOMÍA",                   "credits": "3", "lyrics": "B", "number": "87", "semester": "MAY-AGO 2026", "observations": "", "codeRequired": "MAT-160",           "pensumCredit": _pensum_credit_inf},
            {"codeSubject": "ADM-105", "subject": "PRINCIPIOS DE ADMINISTRACIÓN",                 "credits": "3", "lyrics": "A", "number": "91", "semester": "MAY-AGO 2026", "observations": "", "codeRequired": "",                  "pensumCredit": _pensum_credit_inf},
        ],

        # Materias restantes del P8-P13
        pending_subjects=[
            {"codeSubject": "INF-375", "subject": "PRINCIPIOS DE SISTEMAS OPERATIVOS",             "credits": 3, "lyrics": "", "number": "", "semester": "", "observations": "", "codeRequired": "INF-273",                   "pensumCredit": _pensum_credit_inf},
            {"codeSubject": "INF-377", "subject": "REDES DE COMPUTADORAS II",                      "credits": 3, "lyrics": "", "number": "", "semester": "", "observations": "", "codeRequired": "INF-372",                   "pensumCredit": _pensum_credit_inf},
            {"codeSubject": "INF-378", "subject": "SISTEMAS CIBERFÍSICOS II",                      "credits": 3, "lyrics": "", "number": "", "semester": "", "observations": "", "codeRequired": "INF-372,INF-373",           "pensumCredit": _pensum_credit_inf},
            # P9
            {"codeSubject": "INF-380", "subject": "ADMINISTRACIÓN DE PROYECTOS",                   "credits": 3, "lyrics": "", "number": "", "semester": "", "observations": "", "codeRequired": "ADM-105",                   "pensumCredit": _pensum_credit_inf},
            {"codeSubject": "INF-384", "subject": "AUDITORÍA DE SISTEMAS COMPUTACIONALES",         "credits": 3, "lyrics": "", "number": "", "semester": "", "observations": "", "codeRequired": "ADM-105,INF-370,INF-374",   "pensumCredit": _pensum_credit_inf},
            {"codeSubject": "INF-376", "subject": "DATA WAREHOUSING",                              "credits": 3, "lyrics": "", "number": "", "semester": "", "observations": "", "codeRequired": "INF-379,INF-276,INF-374,INF-371", "pensumCredit": _pensum_credit_inf},
            {"codeSubject": "ING-540", "subject": "INTRODUCCIÓN A LA INGENIERÍA ECONÓMICA",        "credits": 3, "lyrics": "", "number": "", "semester": "", "observations": "", "codeRequired": "ECO-100",                   "pensumCredit": _pensum_credit_inf},
            {"codeSubject": "INF-383", "subject": "MACHINE LEARNING",                              "credits": 3, "lyrics": "", "number": "", "semester": "", "observations": "", "codeRequired": "INF-371,INF-379",           "pensumCredit": _pensum_credit_inf},
            {"codeSubject": "INF-381", "subject": "SISTEMAS CIBERFÍSICOS III",                     "credits": 3, "lyrics": "", "number": "", "semester": "", "observations": "", "codeRequired": "INF-270,INF-377,INF-378",   "pensumCredit": _pensum_credit_inf},
            # P10
            {"codeSubject": "INF-434", "subject": "BIG DATA CON PROCESAMIENTO DISTRIBUIDO",        "credits": 3, "lyrics": "", "number": "", "semester": "", "observations": "", "codeRequired": "INF-375,INF-379",           "pensumCredit": _pensum_credit_inf},
            {"codeSubject": "INF-385", "subject": "VIRTUALIZACIÓN Y COMPUTACIÓN EN LA NUBE",       "credits": 3, "lyrics": "", "number": "", "semester": "", "observations": "", "codeRequired": "INF-378,INF-375,INF-371,INF-379", "pensumCredit": _pensum_credit_inf},
            {"codeSubject": "INF-432", "subject": "INNOVACIÓN Y EMPRENDIMIENTO EN INGENIERÍA",     "credits": 3, "lyrics": "", "number": "", "semester": "", "observations": "", "codeRequired": "ING-540",                   "pensumCredit": _pensum_credit_inf},
            {"codeSubject": "INF-431", "subject": "MINERÍA DE DATOS",                              "credits": 3, "lyrics": "", "number": "", "semester": "", "observations": "", "codeRequired": "INF-383",                   "pensumCredit": _pensum_credit_inf},
            {"codeSubject": "INF-435", "subject": "PROYECTO INTEGRADOR EN SISTEMAS CIBERFÍSICOS",  "credits": 3, "lyrics": "", "number": "", "semester": "", "observations": "", "codeRequired": "INF-380,INF-381",           "pensumCredit": _pensum_credit_inf},
            # P11
            {"codeSubject": "INF-433", "subject": "ELECTIVA IV",                                   "credits": 3, "lyrics": "", "number": "", "semester": "", "observations": "", "codeRequired": "INF-276,INF-374,INF-381,INF-383", "pensumCredit": _pensum_credit_inf},
            {"codeSubject": "INI-510", "subject": "ÉTICA PROFESIONAL",                             "credits": 3, "lyrics": "", "number": "", "semester": "", "observations": "", "codeRequired": "",                          "pensumCredit": _pensum_credit_inf},
            {"codeSubject": "INF-440", "subject": "MODELADO Y SIMULACIÓN DE SISTEMAS CIBERFÍSICOS","credits": 3, "lyrics": "", "number": "", "semester": "", "observations": "", "codeRequired": "INF-435",                   "pensumCredit": _pensum_credit_inf},
            {"codeSubject": "INF-438", "subject": "PROYECTO INTEGRADOR EN CIENCIAS E INGENIERÍA DE DATOS", "credits": 3, "lyrics": "", "number": "", "semester": "", "observations": "", "codeRequired": "INF-380,INF-383,INF-431,INF-434", "pensumCredit": _pensum_credit_inf},
            {"codeSubject": "INF-437", "subject": "SEMINARIO DE INVESTIGACIÓN PARA TRABAJO DE GRADO", "credits": 3, "lyrics": "", "number": "", "semester": "", "observations": "", "codeRequired": "INF-380,INF-432",       "pensumCredit": _pensum_credit_inf},
            # P12
            {"codeSubject": "INF-905", "subject": "PASANTÍA FINAL",                                "credits": 8, "lyrics": "", "number": "", "semester": "", "observations": "", "codeRequired": "INF-386,INF-432,INF-435,INF-438,INI-510", "pensumCredit": _pensum_credit_inf},
            # P13
            {"codeSubject": "INF-901", "subject": "TRABAJO DE GRADO (ING. DE SISTEMAS)",           "credits": 6, "lyrics": "", "number": "", "semester": "", "observations": "", "codeRequired": "INF-437",                   "pensumCredit": _pensum_credit_inf},
        ],

        officially_enrolled=[
            {"codeSubject": "INF-375", "subjectName": "PRINCIPIOS DE SISTEMAS OPERATIVOS",        "credits": "3", "section": "01", "teacher": "Dr. Alan Turing"},
            {"codeSubject": "INF-377", "subjectName": "REDES DE COMPUTADORAS II",                 "credits": "3", "section": "02", "teacher": "Ing. Vint Cerf"},
            {"codeSubject": "INF-378", "subjectName": "SISTEMAS CIBERFÍSICOS II",                 "credits": "3", "section": "01", "teacher": "Dra. Ada Lovelace"},
        ],
        unofficial_selected=[
            {"codeSubject": "INF-375", "subjectName": "PRINCIPIOS DE SISTEMAS OPERATIVOS",        "credits": "3", "section": "01", "teacher": "Dr. Alan Turing"},
            {"codeSubject": "INF-377", "subjectName": "REDES DE COMPUTADORAS II",                 "credits": "3", "section": "02", "teacher": "Ing. Vint Cerf"},
            {"codeSubject": "INF-378", "subjectName": "SISTEMAS CIBERFÍSICOS II",                 "credits": "3", "section": "01", "teacher": "Dra. Ada Lovelace"},
        ],
        gpa_target=3.80,
        facultad="Facultad de Ciencias y Tecnología",
    ),


    # ──────────────────────────────────────────────────────────────────────────
    # 2. CARLOS MENDOZA — Arquitectura — ~50% completado (período 6) — GPA 3.42
    #    Situación: mitad de carrera, buen rendimiento, varias materias de diseño.
    # ──────────────────────────────────────────────────────────────────────────
    "cm22-0941": generate_mock_student(
        matricula="cm22-0941",
        names="CARLOS ALBERTO MENDOZA RUIZ",
        career_name=f"ARQUITECTURA {_pensum_credit_arq}/2-22-22",
        id_persona=220941,
        id_carrera=6,
        total_pensum_credits=236,
        start_year=2022,
        end_year=2025,

        # Períodos 1-6 completados (117 CR)
        completed_subjects=[
            # ── Período 1 ──
            {"codeSubject": "ARQ-601", "subject": "DISEÑO I",                                      "credits": "3", "lyrics": "A", "number": "91", "semester": "ENE-ABR 2022", "observations": "", "codeRequired": "",                  "pensumCredit": _pensum_credit_arq},
            {"codeSubject": "LET-104", "subject": "EXPRESIÓN ORAL Y PRODUCCIÓN ESCRITA",           "credits": "3", "lyrics": "B", "number": "85", "semester": "ENE-ABR 2022", "observations": "", "codeRequired": "",                  "pensumCredit": _pensum_credit_arq},
            {"codeSubject": "ARQ-701", "subject": "GEOMETRÍA DESCRIPTIVA I",                       "credits": "3", "lyrics": "A", "number": "90", "semester": "ENE-ABR 2022", "observations": "", "codeRequired": "",                  "pensumCredit": _pensum_credit_arq},
            {"codeSubject": "HUM-150", "subject": "HISTORIA DE LA CULTURA UNIVERSAL",              "credits": "3", "lyrics": "B", "number": "86", "semester": "ENE-ABR 2022", "observations": "", "codeRequired": "",                  "pensumCredit": _pensum_credit_arq},
            {"codeSubject": "ARQ-600", "subject": "INTRODUCCIÓN A LA CARRERA DE ARQUITECTURA",    "credits": "1", "lyrics": "A", "number": "95", "semester": "ENE-ABR 2022", "observations": "", "codeRequired": "",                  "pensumCredit": _pensum_credit_arq},
            {"codeSubject": "MAT-060", "subject": "MATEMÁTICA BÁSICA",                             "credits": "4", "lyrics": "B", "number": "84", "semester": "ENE-ABR 2022", "observations": "", "codeRequired": "",                  "pensumCredit": _pensum_credit_arq},
            {"codeSubject": "ORI-100", "subject": "ORIENTACIÓN UNIVERSITARIA",                     "credits": "1", "lyrics": "A", "number": "92", "semester": "ENE-ABR 2022", "observations": "", "codeRequired": "",                  "pensumCredit": _pensum_credit_arq},
            {"codeSubject": "ARQ-700", "subject": "PANORAMA DE LAS ARTES",                        "credits": "2", "lyrics": "A", "number": "93", "semester": "ENE-ABR 2022", "observations": "", "codeRequired": "",                  "pensumCredit": _pensum_credit_arq},
            # ── Período 2 ──
            {"codeSubject": "MAT-170", "subject": "CÁLCULO DIFERENCIAL E INTEGRAL I",             "credits": "3", "lyrics": "B", "number": "83", "semester": "MAY-AGO 2022", "observations": "", "codeRequired": "MAT-060",           "pensumCredit": _pensum_credit_arq},
            {"codeSubject": "ARQ-703", "subject": "COMPOSICIÓN",                                   "credits": "2", "lyrics": "A", "number": "90", "semester": "MAY-AGO 2022", "observations": "", "codeRequired": "ARQ-601",           "pensumCredit": _pensum_credit_arq},
            {"codeSubject": "ARQ-705", "subject": "DIBUJO DIGITAL",                               "credits": "2", "lyrics": "A", "number": "94", "semester": "MAY-AGO 2022", "observations": "", "codeRequired": "ARQ-701",           "pensumCredit": _pensum_credit_arq},
            {"codeSubject": "ARQ-602", "subject": "DISEÑO II",                                     "credits": "3", "lyrics": "A", "number": "92", "semester": "MAY-AGO 2022", "observations": "", "codeRequired": "ARQ-601",           "pensumCredit": _pensum_credit_arq},
            {"codeSubject": "ELT-001", "subject": "ELECTIVA I (ARTES Y DEPORTES)",                "credits": "1", "lyrics": "A", "number": "90", "semester": "MAY-AGO 2022", "observations": "", "codeRequired": "",                  "pensumCredit": _pensum_credit_arq},
            {"codeSubject": "ARQ-702", "subject": "GEOMETRÍA DESCRIPTIVA II",                     "credits": "3", "lyrics": "B", "number": "86", "semester": "MAY-AGO 2022", "observations": "", "codeRequired": "ARQ-701",           "pensumCredit": _pensum_credit_arq},
            {"codeSubject": "HUM-160", "subject": "HISTORIA DOMINICANA",                          "credits": "3", "lyrics": "B", "number": "84", "semester": "MAY-AGO 2022", "observations": "", "codeRequired": "",                  "pensumCredit": _pensum_credit_arq},
            {"codeSubject": "ARQ-704", "subject": "REPRESENTACIÓN ARQUITECTÓNICA",                "credits": "3", "lyrics": "A", "number": "91", "semester": "MAY-AGO 2022", "observations": "", "codeRequired": "ARQ-700,ARQ-701",   "pensumCredit": _pensum_credit_arq},
            # ── Período 3 ──
            {"codeSubject": "ARQ-603", "subject": "DISEÑO III",                                    "credits": "3", "lyrics": "A", "number": "92", "semester": "SEP-DIC 2022", "observations": "", "codeRequired": "ARQ-600,ARQ-602,ARQ-704,ORI-100", "pensumCredit": _pensum_credit_arq},
            {"codeSubject": "ARQ-707", "subject": "FABRICACIÓN DIGITAL I",                        "credits": "2", "lyrics": "A", "number": "93", "semester": "SEP-DIC 2022", "observations": "", "codeRequired": "ARQ-703",           "pensumCredit": _pensum_credit_arq},
            {"codeSubject": "FIS-011", "subject": "FÍSICA BÁSICA I",                              "credits": "3", "lyrics": "B", "number": "82", "semester": "SEP-DIC 2022", "observations": "", "codeRequired": "",                  "pensumCredit": _pensum_credit_arq},
            {"codeSubject": "ARQ-400", "subject": "INTERPRETACIÓN ARQUITECTÓNICA Y SU ENTORNO",   "credits": "2", "lyrics": "A", "number": "90", "semester": "SEP-DIC 2022", "observations": "", "codeRequired": "ARQ-700,HUM-150",   "pensumCredit": _pensum_credit_arq},
            {"codeSubject": "SOC-100", "subject": "INTRODUCCIÓN A LA SOCIOLOGÍA",                 "credits": "3", "lyrics": "B", "number": "85", "semester": "SEP-DIC 2022", "observations": "", "codeRequired": "",                  "pensumCredit": _pensum_credit_arq},
            {"codeSubject": "ARQ-232", "subject": "MÉTODOS Y TÉCNICAS DE INVESTIGACIÓN I",        "credits": "2", "lyrics": "A", "number": "88", "semester": "SEP-DIC 2022", "observations": "", "codeRequired": "LET-104",           "pensumCredit": _pensum_credit_arq},
            {"codeSubject": "ARQ-706", "subject": "MODELADO DIGITAL",                             "credits": "2", "lyrics": "A", "number": "95", "semester": "SEP-DIC 2022", "observations": "", "codeRequired": "ARQ-702,ARQ-705",   "pensumCredit": _pensum_credit_arq},
            {"codeSubject": "ING-210", "subject": "TOPOGRAFÍA",                                   "credits": "3", "lyrics": "B", "number": "83", "semester": "SEP-DIC 2022", "observations": "", "codeRequired": "ARQ-704",           "pensumCredit": _pensum_credit_arq},
            # ── Período 4 ──
            {"codeSubject": "ARQ-604", "subject": "DISEÑO IV",                                     "credits": "4", "lyrics": "A", "number": "91", "semester": "ENE-ABR 2023", "observations": "", "codeRequired": "ARQ-603,ARQ-703,ARQ-705", "pensumCredit": _pensum_credit_arq},
            {"codeSubject": "ARQ-570", "subject": "FUNDAMENTOS ESTRUCTURALES I",                  "credits": "3", "lyrics": "B", "number": "85", "semester": "ENE-ABR 2023", "observations": "", "codeRequired": "FIS-011,MAT-170",   "pensumCredit": _pensum_credit_arq},
            {"codeSubject": "ARQ-401", "subject": "HISTORIA DEL ARTE Y LA ARQUITECTURA I",       "credits": "3", "lyrics": "A", "number": "90", "semester": "ENE-ABR 2023", "observations": "", "codeRequired": "ARQ-400",           "pensumCredit": _pensum_credit_arq},
            {"codeSubject": "LEX-110", "subject": "INGLÉS BÁSICO",                                "credits": "3", "lyrics": "B", "number": "83", "semester": "ENE-ABR 2023", "observations": "", "codeRequired": "",                  "pensumCredit": _pensum_credit_arq},
            {"codeSubject": "ARQ-404", "subject": "METODOLOGÍA DEL DISEÑO",                       "credits": "3", "lyrics": "A", "number": "88", "semester": "ENE-ABR 2023", "observations": "", "codeRequired": "ARQ-232",           "pensumCredit": _pensum_credit_arq},
            {"codeSubject": "ARQ-708", "subject": "PRESENTACIÓN DIGITAL",                         "credits": "2", "lyrics": "A", "number": "92", "semester": "ENE-ABR 2023", "observations": "", "codeRequired": "ARQ-704,ARQ-706",   "pensumCredit": _pensum_credit_arq},
            {"codeSubject": "ARQ-800", "subject": "SOCIOLOGÍA URBANA",                            "credits": "3", "lyrics": "B", "number": "84", "semester": "ENE-ABR 2023", "observations": "", "codeRequired": "SOC-100",           "pensumCredit": _pensum_credit_arq},
            # ── Período 5 ──
            {"codeSubject": "ARQ-605", "subject": "DISEÑO V",                                     "credits": "4", "lyrics": "A", "number": "93", "semester": "MAY-AGO 2023", "observations": "", "codeRequired": "ARQ-604",           "pensumCredit": _pensum_credit_arq},
            {"codeSubject": "ARQ-571", "subject": "FUNDAMENTOS ESTRUCTURALES II",                 "credits": "3", "lyrics": "B", "number": "86", "semester": "MAY-AGO 2023", "observations": "", "codeRequired": "ARQ-570",           "pensumCredit": _pensum_credit_arq},
            {"codeSubject": "ARQ-402", "subject": "HISTORIA DEL ARTE Y LA ARQUITECTURA II",      "credits": "3", "lyrics": "A", "number": "91", "semester": "MAY-AGO 2023", "observations": "", "codeRequired": "ARQ-401",           "pensumCredit": _pensum_credit_arq},
            {"codeSubject": "ARQ-709", "subject": "INTRODUCCIÓN AL BIM",                          "credits": "2", "lyrics": "A", "number": "95", "semester": "MAY-AGO 2023", "observations": "", "codeRequired": "ING-210",           "pensumCredit": _pensum_credit_arq},
            {"codeSubject": "ARQ-581", "subject": "MATERIALES Y MÉTODOS DE CONSTRUCCIÓN I",      "credits": "3", "lyrics": "B", "number": "85", "semester": "MAY-AGO 2023", "observations": "", "codeRequired": "ARQ-603",           "pensumCredit": _pensum_credit_arq},
            {"codeSubject": "ADM-105", "subject": "PRINCIPIOS DE ADMINISTRACIÓN",                 "credits": "3", "lyrics": "B", "number": "84", "semester": "MAY-AGO 2023", "observations": "", "codeRequired": "",                  "pensumCredit": _pensum_credit_arq},
            {"codeSubject": "ARQ-801", "subject": "URBANISMO",                                    "credits": "3", "lyrics": "A", "number": "89", "semester": "MAY-AGO 2023", "observations": "", "codeRequired": "ARQ-400",           "pensumCredit": _pensum_credit_arq},
            # ── Período 6 ──
            {"codeSubject": "RNA-100", "subject": "CONCIENCIA MEDIOAMBIENTAL",                    "credits": "1", "lyrics": "A", "number": "92", "semester": "SEP-DIC 2023", "observations": "", "codeRequired": "",                  "pensumCredit": _pensum_credit_arq},
            {"codeSubject": "ARQ-802", "subject": "DISEÑO URBANO I",                              "credits": "3", "lyrics": "A", "number": "90", "semester": "SEP-DIC 2023", "observations": "", "codeRequired": "ARQ-800,ARQ-801",   "pensumCredit": _pensum_credit_arq},
            {"codeSubject": "ARQ-606", "subject": "DISEÑO VI",                                    "credits": "4", "lyrics": "A", "number": "92", "semester": "SEP-DIC 2023", "observations": "", "codeRequired": "ARQ-404,ARQ-605,ARQ-708", "pensumCredit": _pensum_credit_arq},
            {"codeSubject": "ARQ-403", "subject": "HISTORIA DEL ARTE Y LA ARQUITECTURA III",     "credits": "3", "lyrics": "A", "number": "88", "semester": "SEP-DIC 2023", "observations": "", "codeRequired": "ARQ-402",           "pensumCredit": _pensum_credit_arq},
            {"codeSubject": "ARQ-584", "subject": "INSTALACIÓN ELÉCTRICA",                       "credits": "3", "lyrics": "B", "number": "83", "semester": "SEP-DIC 2023", "observations": "", "codeRequired": "ARQ-604",           "pensumCredit": _pensum_credit_arq},
            {"codeSubject": "ARQ-572", "subject": "INTRO. AL DISEÑO DE ESTRUCTURAS DE CONCRETO", "credits": "3", "lyrics": "B", "number": "82", "semester": "SEP-DIC 2023", "observations": "", "codeRequired": "ARQ-571",           "pensumCredit": _pensum_credit_arq},
            {"codeSubject": "ARQ-582", "subject": "MATERIALES Y MÉTODOS DE CONSTRUCCIÓN II",     "credits": "3", "lyrics": "B", "number": "84", "semester": "SEP-DIC 2023", "observations": "", "codeRequired": "ARQ-581",           "pensumCredit": _pensum_credit_arq},
        ],

        pending_subjects=[
            # P7
            {"codeSubject": "ARQ-620", "subject": "ARQUITECTURA BIOCLIMÁTICA",                    "credits": 3, "lyrics": "", "number": "", "semester": "", "observations": "", "codeRequired": "ARQ-605,RNA-100",           "pensumCredit": _pensum_credit_arq},
            {"codeSubject": "ARQ-607", "subject": "DISEÑO VII",                                   "credits": 5, "lyrics": "", "number": "", "semester": "", "observations": "", "codeRequired": "ARQ-606,ARQ-802",           "pensumCredit": _pensum_credit_arq},
            {"codeSubject": "ARQ-405", "subject": "HISTORIA DE LA ARQUITECTURA DOMINICANA",       "credits": 3, "lyrics": "", "number": "", "semester": "", "observations": "", "codeRequired": "ARQ-402,HUM-160",           "pensumCredit": _pensum_credit_arq},
            {"codeSubject": "ARQ-585", "subject": "INSTALACIONES SANITARIAS",                     "credits": 3, "lyrics": "", "number": "", "semester": "", "observations": "", "codeRequired": "ARQ-604",                   "pensumCredit": _pensum_credit_arq},
            {"codeSubject": "ARQ-573", "subject": "INTRO. AL DISEÑO DE ESTRUCTURAS DE ACERO",    "credits": 3, "lyrics": "", "number": "", "semester": "", "observations": "", "codeRequired": "ARQ-571",                   "pensumCredit": _pensum_credit_arq},
            {"codeSubject": "ARQ-583", "subject": "MATERIALES Y MÉTODOS DE CONSTRUCCIÓN III",    "credits": 3, "lyrics": "", "number": "", "semester": "", "observations": "", "codeRequired": "ARQ-582",                   "pensumCredit": _pensum_credit_arq},
            # P8
            {"codeSubject": "ARQ-521", "subject": "ADMINISTRACIÓN Y ORGANIZACIÓN DE OBRAS I",    "credits": 3, "lyrics": "", "number": "", "semester": "", "observations": "", "codeRequired": "ADM-105,ARQ-582,ARQ-709",   "pensumCredit": _pensum_credit_arq},
            {"codeSubject": "ARQ-803", "subject": "ARQUITECTURA DEL PAISAJE",                     "credits": 3, "lyrics": "", "number": "", "semester": "", "observations": "", "codeRequired": "ARQ-802",                   "pensumCredit": _pensum_credit_arq},
            {"codeSubject": "ARQ-608", "subject": "DISEÑO VIII",                                  "credits": 6, "lyrics": "", "number": "", "semester": "", "observations": "", "codeRequired": "ARQ-572,ARQ-573,ARQ-581,ARQ-607,ARQ-709", "pensumCredit": _pensum_credit_arq},
            {"codeSubject": "ELT-002", "subject": "ELECTIVA II",                                  "credits": 3, "lyrics": "", "number": "", "semester": "", "observations": "", "codeRequired": "ARQ-232,ARQ-606,ARQ-707,ARQ-709", "pensumCredit": _pensum_credit_arq},
            {"codeSubject": "LEX-221", "subject": "INGLÉS TÉCNICO PARA ARQUITECTOS",              "credits": 3, "lyrics": "", "number": "", "semester": "", "observations": "", "codeRequired": "LEX-110",                   "pensumCredit": _pensum_credit_arq},
            {"codeSubject": "ARQ-586", "subject": "SISTEMAS Y EQUIPAMIENTOS EN EDIFICACIONES",   "credits": 3, "lyrics": "", "number": "", "semester": "", "observations": "", "codeRequired": "ARQ-620",                   "pensumCredit": _pensum_credit_arq},
            # P9
            {"codeSubject": "ARQ-522", "subject": "ADMINISTRACIÓN Y ORGANIZACIÓN DE OBRAS II",   "credits": 3, "lyrics": "", "number": "", "semester": "", "observations": "", "codeRequired": "ARQ-521",                   "pensumCredit": _pensum_credit_arq},
            {"codeSubject": "ARQ-621", "subject": "DISEÑO DE INTERIORES",                        "credits": 3, "lyrics": "", "number": "", "semester": "", "observations": "", "codeRequired": "ARQ-583,ARQ-606",           "pensumCredit": _pensum_credit_arq},
            {"codeSubject": "ARQ-609", "subject": "DISEÑO IX",                                   "credits": 6, "lyrics": "", "number": "", "semester": "", "observations": "", "codeRequired": "ARQ-584,ARQ-585,ARQ-586,ARQ-608", "pensumCredit": _pensum_credit_arq},
            {"codeSubject": "ELT-003", "subject": "ELECTIVA III",                                 "credits": 3, "lyrics": "", "number": "", "semester": "", "observations": "", "codeRequired": "ARQ-232,ARQ-606,ARQ-707,ARQ-709,ARQ-802", "pensumCredit": _pensum_credit_arq},
            {"codeSubject": "ARQ-622", "subject": "PROYECTO INMOBILIARIO",                        "credits": 2, "lyrics": "", "number": "", "semester": "", "observations": "", "codeRequired": "ARQ-521",                   "pensumCredit": _pensum_credit_arq},
            {"codeSubject": "ARQ-406", "subject": "TEORÍAS DE LA ARQUITECTURA I",                "credits": 3, "lyrics": "", "number": "", "semester": "", "observations": "", "codeRequired": "ARQ-400",                   "pensumCredit": _pensum_credit_arq},
            # P10
            {"codeSubject": "ARQ-804", "subject": "DISEÑO URBANO II",                            "credits": 3, "lyrics": "", "number": "", "semester": "", "observations": "", "codeRequired": "ARQ-802",                   "pensumCredit": _pensum_credit_arq},
            {"codeSubject": "ARQ-610", "subject": "DISEÑO X",                                    "credits": 6, "lyrics": "", "number": "", "semester": "", "observations": "", "codeRequired": "ARQ-609,ARQ-621",           "pensumCredit": _pensum_credit_arq},
            {"codeSubject": "ELT-004", "subject": "ELECTIVA IV",                                  "credits": 3, "lyrics": "", "number": "", "semester": "", "observations": "", "codeRequired": "ARQ-232,ARQ-405,ARQ-606,ARQ-620,ARQ-707,ARQ-709,ARQ-802", "pensumCredit": _pensum_credit_arq},
            {"codeSubject": "ARQ-901", "subject": "FORMULACIÓN DE PROYECTO DE GRADO",            "credits": 3, "lyrics": "", "number": "", "semester": "", "observations": "", "codeRequired": "ARQ-609,LEX-221",           "pensumCredit": _pensum_credit_arq},
            {"codeSubject": "ARQ-407", "subject": "TEORÍAS DE LA ARQUITECTURA II",               "credits": 3, "lyrics": "", "number": "", "semester": "", "observations": "", "codeRequired": "ARQ-406",                   "pensumCredit": _pensum_credit_arq},
            # P11
            {"codeSubject": "ELT-005", "subject": "ELECTIVA V",                                   "credits": 3, "lyrics": "", "number": "", "semester": "", "observations": "", "codeRequired": "ARQ-232,ARQ-405,ARQ-521,ARQ-584,ARQ-606,ARQ-620,ARQ-707,ARQ-709,ARQ-802,ARQ-803", "pensumCredit": _pensum_credit_arq},
            {"codeSubject": "ADM-144", "subject": "EMPRENDIMIENTO",                               "credits": 3, "lyrics": "", "number": "", "semester": "", "observations": "", "codeRequired": "ADM-105",                   "pensumCredit": _pensum_credit_arq},
            {"codeSubject": "ARQ-415", "subject": "GERENCIA Y PRÁCTICA PROFESIONAL",              "credits": 3, "lyrics": "", "number": "", "semester": "", "observations": "", "codeRequired": "ARQ-522",                   "pensumCredit": _pensum_credit_arq},
            {"codeSubject": "ARQ-902", "subject": "PRE-PROYECTO DE GRADO",                        "credits": 6, "lyrics": "", "number": "", "semester": "", "observations": "", "codeRequired": "ARQ-610,ARQ-901",           "pensumCredit": _pensum_credit_arq},
            # P12
            {"codeSubject": "ELT-006", "subject": "ELECTIVA VI",                                  "credits": 3, "lyrics": "", "number": "", "semester": "", "observations": "", "codeRequired": "ARQ-232,ARQ-405,ARQ-406,ARQ-521,ARQ-522,ARQ-584,ARQ-606,ARQ-620,ARQ-707,ARQ-709,ARQ-802,ARQ-803", "pensumCredit": _pensum_credit_arq},
            {"codeSubject": "ARQ-903", "subject": "PROYECTO DE GRADO",                            "credits": 6, "lyrics": "", "number": "", "semester": "", "observations": "", "codeRequired": "ARQ-902",                   "pensumCredit": _pensum_credit_arq},
        ],

        officially_enrolled=[
            {"codeSubject": "ARQ-620", "subjectName": "ARQUITECTURA BIOCLIMÁTICA",                "credits": "3", "section": "01", "teacher": "Arq. Norman Foster"},
            {"codeSubject": "ARQ-607", "subjectName": "DISEÑO VII",                               "credits": "5", "section": "01", "teacher": "Arq. Frank Gehry"},
            {"codeSubject": "ARQ-405", "subjectName": "HISTORIA DE LA ARQUITECTURA DOMINICANA",   "credits": "3", "section": "02", "teacher": "Arq. José Ramón Báez López-Penha"},
        ],
        unofficial_selected=[
            {"codeSubject": "ARQ-620", "subjectName": "ARQUITECTURA BIOCLIMÁTICA",                "credits": "3", "section": "01", "teacher": "Arq. Norman Foster"},
            {"codeSubject": "ARQ-607", "subjectName": "DISEÑO VII",                               "credits": "5", "section": "01", "teacher": "Arq. Frank Gehry"},
            {"codeSubject": "ARQ-405", "subjectName": "HISTORIA DE LA ARQUITECTURA DOMINICANA",   "credits": "3", "section": "02", "teacher": "Arq. José Ramón Báez López-Penha"},
        ],
        gpa_target=3.42,
        facultad="Facultad de Arquitectura y Arte",
    ),


    # ──────────────────────────────────────────────────────────────────────────
    # 3. HILDA PÉREZ — Doctor en Medicina — períodos 1-6 aprobados — GPA 3.16
    #    Situación: terminó las ciencias básicas, entrando al ciclo clínico.
    # ──────────────────────────────────────────────────────────────────────────
    "hp19-0112": generate_mock_student(
        matricula="hp19-0112",
        names="HILDA PATRICIA PEREZ MEJIA",
        career_name=f"MEDICINA {_pensum_credit_med}/1-12-12",
        id_persona=190112,
        id_carrera=301112,
        total_pensum_credits=310,
        start_year=2019,
        end_year=2024,

        # Períodos 1-6 completados (132 CR)
        completed_subjects=[
            # ── Período 1 ──
            {"codeSubject": "BIO-101", "subject": "BIOLOGÍA GENERAL",                             "credits": "4", "lyrics": "B", "number": "87", "semester": "ENE-ABR 2019", "observations": "", "codeRequired": "",      "pensumCredit": _pensum_credit_med},
            {"codeSubject": "ELT-001", "subject": "ELECTIVA I (ARTES Y DEPORTES)",                "credits": "1", "lyrics": "A", "number": "92", "semester": "ENE-ABR 2019", "observations": "", "codeRequired": "",      "pensumCredit": _pensum_credit_med},
            {"codeSubject": "MED-069", "subject": "HISTORIA CIENCIAS DE LA SALUD Y SOCIOLOGÍA MÉDICA", "credits": "3", "lyrics": "B", "number": "85", "semester": "ENE-ABR 2019", "observations": "", "codeRequired": "", "pensumCredit": _pensum_credit_med},
            {"codeSubject": "HUM-150", "subject": "HISTORIA DE LA CULTURA UNIVERSAL",             "credits": "3", "lyrics": "B", "number": "84", "semester": "ENE-ABR 2019", "observations": "", "codeRequired": "",      "pensumCredit": _pensum_credit_med},
            {"codeSubject": "LEX-113", "subject": "INGLÉS INTRODUCTORIO DE CS. DE LA SALUD",     "credits": "3", "lyrics": "B", "number": "82", "semester": "ENE-ABR 2019", "observations": "", "codeRequired": "",      "pensumCredit": _pensum_credit_med},
            {"codeSubject": "LET-101", "subject": "LENGUA ESPAÑOLA Y TÉCNICA DE LA EXPRESIÓN I", "credits": "3", "lyrics": "A", "number": "90", "semester": "ENE-ABR 2019", "observations": "", "codeRequired": "",      "pensumCredit": _pensum_credit_med},
            {"codeSubject": "MAT-060", "subject": "MATEMÁTICA BÁSICA",                            "credits": "4", "lyrics": "C", "number": "78", "semester": "ENE-ABR 2019", "observations": "", "codeRequired": "",      "pensumCredit": _pensum_credit_med},
            {"codeSubject": "ORI-100", "subject": "ORIENTACIÓN UNIVERSITARIA",                    "credits": "1", "lyrics": "A", "number": "92", "semester": "ENE-ABR 2019", "observations": "", "codeRequired": "",      "pensumCredit": _pensum_credit_med},
            {"codeSubject": "MED-079", "subject": "ORIENTACIÓN MÉDICA",                           "credits": "2", "lyrics": "A", "number": "94", "semester": "ENE-ABR 2019", "observations": "", "codeRequired": "",      "pensumCredit": _pensum_credit_med},
            {"codeSubject": "QUI-111", "subject": "QUÍMICA GENERAL I",                            "credits": "4", "lyrics": "B", "number": "85", "semester": "ENE-ABR 2019", "observations": "", "codeRequired": "",      "pensumCredit": _pensum_credit_med},
            # ── Período 2 ──
            {"codeSubject": "MAT-333", "subject": "BIOESTADÍSTICA Y DEMOGRAFÍA",                  "credits": "3", "lyrics": "B", "number": "86", "semester": "MAY-AGO 2019", "observations": "", "codeRequired": "MAT-060", "pensumCredit": _pensum_credit_med},
            {"codeSubject": "BIO-102", "subject": "BIOLOGÍA GENERAL II",                          "credits": "4", "lyrics": "B", "number": "84", "semester": "MAY-AGO 2019", "observations": "", "codeRequired": "BIO-101", "pensumCredit": _pensum_credit_med},
            {"codeSubject": "ELT-002", "subject": "ELECTIVA II",                                  "credits": "3", "lyrics": "B", "number": "85", "semester": "MAY-AGO 2019", "observations": "", "codeRequired": "",      "pensumCredit": _pensum_credit_med},
            {"codeSubject": "FIS-011", "subject": "FÍSICA BÁSICA I",                              "credits": "4", "lyrics": "C", "number": "77", "semester": "MAY-AGO 2019", "observations": "", "codeRequired": "",      "pensumCredit": _pensum_credit_med},
            {"codeSubject": "INF-200", "subject": "INFORMÁTICA BÁSICA Y CULTURAL",                "credits": "3", "lyrics": "B", "number": "83", "semester": "MAY-AGO 2019", "observations": "", "codeRequired": "",      "pensumCredit": _pensum_credit_med},
            {"codeSubject": "LEX-120", "subject": "INGLÉS INTRODUCTORIO DE CIENCIAS DE LA SALUD II", "credits": "3", "lyrics": "B", "number": "84", "semester": "MAY-AGO 2019", "observations": "", "codeRequired": "LEX-113", "pensumCredit": _pensum_credit_med},
            {"codeSubject": "LET-102", "subject": "LENGUA ESPAÑOLA Y TÉCNICA DE LA EXPRESIÓN II", "credits": "3", "lyrics": "A", "number": "89", "semester": "MAY-AGO 2019", "observations": "", "codeRequired": "LET-101", "pensumCredit": _pensum_credit_med},
            {"codeSubject": "QUI-112", "subject": "QUÍMICA GENERAL II",                           "credits": "4", "lyrics": "B", "number": "85", "semester": "MAY-AGO 2019", "observations": "", "codeRequired": "QUI-111", "pensumCredit": _pensum_credit_med},
            # ── Período 3 ──
            {"codeSubject": "MAT-338", "subject": "BIOESTADÍSTICA Y DEMOGRAFÍA II",               "credits": "3", "lyrics": "B", "number": "84", "semester": "SEP-DIC 2019", "observations": "", "codeRequired": "MAT-333", "pensumCredit": _pensum_credit_med},
            {"codeSubject": "BIO-215", "subject": "BIOLOGÍA MOLECULAR",                           "credits": "4", "lyrics": "B", "number": "86", "semester": "SEP-DIC 2019", "observations": "", "codeRequired": "BIO-102", "pensumCredit": _pensum_credit_med},
            {"codeSubject": "ELT-003", "subject": "ELECTIVA III",                                 "credits": "4", "lyrics": "B", "number": "83", "semester": "SEP-DIC 2019", "observations": "", "codeRequired": "BIO-101,BIO-102", "pensumCredit": _pensum_credit_med},
            {"codeSubject": "ELT-004", "subject": "ELECTIVA IV",                                  "credits": "3", "lyrics": "A", "number": "90", "semester": "SEP-DIC 2019", "observations": "", "codeRequired": "LET-102", "pensumCredit": _pensum_credit_med},
            {"codeSubject": "FIS-012", "subject": "FÍSICA BÁSICA II",                             "credits": "4", "lyrics": "C", "number": "76", "semester": "SEP-DIC 2019", "observations": "", "codeRequired": "",      "pensumCredit": _pensum_credit_med},
            {"codeSubject": "LEX-128", "subject": "INGLÉS TÉCNICO DE CIENCIAS DE LA SALUD",      "credits": "3", "lyrics": "B", "number": "83", "semester": "SEP-DIC 2019", "observations": "", "codeRequired": "LEX-113", "pensumCredit": _pensum_credit_med},
            {"codeSubject": "MED-021", "subject": "INTRODUCCIÓN A LA INVESTIGACIÓN CIENTÍFICA",   "credits": "3", "lyrics": "A", "number": "91", "semester": "SEP-DIC 2019", "observations": "", "codeRequired": "",      "pensumCredit": _pensum_credit_med},
            {"codeSubject": "PSI-100", "subject": "PSICOLOGÍA GENERAL",                           "credits": "3", "lyrics": "B", "number": "85", "semester": "SEP-DIC 2019", "observations": "", "codeRequired": "",      "pensumCredit": _pensum_credit_med},
            {"codeSubject": "QUI-231", "subject": "QUÍMICA ORGÁNICA I",                           "credits": "4", "lyrics": "B", "number": "82", "semester": "SEP-DIC 2019", "observations": "", "codeRequired": "QUI-112", "pensumCredit": _pensum_credit_med},
            # ── Período 4 ──
            {"codeSubject": "BIO-265", "subject": "BIOFÍSICA",                                    "credits": "4", "lyrics": "B", "number": "84", "semester": "ENE-ABR 2020", "observations": "", "codeRequired": "FIS-012", "pensumCredit": _pensum_credit_med},
            {"codeSubject": "ELT-005", "subject": "ELECTIVA V",                                   "credits": "5", "lyrics": "B", "number": "83", "semester": "ENE-ABR 2020", "observations": "", "codeRequired": "MAT-333", "pensumCredit": _pensum_credit_med},
            {"codeSubject": "HUM-160", "subject": "HISTORIA DOMINICANA",                          "credits": "3", "lyrics": "B", "number": "85", "semester": "ENE-ABR 2020", "observations": "", "codeRequired": "",      "pensumCredit": _pensum_credit_med},
            {"codeSubject": "MED-103", "subject": "INFORMÁTICA PARA CIENCIAS DE LA SALUD",        "credits": "3", "lyrics": "A", "number": "88", "semester": "ENE-ABR 2020", "observations": "", "codeRequired": "INF-200", "pensumCredit": _pensum_credit_med},
            {"codeSubject": "LEX-140", "subject": "INGLÉS TÉCNICO CIENCIAS DE LA SALUD II",      "credits": "3", "lyrics": "B", "number": "82", "semester": "ENE-ABR 2020", "observations": "", "codeRequired": "LEX-128", "pensumCredit": _pensum_credit_med},
            {"codeSubject": "PSI-105", "subject": "PSICOLOGÍA APLICADA",                          "credits": "3", "lyrics": "B", "number": "85", "semester": "ENE-ABR 2020", "observations": "", "codeRequired": "PSI-100", "pensumCredit": _pensum_credit_med},
            {"codeSubject": "QUI-232", "subject": "QUÍMICA ORGÁNICA II",                          "credits": "4", "lyrics": "B", "number": "83", "semester": "ENE-ABR 2020", "observations": "", "codeRequired": "QUI-231", "pensumCredit": _pensum_credit_med},
            {"codeSubject": "LET-211", "subject": "RAÍCES GRIEGAS Y LATINAS",                    "credits": "3", "lyrics": "A", "number": "90", "semester": "ENE-ABR 2020", "observations": "", "codeRequired": "LET-102", "pensumCredit": _pensum_credit_med},
            # ── Período 5 ──
            {"codeSubject": "MED-115", "subject": "ANATOMÍA INTEGRADA I",                        "credits": "8", "lyrics": "B", "number": "86", "semester": "MAY-AGO 2020", "observations": "", "codeRequired": "",      "pensumCredit": _pensum_credit_med},
            {"codeSubject": "MED-145", "subject": "BIOÉTICA MÉDICA",                              "credits": "4", "lyrics": "A", "number": "90", "semester": "MAY-AGO 2020", "observations": "", "codeRequired": "",      "pensumCredit": _pensum_credit_med},
            {"codeSubject": "MED-125", "subject": "BIOQUÍMICA Y GENÉTICA",                        "credits": "9", "lyrics": "B", "number": "85", "semester": "MAY-AGO 2020", "observations": "", "codeRequired": "",      "pensumCredit": _pensum_credit_med},
            {"codeSubject": "MED-135", "subject": "INTRODUCCIÓN A LAS CIENCIAS BÁSICAS I",       "credits": "4", "lyrics": "A", "number": "88", "semester": "MAY-AGO 2020", "observations": "", "codeRequired": "",      "pensumCredit": _pensum_credit_med},
            # ── Período 6 ──
            {"codeSubject": "MED-116", "subject": "ANATOMÍA INTEGRADA II",                       "credits": "8", "lyrics": "B", "number": "84", "semester": "SEP-DIC 2020", "observations": "", "codeRequired": "MED-115", "pensumCredit": _pensum_credit_med},
            {"codeSubject": "MED-146", "subject": "CIENCIAS DEL COMPORTAMIENTO",                  "credits": "4", "lyrics": "A", "number": "89", "semester": "SEP-DIC 2020", "observations": "", "codeRequired": "MED-145", "pensumCredit": _pensum_credit_med},
            {"codeSubject": "MED-136", "subject": "INTRODUCCIÓN A LAS CIENCIAS BÁSICAS II",      "credits": "4", "lyrics": "A", "number": "88", "semester": "SEP-DIC 2020", "observations": "", "codeRequired": "MED-135", "pensumCredit": _pensum_credit_med},
            {"codeSubject": "MED-126", "subject": "MICROBIOLOGÍA Y PARASITOLOGÍA",                "credits": "8", "lyrics": "B", "number": "86", "semester": "SEP-DIC 2020", "observations": "", "codeRequired": "MED-125", "pensumCredit": _pensum_credit_med},
        ],

        pending_subjects=[
            # P7
            {"codeSubject": "MED-237", "subject": "CIENCIAS BÁSICAS POR SISTEMAS I",              "credits": 16, "lyrics": "", "number": "", "semester": "", "observations": "", "codeRequired": "MED-116,MED-126,MED-136,MED-146", "pensumCredit": _pensum_credit_med},
            {"codeSubject": "MED-247", "subject": "EPIDEMIOLOGÍA",                                "credits": 3,  "lyrics": "", "number": "", "semester": "", "observations": "", "codeRequired": "MED-116,MED-126,MED-136,MED-146", "pensumCredit": _pensum_credit_med},
            {"codeSubject": "MED-257", "subject": "SEMIOLOGÍA I",                                 "credits": 4,  "lyrics": "", "number": "", "semester": "", "observations": "", "codeRequired": "MED-116,MED-126,MED-136,MED-146", "pensumCredit": _pensum_credit_med},
            # P8
            {"codeSubject": "MED-238", "subject": "CIENCIAS BÁSICAS POR SISTEMAS II",             "credits": 16, "lyrics": "", "number": "", "semester": "", "observations": "", "codeRequired": "MED-237",               "pensumCredit": _pensum_credit_med},
            {"codeSubject": "MED-248", "subject": "MEDICINA PREVENTIVA",                          "credits": 3,  "lyrics": "", "number": "", "semester": "", "observations": "", "codeRequired": "MED-247",               "pensumCredit": _pensum_credit_med},
            {"codeSubject": "MED-258", "subject": "SEMIOLOGÍA II",                                "credits": 4,  "lyrics": "", "number": "", "semester": "", "observations": "", "codeRequired": "MED-257",               "pensumCredit": _pensum_credit_med},
            # P9
            {"codeSubject": "MED-249", "subject": "ADMINISTRACIÓN Y LEGISLACIÓN SANITARIA",       "credits": 3,  "lyrics": "", "number": "", "semester": "", "observations": "", "codeRequired": "MED-248",               "pensumCredit": _pensum_credit_med},
            {"codeSubject": "MED-239", "subject": "CIENCIAS BÁSICAS POR SISTEMAS III",            "credits": 11, "lyrics": "", "number": "", "semester": "", "observations": "", "codeRequired": "MED-238",               "pensumCredit": _pensum_credit_med},
            {"codeSubject": "MED-269", "subject": "REVISIÓN INTEGRADA Y PREPARACIÓN PARA REVÁLIDA","credits": 5,  "lyrics": "", "number": "", "semester": "", "observations": "", "codeRequired": "MED-238,MED-248,MED-258", "pensumCredit": _pensum_credit_med},
            {"codeSubject": "MED-259", "subject": "SOPORTE BÁSICO DE VIDA",                       "credits": 4,  "lyrics": "", "number": "", "semester": "", "observations": "", "codeRequired": "MED-258",               "pensumCredit": _pensum_credit_med},
            # P10
            {"codeSubject": "MED-314", "subject": "PATOLOGÍA MÉDICA I",                           "credits": 28, "lyrics": "", "number": "", "semester": "", "observations": "", "codeRequired": "MED-115,MED-116,MED-125,MED-126,MED-135,MED-136,MED-145,MED-146,MED-237,MED-238,MED-239,MED-247,MED-248,MED-249,MED-257,MED-258,MED-259,MED-269", "pensumCredit": _pensum_credit_med},
            # P11
            {"codeSubject": "MED-315", "subject": "PATOLOGÍA MÉDICA II",                          "credits": 28, "lyrics": "", "number": "", "semester": "", "observations": "", "codeRequired": "MED-314",               "pensumCredit": _pensum_credit_med},
            # P12
            {"codeSubject": "MED-324", "subject": "PATOLOGÍA QUIRÚRGICA",                         "credits": 28, "lyrics": "", "number": "", "semester": "", "observations": "", "codeRequired": "MED-315",               "pensumCredit": _pensum_credit_med},
            # P13
            {"codeSubject": "MED-304", "subject": "GINECOLOGÍA Y OBSTETRICIA",                    "credits": 12, "lyrics": "", "number": "", "semester": "", "observations": "", "codeRequired": "MED-324",               "pensumCredit": _pensum_credit_med},
            {"codeSubject": "MED-327", "subject": "NEONATOLOGÍA",                                 "credits": 1,  "lyrics": "", "number": "", "semester": "", "observations": "", "codeRequired": "MED-324",               "pensumCredit": _pensum_credit_med},
            {"codeSubject": "MED-307", "subject": "PEDIATRÍA",                                    "credits": 16, "lyrics": "", "number": "", "semester": "", "observations": "", "codeRequired": "MED-324",               "pensumCredit": _pensum_credit_med},
            {"codeSubject": "MED-353", "subject": "PSIQUIATRÍA",                                  "credits": 4,  "lyrics": "", "number": "", "semester": "", "observations": "", "codeRequired": "MED-324",               "pensumCredit": _pensum_credit_med},
            # P14
            {"codeSubject": "MED-405", "subject": "CLÍNICA MÉDICA",                               "credits": 14, "lyrics": "", "number": "", "semester": "", "observations": "", "codeRequired": "",                      "pensumCredit": _pensum_credit_med},
            {"codeSubject": "MED-407", "subject": "CLÍNICA PEDIÁTRICA",                           "credits": 10, "lyrics": "", "number": "", "semester": "", "observations": "", "codeRequired": "",                      "pensumCredit": _pensum_credit_med},
            # P15
            {"codeSubject": "MED-403", "subject": "CLÍNICA PSIQUIÁTRICA",                         "credits": 6,  "lyrics": "", "number": "", "semester": "", "observations": "", "codeRequired": "",                      "pensumCredit": _pensum_credit_med},
            {"codeSubject": "MED-406", "subject": "CLÍNICA QUIRÚRGICA",                           "credits": 14, "lyrics": "", "number": "", "semester": "", "observations": "", "codeRequired": "",                      "pensumCredit": _pensum_credit_med},
            {"codeSubject": "ELT-006", "subject": "ELECTIVA VI",                                  "credits": 3,  "lyrics": "", "number": "", "semester": "", "observations": "", "codeRequired": "",                      "pensumCredit": _pensum_credit_med},
            # P16
            {"codeSubject": "MED-404", "subject": "CLÍNICA GINECO-OBSTÉTRICA",                   "credits": 10, "lyrics": "", "number": "", "semester": "", "observations": "", "codeRequired": "",                      "pensumCredit": _pensum_credit_med},
            {"codeSubject": "MED-408", "subject": "MEDICINA SOCIAL O FAMILIAR",                   "credits": 6,  "lyrics": "", "number": "", "semester": "", "observations": "", "codeRequired": "",                      "pensumCredit": _pensum_credit_med},
            {"codeSubject": "MED-900", "subject": "TRABAJO DE GRADO (MEDICINA)",                  "credits": 6,  "lyrics": "", "number": "", "semester": "", "observations": "", "codeRequired": "",                      "pensumCredit": _pensum_credit_med},
        ],

        officially_enrolled=[
            {"codeSubject": "MED-237", "subjectName": "CIENCIAS BÁSICAS POR SISTEMAS I",          "credits": "16", "section": "01", "teacher": "Dra. Elizabeth Blackwell"},
            {"codeSubject": "MED-247", "subjectName": "EPIDEMIOLOGÍA",                            "credits": "3",  "section": "01", "teacher": "Dr. John Snow"},
            {"codeSubject": "MED-257", "subjectName": "SEMIOLOGÍA I",                             "credits": "4",  "section": "02", "teacher": "Dr. René Laënnec"},
        ],
        unofficial_selected=[
            {"codeSubject": "MED-237", "subjectName": "CIENCIAS BÁSICAS POR SISTEMAS I",          "credits": "16", "section": "01", "teacher": "Dra. Elizabeth Blackwell"},
            {"codeSubject": "MED-247", "subjectName": "EPIDEMIOLOGÍA",                            "credits": "3",  "section": "01", "teacher": "Dr. John Snow"},
            {"codeSubject": "MED-257", "subjectName": "SEMIOLOGÍA I",                             "credits": "4",  "section": "02", "teacher": "Dr. René Laënnec"},
        ],
        gpa_target=3.16,
        facultad="Facultad de Ciencias de la Salud",
    ),


    # ──────────────────────────────────────────────────────────────────────────
    # 4. MIGUEL RODRÍGUEZ — Derecho — períodos 1-7 aprobados — GPA 2.2 ⚠️
    #    Situación CRÍTICA: índice por debajo de 2.8 → debe disparar alertas
    #    de riesgo académico y recomendaciones para subir el GPA a 3.0.
    # ──────────────────────────────────────────────────────────────────────────
    "mr21-1402": generate_mock_student(
        matricula="mr21-1402",
        names="MIGUEL ANGEL RODRIGUEZ PEÑA",
        career_name=f"DERECHO {_pensum_credit_der}/2-10-10",
        id_persona=211402,
        id_carrera=401210,
        total_pensum_credits=249,
        start_year=2021,
        end_year=2025,

        # Períodos 1-7 completados (144 CR) — mayoría C, algunas B
        completed_subjects=[
            # ── Período 1 ──
            {"codeSubject": "ELT-001", "subject": "ELECTIVA I (ARTES Y DEPORTES)",                "credits": "1", "lyrics": "B", "number": "83", "semester": "ENE-ABR 2021", "observations": "", "codeRequired": "",          "pensumCredit": _pensum_credit_der},
            {"codeSubject": "LET-104", "subject": "EXPRESIÓN ORAL Y PRODUCCIÓN ESCRITA",          "credits": "3", "lyrics": "C", "number": "76", "semester": "ENE-ABR 2021", "observations": "", "codeRequired": "",          "pensumCredit": _pensum_credit_der},
            {"codeSubject": "LEX-160", "subject": "FRANCÉS I",                                    "credits": "3", "lyrics": "C", "number": "75", "semester": "ENE-ABR 2021", "observations": "", "codeRequired": "",          "pensumCredit": _pensum_credit_der},
            {"codeSubject": "LEX-110", "subject": "INGLÉS BÁSICO",                                "credits": "3", "lyrics": "C", "number": "77", "semester": "ENE-ABR 2021", "observations": "", "codeRequired": "",          "pensumCredit": _pensum_credit_der},
            {"codeSubject": "DER-112", "subject": "INTRODUCCIÓN AL ESTUDIO DEL DERECHO I",       "credits": "3", "lyrics": "B", "number": "84", "semester": "ENE-ABR 2021", "observations": "", "codeRequired": "",          "pensumCredit": _pensum_credit_der},
            {"codeSubject": "MAT-060", "subject": "MATEMÁTICA BÁSICA",                            "credits": "4", "lyrics": "C", "number": "75", "semester": "ENE-ABR 2021", "observations": "", "codeRequired": "",          "pensumCredit": _pensum_credit_der},
            {"codeSubject": "ORI-100", "subject": "ORIENTACIÓN UNIVERSITARIA",                    "credits": "1", "lyrics": "B", "number": "83", "semester": "ENE-ABR 2021", "observations": "", "codeRequired": "",          "pensumCredit": _pensum_credit_der},
            {"codeSubject": "DER-108", "subject": "TEORÍA DEL DERECHO",                           "credits": "3", "lyrics": "C", "number": "78", "semester": "ENE-ABR 2021", "observations": "", "codeRequired": "",          "pensumCredit": _pensum_credit_der},
            # ── Período 2 ──
            {"codeSubject": "LEX-161", "subject": "FRANCÉS II",                                   "credits": "3", "lyrics": "C", "number": "76", "semester": "MAY-AGO 2021", "observations": "", "codeRequired": "LEX-160",   "pensumCredit": _pensum_credit_der},
            {"codeSubject": "HUM-160", "subject": "HISTORIA DOMINICANA",                          "credits": "3", "lyrics": "B", "number": "83", "semester": "MAY-AGO 2021", "observations": "", "codeRequired": "",          "pensumCredit": _pensum_credit_der},
            {"codeSubject": "INF-200", "subject": "INFORMÁTICA BÁSICA Y CULTURAL",                "credits": "3", "lyrics": "C", "number": "77", "semester": "MAY-AGO 2021", "observations": "", "codeRequired": "",          "pensumCredit": _pensum_credit_der},
            {"codeSubject": "SOC-100", "subject": "INTRODUCCIÓN A LA SOCIOLOGÍA",                 "credits": "3", "lyrics": "C", "number": "76", "semester": "MAY-AGO 2021", "observations": "", "codeRequired": "",          "pensumCredit": _pensum_credit_der},
            {"codeSubject": "DER-113", "subject": "INTRODUCCIÓN AL ESTUDIO DEL DERECHO II",      "credits": "3", "lyrics": "C", "number": "78", "semester": "MAY-AGO 2021", "observations": "", "codeRequired": "DER-112",   "pensumCredit": _pensum_credit_der},
            {"codeSubject": "PSI-100", "subject": "PSICOLOGÍA GENERAL",                           "credits": "3", "lyrics": "B", "number": "82", "semester": "MAY-AGO 2021", "observations": "", "codeRequired": "",          "pensumCredit": _pensum_credit_der},
            {"codeSubject": "DER-114", "subject": "REDACCIÓN",                                    "credits": "3", "lyrics": "C", "number": "77", "semester": "MAY-AGO 2021", "observations": "", "codeRequired": "LET-104",   "pensumCredit": _pensum_credit_der},
            # ── Período 3 ──
            {"codeSubject": "DER-116", "subject": "DERECHO CIVIL I",                             "credits": "3", "lyrics": "C", "number": "78", "semester": "SEP-DIC 2021", "observations": "", "codeRequired": "DER-113",   "pensumCredit": _pensum_credit_der},
            {"codeSubject": "DER-240", "subject": "DERECHO CONSTITUCIONAL I",                    "credits": "3", "lyrics": "B", "number": "84", "semester": "SEP-DIC 2021", "observations": "", "codeRequired": "DER-113",   "pensumCredit": _pensum_credit_der},
            {"codeSubject": "DER-130", "subject": "DERECHO ROMANO",                              "credits": "3", "lyrics": "C", "number": "76", "semester": "SEP-DIC 2021", "observations": "", "codeRequired": "DER-113",   "pensumCredit": _pensum_credit_der},
            {"codeSubject": "CON-116", "subject": "FUNDAMENTOS DE CONTABILIDAD",                  "credits": "4", "lyrics": "C", "number": "75", "semester": "SEP-DIC 2021", "observations": "", "codeRequired": "",          "pensumCredit": _pensum_credit_der},
            {"codeSubject": "HUM-150", "subject": "HISTORIA DE LA CULTURA UNIVERSAL",             "credits": "3", "lyrics": "B", "number": "83", "semester": "SEP-DIC 2021", "observations": "", "codeRequired": "",          "pensumCredit": _pensum_credit_der},
            {"codeSubject": "DER-122", "subject": "INFORMÁTICA JURÍDICA",                         "credits": "2", "lyrics": "C", "number": "77", "semester": "SEP-DIC 2021", "observations": "", "codeRequired": "INF-200",   "pensumCredit": _pensum_credit_der},
            {"codeSubject": "LEX-141", "subject": "INGLÉS JURÍDICO I",                            "credits": "3", "lyrics": "C", "number": "78", "semester": "SEP-DIC 2021", "observations": "", "codeRequired": "LEX-110",   "pensumCredit": _pensum_credit_der},
            {"codeSubject": "DER-121", "subject": "LÓGICA JURÍDICA",                              "credits": "2", "lyrics": "C", "number": "76", "semester": "SEP-DIC 2021", "observations": "", "codeRequired": "DER-108",   "pensumCredit": _pensum_credit_der},
            # ── Período 4 ──
            {"codeSubject": "DER-125", "subject": "ARGUMENTACIÓN JURÍDICA I",                    "credits": "3", "lyrics": "C", "number": "79", "semester": "ENE-ABR 2022", "observations": "", "codeRequired": "DER-114",   "pensumCredit": _pensum_credit_der},
            {"codeSubject": "DER-117", "subject": "DERECHO CIVIL II",                            "credits": "4", "lyrics": "C", "number": "77", "semester": "ENE-ABR 2022", "observations": "", "codeRequired": "DER-116",   "pensumCredit": _pensum_credit_der},
            {"codeSubject": "DER-241", "subject": "DERECHO CONSTITUCIONAL II",                   "credits": "3", "lyrics": "B", "number": "83", "semester": "ENE-ABR 2022", "observations": "", "codeRequired": "DER-240",   "pensumCredit": _pensum_credit_der},
            {"codeSubject": "DER-123", "subject": "DERECHO PENAL I",                             "credits": "3", "lyrics": "C", "number": "78", "semester": "ENE-ABR 2022", "observations": "", "codeRequired": "DER-113",   "pensumCredit": _pensum_credit_der},
            {"codeSubject": "DER-242", "subject": "HISTORIA DE LAS IDEAS POLÍTICAS",             "credits": "3", "lyrics": "C", "number": "76", "semester": "ENE-ABR 2022", "observations": "", "codeRequired": "DER-240,HUM-150", "pensumCredit": _pensum_credit_der},
            {"codeSubject": "LEX-142", "subject": "INGLÉS JURÍDICO II",                           "credits": "3", "lyrics": "C", "number": "77", "semester": "ENE-ABR 2022", "observations": "", "codeRequired": "LEX-141",   "pensumCredit": _pensum_credit_der},
            {"codeSubject": "SOC-114", "subject": "SOCIOLOGÍA JURÍDICA",                          "credits": "2", "lyrics": "C", "number": "78", "semester": "ENE-ABR 2022", "observations": "", "codeRequired": "SOC-100",   "pensumCredit": _pensum_credit_der},
            # ── Período 5 ──
            {"codeSubject": "DER-126", "subject": "ARGUMENTACIÓN JURÍDICA II",                   "credits": "3", "lyrics": "C", "number": "76", "semester": "MAY-AGO 2022", "observations": "", "codeRequired": "DER-125",   "pensumCredit": _pensum_credit_der},
            {"codeSubject": "DER-304", "subject": "DERECHO CIVIL III",                           "credits": "5", "lyrics": "C", "number": "77", "semester": "MAY-AGO 2022", "observations": "", "codeRequired": "DER-117",   "pensumCredit": _pensum_credit_der},
            {"codeSubject": "DER-127", "subject": "DERECHO CONTEMPORÁNEO (DERECHO COMPARADO)",   "credits": "3", "lyrics": "B", "number": "82", "semester": "MAY-AGO 2022", "observations": "", "codeRequired": "DER-240",   "pensumCredit": _pensum_credit_der},
            {"codeSubject": "DER-231", "subject": "DERECHO PENAL II",                            "credits": "3", "lyrics": "C", "number": "76", "semester": "MAY-AGO 2022", "observations": "", "codeRequired": "DER-123",   "pensumCredit": _pensum_credit_der},
            {"codeSubject": "DER-250", "subject": "HISTORIA DEL DERECHO",                        "credits": "2", "lyrics": "C", "number": "75", "semester": "MAY-AGO 2022", "observations": "", "codeRequired": "DER-242,HUM-160", "pensumCredit": _pensum_credit_der},
            {"codeSubject": "ECO-100", "subject": "INTRODUCCIÓN A LA ECONOMÍA",                   "credits": "3", "lyrics": "C", "number": "78", "semester": "MAY-AGO 2022", "observations": "", "codeRequired": "MAT-060",   "pensumCredit": _pensum_credit_der},
            {"codeSubject": "PSI-253", "subject": "PSICOLOGÍA APLICADA A CIENCIAS JURÍDICAS",    "credits": "2", "lyrics": "C", "number": "77", "semester": "MAY-AGO 2022", "observations": "", "codeRequired": "PSI-100",   "pensumCredit": _pensum_credit_der},
            # ── Período 6 ──
            {"codeSubject": "DER-305", "subject": "DERECHO CIVIL IV",                            "credits": "5", "lyrics": "C", "number": "76", "semester": "SEP-DIC 2022", "observations": "", "codeRequired": "DER-304",   "pensumCredit": _pensum_credit_der},
            {"codeSubject": "DER-352", "subject": "DERECHO INTERNACIONAL PÚBLICO",               "credits": "3", "lyrics": "B", "number": "82", "semester": "SEP-DIC 2022", "observations": "", "codeRequired": "DER-241",   "pensumCredit": _pensum_credit_der},
            {"codeSubject": "DER-332", "subject": "DERECHO PENAL III",                           "credits": "3", "lyrics": "C", "number": "77", "semester": "SEP-DIC 2022", "observations": "", "codeRequired": "DER-231",   "pensumCredit": _pensum_credit_der},
            {"codeSubject": "DER-310", "subject": "DERECHO PROCESAL CIVIL I",                    "credits": "3", "lyrics": "C", "number": "78", "semester": "SEP-DIC 2022", "observations": "", "codeRequired": "DER-117",   "pensumCredit": _pensum_credit_der},
            {"codeSubject": "FIL-104", "subject": "ÉTICA FORMATIVA",                              "credits": "2", "lyrics": "B", "number": "83", "semester": "SEP-DIC 2022", "observations": "", "codeRequired": "",          "pensumCredit": _pensum_credit_der},
            {"codeSubject": "DER-119", "subject": "METODOLOGÍA DE LA INVESTIGACIÓN CIENTÍFICA",  "credits": "3", "lyrics": "C", "number": "76", "semester": "SEP-DIC 2022", "observations": "", "codeRequired": "DER-121,DER-126", "pensumCredit": _pensum_credit_der},
            {"codeSubject": "ADM-105", "subject": "PRINCIPIOS DE ADMINISTRACIÓN",                 "credits": "3", "lyrics": "C", "number": "77", "semester": "SEP-DIC 2022", "observations": "", "codeRequired": "",          "pensumCredit": _pensum_credit_der},
            # ── Período 7 ──
            {"codeSubject": "DER-335", "subject": "DERECHO CIVIL V",                             "credits": "4", "lyrics": "C", "number": "76", "semester": "ENE-ABR 2023", "observations": "", "codeRequired": "DER-305",   "pensumCredit": _pensum_credit_der},
            {"codeSubject": "DER-384", "subject": "DERECHO COMERCIAL",                           "credits": "3", "lyrics": "C", "number": "77", "semester": "ENE-ABR 2023", "observations": "", "codeRequired": "DER-305",   "pensumCredit": _pensum_credit_der},
            {"codeSubject": "DER-205", "subject": "DERECHO LABORAL I",                           "credits": "3", "lyrics": "B", "number": "82", "semester": "ENE-ABR 2023", "observations": "", "codeRequired": "DER-305",   "pensumCredit": _pensum_credit_der},
            {"codeSubject": "DER-333", "subject": "DERECHO PENAL IV",                            "credits": "3", "lyrics": "C", "number": "76", "semester": "ENE-ABR 2023", "observations": "", "codeRequired": "DER-332",   "pensumCredit": _pensum_credit_der},
            {"codeSubject": "DER-311", "subject": "DERECHO PROCESAL CIVIL II",                   "credits": "4", "lyrics": "C", "number": "78", "semester": "ENE-ABR 2023", "observations": "", "codeRequired": "DER-310",   "pensumCredit": _pensum_credit_der},
            {"codeSubject": "DER-266", "subject": "ÉTICA JURÍDICA",                              "credits": "2", "lyrics": "C", "number": "76", "semester": "ENE-ABR 2023", "observations": "", "codeRequired": "DER-121,DER-304,FIL-104", "pensumCredit": _pensum_credit_der},
            {"codeSubject": "DER-120", "subject": "METODOLOGÍA DE LA INVEST. EN LAS CS. JURÍDICAS", "credits": "2", "lyrics": "C", "number": "77", "semester": "ENE-ABR 2023", "observations": "", "codeRequired": "DER-119", "pensumCredit": _pensum_credit_der},
        ],

        pending_subjects=[
            # P8
            {"codeSubject": "DER-411", "subject": "DERECHO AMBIENTAL",                           "credits": 3, "lyrics": "", "number": "", "semester": "", "observations": "", "codeRequired": "DER-241",                   "pensumCredit": _pensum_credit_der},
            {"codeSubject": "DER-336", "subject": "DERECHO CIVIL VI",                            "credits": 4, "lyrics": "", "number": "", "semester": "", "observations": "", "codeRequired": "DER-335",                   "pensumCredit": _pensum_credit_der},
            {"codeSubject": "DER-219", "subject": "DERECHO LABORAL II",                          "credits": 3, "lyrics": "", "number": "", "semester": "", "observations": "", "codeRequired": "DER-205",                   "pensumCredit": _pensum_credit_der},
            {"codeSubject": "DER-412", "subject": "DERECHO PROCESAL CIVIL III",                  "credits": 3, "lyrics": "", "number": "", "semester": "", "observations": "", "codeRequired": "DER-311",                   "pensumCredit": _pensum_credit_der},
            {"codeSubject": "DER-207", "subject": "FILOSOFÍA DEL DERECHO",                       "credits": 3, "lyrics": "", "number": "", "semester": "", "observations": "", "codeRequired": "DER-242,DER-266",           "pensumCredit": _pensum_credit_der},
            {"codeSubject": "DER-370", "subject": "PROPIEDAD INTELECTUAL",                        "credits": 2, "lyrics": "", "number": "", "semester": "", "observations": "", "codeRequired": "DER-266",                   "pensumCredit": _pensum_credit_der},
            {"codeSubject": "DER-306", "subject": "RESPONSABILIDAD CIVIL",                        "credits": 3, "lyrics": "", "number": "", "semester": "", "observations": "", "codeRequired": "DER-305",                   "pensumCredit": _pensum_credit_der},
            # P9
            {"codeSubject": "DER-208", "subject": "CONTRATOS COMERCIALES",                        "credits": 3, "lyrics": "", "number": "", "semester": "", "observations": "", "codeRequired": "DER-305,DER-306",           "pensumCredit": _pensum_credit_der},
            {"codeSubject": "DER-209", "subject": "DERECHO ADMINISTRATIVO I",                    "credits": 3, "lyrics": "", "number": "", "semester": "", "observations": "", "codeRequired": "DER-241",                   "pensumCredit": _pensum_credit_der},
            {"codeSubject": "DER-508", "subject": "DERECHO CIVIL VII",                           "credits": 5, "lyrics": "", "number": "", "semester": "", "observations": "", "codeRequired": "DER-336",                   "pensumCredit": _pensum_credit_der},
            {"codeSubject": "DER-413", "subject": "DERECHO PROCESAL CIVIL IV",                   "credits": 3, "lyrics": "", "number": "", "semester": "", "observations": "", "codeRequired": "DER-412",                   "pensumCredit": _pensum_credit_der},
            {"codeSubject": "DER-507", "subject": "DERECHO PROCESAL LABORAL",                    "credits": 3, "lyrics": "", "number": "", "semester": "", "observations": "", "codeRequired": "DER-219",                   "pensumCredit": _pensum_credit_der},
            {"codeSubject": "DER-434", "subject": "DERECHO PROCESAL PENAL I",                    "credits": 3, "lyrics": "", "number": "", "semester": "", "observations": "", "codeRequired": "DER-333",                   "pensumCredit": _pensum_credit_der},
            {"codeSubject": "DER-525", "subject": "DERECHO TRIBUTARIO",                          "credits": 2, "lyrics": "", "number": "", "semester": "", "observations": "", "codeRequired": "DER-241",                   "pensumCredit": _pensum_credit_der},
            # P10
            {"codeSubject": "DER-543", "subject": "DERECHO ADMINISTRATIVO II",                   "credits": 3, "lyrics": "", "number": "", "semester": "", "observations": "", "codeRequired": "DER-209",                   "pensumCredit": _pensum_credit_der},
            {"codeSubject": "DER-509", "subject": "DERECHO CIVIL VIII",                          "credits": 5, "lyrics": "", "number": "", "semester": "", "observations": "", "codeRequired": "DER-508",                   "pensumCredit": _pensum_credit_der},
            {"codeSubject": "DER-424", "subject": "DERECHO ELECTORAL",                           "credits": 2, "lyrics": "", "number": "", "semester": "", "observations": "", "codeRequired": "DER-241,DER-508",           "pensumCredit": _pensum_credit_der},
            {"codeSubject": "DER-510", "subject": "DERECHO INMOBILIARIO I",                      "credits": 3, "lyrics": "", "number": "", "semester": "", "observations": "", "codeRequired": "DER-335",                   "pensumCredit": _pensum_credit_der},
            {"codeSubject": "DER-435", "subject": "DERECHO PROCESAL PENAL II",                   "credits": 3, "lyrics": "", "number": "", "semester": "", "observations": "", "codeRequired": "DER-434",                   "pensumCredit": _pensum_credit_der},
            {"codeSubject": "DER-390", "subject": "DERECHO SOCIETARIO",                          "credits": 3, "lyrics": "", "number": "", "semester": "", "observations": "", "codeRequired": "DER-208",                   "pensumCredit": _pensum_credit_der},
            {"codeSubject": "DER-265", "subject": "DERECHOS HUMANOS",                            "credits": 2, "lyrics": "", "number": "", "semester": "", "observations": "", "codeRequired": "DER-241",                   "pensumCredit": _pensum_credit_der},
            # P11
            {"codeSubject": "DER-511", "subject": "DERECHO INMOBILIARIO II",                     "credits": 3, "lyrics": "", "number": "", "semester": "", "observations": "", "codeRequired": "DER-510",                   "pensumCredit": _pensum_credit_der},
            {"codeSubject": "DER-554", "subject": "DERECHO INTERNACIONAL PRIVADO",               "credits": 3, "lyrics": "", "number": "", "semester": "", "observations": "", "codeRequired": "DER-390,DER-413,DER-509",   "pensumCredit": _pensum_credit_der},
            {"codeSubject": "DER-579", "subject": "DERECHO NOTARIAL",                            "credits": 2, "lyrics": "", "number": "", "semester": "", "observations": "", "codeRequired": "DER-509",                   "pensumCredit": _pensum_credit_der},
            {"codeSubject": "DER-211", "subject": "DERECHOS DEL CONSUMIDOR Y DEFENSA DE LA COMPETENCIA", "credits": 3, "lyrics": "", "number": "", "semester": "", "observations": "", "codeRequired": "DER-209,DER-241", "pensumCredit": _pensum_credit_der},
            {"codeSubject": "ELT-002", "subject": "ELECTIVA II",                                  "credits": 3, "lyrics": "", "number": "", "semester": "", "observations": "", "codeRequired": "DER-241,DER-305,DER-306,DER-333,DER-390,DER-413,DER-543", "pensumCredit": _pensum_credit_der},
            {"codeSubject": "DER-514", "subject": "PRÁCTICA DE LA PROFESIÓN JURÍDICA I",         "credits": 2, "lyrics": "", "number": "", "semester": "", "observations": "", "codeRequired": "DER-305,DER-413,DER-435",   "pensumCredit": _pensum_credit_der},
            {"codeSubject": "DER-580", "subject": "REDACCIÓN DE ACTOS JURÍDICOS",                "credits": 3, "lyrics": "", "number": "", "semester": "", "observations": "", "codeRequired": "DER-126",                   "pensumCredit": _pensum_credit_der},
            {"codeSubject": "DER-597", "subject": "RESPONSABILIDAD CIVIL ESPECIAL",               "credits": 2, "lyrics": "", "number": "", "semester": "", "observations": "", "codeRequired": "DER-306",                   "pensumCredit": _pensum_credit_der},
            # P12
            {"codeSubject": "DER-432", "subject": "DERECHO DE LA CONSTRUCCIÓN",                  "credits": 2, "lyrics": "", "number": "", "semester": "", "observations": "", "codeRequired": "DER-511,DER-543",           "pensumCredit": _pensum_credit_der},
            {"codeSubject": "DER-431", "subject": "DERECHO DEPORTIVO",                           "credits": 2, "lyrics": "", "number": "", "semester": "", "observations": "", "codeRequired": "DER-543,DER-597",           "pensumCredit": _pensum_credit_der},
            {"codeSubject": "DER-422", "subject": "DERECHO PROCESAL ADMINISTRATIVO Y TRIBUTARIO", "credits": 3, "lyrics": "", "number": "", "semester": "", "observations": "", "codeRequired": "DER-525,DER-543",           "pensumCredit": _pensum_credit_der},
            {"codeSubject": "DER-410", "subject": "DERECHO PROCESAL CONSTITUCIONAL",              "credits": 3, "lyrics": "", "number": "", "semester": "", "observations": "", "codeRequired": "DER-241",                   "pensumCredit": _pensum_credit_der},
            {"codeSubject": "DER-213", "subject": "EMPRENDIMIENTO Y GESTIÓN DE DESPACHO",         "credits": 3, "lyrics": "", "number": "", "semester": "", "observations": "", "codeRequired": "ADM-105",                   "pensumCredit": _pensum_credit_der},
            {"codeSubject": "DER-433", "subject": "INTELIGENCIA ARTIFICIAL Y DERECHO",            "credits": 3, "lyrics": "", "number": "", "semester": "", "observations": "", "codeRequired": "DER-122,DER-543",           "pensumCredit": _pensum_credit_der},
            {"codeSubject": "DER-515", "subject": "PRÁCTICA DE LA PROFESIÓN JURÍDICA II",        "credits": 2, "lyrics": "", "number": "", "semester": "", "observations": "", "codeRequired": "DER-514",                   "pensumCredit": _pensum_credit_der},
            {"codeSubject": "DER-223", "subject": "REGULACIÓN ECONÓMICA",                         "credits": 3, "lyrics": "", "number": "", "semester": "", "observations": "", "codeRequired": "DER-384,DER-390",           "pensumCredit": _pensum_credit_der},
            # P13
            {"codeSubject": "DER-900", "subject": "TRABAJO DE GRADO (DERECHO)",                  "credits": 6, "lyrics": "", "number": "", "semester": "", "observations": "", "codeRequired": "",                          "pensumCredit": _pensum_credit_der},
        ],

        officially_enrolled=[
            {"codeSubject": "DER-411", "subjectName": "DERECHO AMBIENTAL",                       "credits": "3", "section": "01", "teacher": "Dr. Luis Henrique da Silva"},
            {"codeSubject": "DER-336", "subjectName": "DERECHO CIVIL VI",                        "credits": "4", "section": "02", "teacher": "Dra. Carmen Aida Ibarra"},
        ],
        unofficial_selected=[
            {"codeSubject": "DER-411", "subjectName": "DERECHO AMBIENTAL",                       "credits": "3", "section": "01", "teacher": "Dr. Luis Henrique da Silva"},
            {"codeSubject": "DER-336", "subjectName": "DERECHO CIVIL VI",                        "credits": "4", "section": "02", "teacher": "Dra. Carmen Aida Ibarra"},
        ],
        gpa_target=2.20,
        facultad="Facultad de Ciencias Jurídicas y Políticas",
    ),
}
