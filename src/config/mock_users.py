# src/config/mock_users.py
# Contiene los datos mock de estudiantes para presentación y demo.

MOCK_USERS = {
    "ms21-2083": {
        "student_data": {
            "id": 212083,
            "names": "MARIA ELENA SANTOS ALMONTE",
            "username": "ms21-2083",
            "email": "ms21-2083@unphu.edu.do",
            "career": "INGENIERIA EN SISTEMAS COMPUTACIONALES 255/3-15-15",
            "enclosure": "Santo Domingo",
            "enrollment": "21-2083",
            "userType": "STUDENT"
        },
        "careers": [
            {
                "IdPersona": 212083,
                "IdCarrera": 251,  # Matches target career in PostgreSQL seed
                "NombreCarrera": "INGENIERIA EN SISTEMAS COMPUTACIONALES 255/3-15-15"
            }
        ],
        "pending_grades": [
            # Historial Académico Completado
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
            {"codeSubject": "INF-501", "subject": "PROGRAMACION WEB", "credits": "4", "lyrics": "A", "number": "95", "semester": "SEP-DIC 2025", "observations": "", "codeRequired": "INF-302", "pensumCredit": "216"},
            # Materias Pendientes
            {"codeSubject": "INF-604", "subject": "MINERIA DE DATOS", "credits": "4", "lyrics": "", "number": "", "semester": "", "observations": "", "codeRequired": "INF-302", "pensumCredit": "216"},
            {"codeSubject": "INF-901", "subject": "TRABAJO DE GRADO", "credits": "6", "lyrics": "", "number": "", "semester": "", "observations": "", "codeRequired": "INF-501 y INF-301", "pensumCredit": "216"}
        ],
        "semester_grades": {
            "2024-1": [{"cumulativeIndex": 3.80, "semesterIndex": 3.80, "subjectCode": "INF-101", "credits": 3, "grade": "A"}],
            "2024-2": [{"cumulativeIndex": 3.75, "semesterIndex": 3.70, "subjectCode": "INF-102", "credits": 4, "grade": "A"}],
            "2024-3": [{"cumulativeIndex": 3.78, "semesterIndex": 3.85, "subjectCode": "INF-201", "credits": 4, "grade": "A"}],
            "2025-1": [{"cumulativeIndex": 3.81, "semesterIndex": 3.90, "subjectCode": "INF-302", "credits": 4, "grade": "A"}],
            "2025-2": [{"cumulativeIndex": 3.77, "semesterIndex": 3.60, "subjectCode": "INF-401", "credits": 4, "grade": "A"}],
            "2025-3": [{"cumulativeIndex": 3.80, "semesterIndex": 3.90, "subjectCode": "INF-501", "credits": 4, "grade": "A"}]
        },
        "officially_enrolled": [
            {"subjectCode": "INF-601", "subjectName": "INTELIGENCIA ARTIFICIAL", "credits": "3", "section": "01", "teacher": "Dr. Alan Turing"},
            {"subjectCode": "INF-602", "subjectName": "SEGURIDAD DE LA INFORMACION", "credits": "3", "section": "02", "teacher": "Ing. Bruce Schneier"}
        ],
        "unofficial_selected": [
            {"subjectCode": "INF-603", "subjectName": "DESARROLLO MOVIL", "credits": "4", "section": "01", "teacher": "Ing. Grace Hopper"}
        ]
    },
    "cm22-0941": {
        "student_data": {
            "id": 220941,
            "names": "CARLOS ALBERTO MENDOZA RUIZ",
            "username": "cm22-0941",
            "email": "cm22-0941@unphu.edu.do",
            "career": "ARQUITECTURA 102/2-22-22",
            "enclosure": "Santo Domingo",
            "enrollment": "22-0941",
            "userType": "STUDENT"
        },
        "careers": [
            {
                "IdPersona": 220941,
                "IdCarrera": 6,  # Matches Architecture ID in Postgres seed
                "NombreCarrera": "ARQUITECTURA 102/2-22-22"
            }
        ],
        "pending_grades": [
            {"codeSubject": "ARQ-101", "subject": "DIBUJO TECNICO", "credits": "3", "lyrics": "A", "number": "90", "semester": "ENE-ABR 2024", "observations": "", "codeRequired": "", "pensumCredit": "230"},
            {"codeSubject": "ARQ-102", "subject": "INTRODUCCION A LA ARQUITECTURA", "credits": "4", "lyrics": "B", "number": "82", "semester": "ENE-ABR 2024", "observations": "", "codeRequired": "", "pensumCredit": "230"},
            {"codeSubject": "ARQ-201", "subject": "DISENO ARQUITECTONICO I", "credits": "5", "lyrics": "A", "number": "91", "semester": "MAY-AGO 2024", "observations": "", "codeRequired": "ARQ-101", "pensumCredit": "230"},
            {"codeSubject": "ARQ-202", "subject": "GEOMETRIA DESCRIPTIVA", "credits": "3", "lyrics": "B", "number": "85", "semester": "MAY-AGO 2024", "observations": "", "codeRequired": "ARQ-101", "pensumCredit": "230"},
            {"codeSubject": "ARQ-301", "subject": "DISENO ARQUITECTONICO II", "credits": "5", "lyrics": "B", "number": "88", "semester": "SEP-DIC 2024", "observations": "", "codeRequired": "ARQ-201", "pensumCredit": "230"},
            {"codeSubject": "ARQ-302", "subject": "HISTORIA DE LA ARQUITECTURA I", "credits": "3", "lyrics": "A", "number": "93", "semester": "SEP-DIC 2024", "observations": "", "codeRequired": "ARQ-102", "pensumCredit": "230"},
            {"codeSubject": "ARQ-303", "subject": "TECNOLOGIA DE CONSTRUCCION I", "credits": "4", "lyrics": "C", "number": "78", "semester": "ENE-ABR 2025", "observations": "", "codeRequired": "ARQ-202", "pensumCredit": "230"},
            {"codeSubject": "ARQ-401", "subject": "DISENO ARQUITECTONICO III", "credits": "5", "lyrics": "A", "number": "92", "semester": "MAY-AGO 2025", "observations": "", "codeRequired": "ARQ-301", "pensumCredit": "230"},
            # Materias Pendientes
            {"codeSubject": "ARQ-501", "subject": "DISENO ARQUITECTONICO IV", "credits": "5", "lyrics": "", "number": "", "semester": "", "observations": "", "codeRequired": "ARQ-401", "pensumCredit": "230"},
            {"codeSubject": "ARQ-502", "subject": "URBANISMO I", "credits": "3", "lyrics": "", "number": "", "semester": "", "observations": "", "codeRequired": "ARQ-302", "pensumCredit": "230"}
        ],
        "semester_grades": {
            "2024-1": [{"cumulativeIndex": 3.30, "semesterIndex": 3.30, "subjectCode": "ARQ-101", "credits": 3, "grade": "A"}],
            "2024-2": [{"cumulativeIndex": 3.40, "semesterIndex": 3.50, "subjectCode": "ARQ-201", "credits": 5, "grade": "A"}],
            "2024-3": [{"cumulativeIndex": 3.42, "semesterIndex": 3.45, "subjectCode": "ARQ-301", "credits": 5, "grade": "B"}],
            "2025-1": [{"cumulativeIndex": 3.40, "semesterIndex": 3.35, "subjectCode": "ARQ-303", "credits": 4, "grade": "C"}],
            "2025-2": [{"cumulativeIndex": 3.42, "semesterIndex": 3.50, "subjectCode": "ARQ-401", "credits": 5, "grade": "A"}]
        },
        "officially_enrolled": [
            {"subjectCode": "ARQ-501", "subjectName": "DISENO ARQUITECTONICO IV", "credits": "5", "section": "01", "teacher": "Arq. Frank Lloyd Wright"},
            {"subjectCode": "ARQ-502", "subjectName": "URBANISMO I", "credits": "3", "section": "01", "teacher": "Arq. Le Corbusier"}
        ],
        "unofficial_selected": [
            {"subjectCode": "ARQ-503", "subjectName": "RESTAURACION DE MONUMENTOS", "credits": "3", "section": "02", "teacher": "Arq. Zaha Hadid"}
        ]
    },
    "hp19-0112": {
        "student_data": {
            "id": 190112,
            "names": "HILDA PATRICIA PEREZ MEJIA",
            "username": "hp19-0112",
            "email": "hp19-0112@unphu.edu.do",
            "career": "MEDICINA 301/1-12-12",
            "enclosure": "Santo Domingo",
            "enrollment": "19-0112",
            "userType": "STUDENT"
        },
        "careers": [
            {
                "IdPersona": 190112,
                "IdCarrera": 301112,
                "NombreCarrera": "MEDICINA 301/1-12-12"
            }
        ],
        "pending_grades": [
            {"codeSubject": "MED-101", "subject": "BIOLOGIA BASICA", "credits": "4", "lyrics": "B", "number": "84", "semester": "ENE-ABR 2023", "observations": "", "codeRequired": "", "pensumCredit": "310"},
            {"codeSubject": "MED-102", "subject": "QUIMICA ORGANICA", "credits": "4", "lyrics": "C", "number": "76", "semester": "ENE-ABR 2023", "observations": "", "codeRequired": "", "pensumCredit": "310"},
            {"codeSubject": "MED-201", "subject": "ANATOMIA HUMANA I", "credits": "6", "lyrics": "B", "number": "88", "semester": "MAY-AGO 2023", "observations": "", "codeRequired": "MED-101", "pensumCredit": "310"},
            {"codeSubject": "MED-202", "subject": "HISTOLOGIA", "credits": "4", "lyrics": "B", "number": "85", "semester": "MAY-AGO 2023", "observations": "", "codeRequired": "MED-101", "pensumCredit": "310"},
            {"codeSubject": "MED-301", "subject": "ANATOMIA HUMANA II", "credits": "6", "lyrics": "C", "number": "79", "semester": "SEP-DIC 2023", "observations": "", "codeRequired": "MED-201", "pensumCredit": "310"},
            {"codeSubject": "MED-302", "subject": "EMBRIOLOGIA", "credits": "3", "lyrics": "B", "number": "83", "semester": "SEP-DIC 2023", "observations": "", "codeRequired": "MED-202", "pensumCredit": "310"},
            {"codeSubject": "MED-401", "subject": "BIOQUIMICA MEDICA", "credits": "5", "lyrics": "A", "number": "90", "semester": "ENE-ABR 2024", "observations": "", "codeRequired": "MED-102", "pensumCredit": "310"},
            {"codeSubject": "MED-402", "subject": "FISIOLOGIA I", "credits": "5", "lyrics": "B", "number": "86", "semester": "MAY-AGO 2024", "observations": "", "codeRequired": "MED-301", "pensumCredit": "310"},
            {"codeSubject": "MED-403", "subject": "MICROBIOLOGIA I", "credits": "4", "lyrics": "B", "number": "84", "semester": "SEP-DIC 2024", "observations": "", "codeRequired": "MED-202", "pensumCredit": "310"},
            # Materias Pendientes
            {"codeSubject": "MED-501", "subject": "FISIOLOGIA HUMANA II", "credits": "4", "lyrics": "", "number": "", "semester": "", "observations": "", "codeRequired": "MED-402", "pensumCredit": "310"},
            {"codeSubject": "MED-502", "subject": "FARMACOLOGIA MEDICA", "credits": "4", "lyrics": "", "number": "", "semester": "", "observations": "", "codeRequired": "MED-401", "pensumCredit": "310"}
        ],
        "semester_grades": {
            "2023-1": [{"cumulativeIndex": 3.00, "semesterIndex": 3.00, "subjectCode": "MED-101", "credits": 4, "grade": "B"}],
            "2023-2": [{"cumulativeIndex": 3.10, "semesterIndex": 3.20, "subjectCode": "MED-201", "credits": 6, "grade": "B"}],
            "2023-3": [{"cumulativeIndex": 3.10, "semesterIndex": 3.10, "subjectCode": "MED-301", "credits": 6, "grade": "C"}],
            "2024-1": [{"cumulativeIndex": 3.11, "semesterIndex": 3.15, "subjectCode": "MED-401", "credits": 5, "grade": "A"}],
            "2024-2": [{"cumulativeIndex": 3.14, "semesterIndex": 3.25, "subjectCode": "MED-402", "credits": 5, "grade": "B"}],
            "2024-3": [{"cumulativeIndex": 3.15, "semesterIndex": 3.20, "subjectCode": "MED-403", "credits": 4, "grade": "B"}]
        },
        "officially_enrolled": [
            {"subjectCode": "MED-501", "subjectName": "FISIOLOGIA HUMANA II", "credits": "4", "section": "01", "teacher": "Dra. Elizabeth Blackwell"},
            {"subjectCode": "MED-502", "subjectName": "FARMACOLOGIA MEDICA", "credits": "4", "section": "03", "teacher": "Dr. Alexander Fleming"}
        ],
        "unofficial_selected": [
            {"subjectCode": "MED-503", "subjectName": "MICROBIOLOGIA CLINICA", "credits": "3", "section": "01", "teacher": "Dr. Louis Pasteur"}
        ]
    },
    "mr21-1402": {
        "student_data": {
            "id": 211402,
            "names": "MIGUEL ANGEL RODRIGUEZ PEÑA",
            "username": "mr21-1402",
            "email": "mr21-1402@unphu.edu.do",
            "career": "DERECHO 401/2-10-10",
            "enclosure": "Santo Domingo",
            "enrollment": "21-1402",
            "userType": "STUDENT"
        },
        "careers": [
            {
                "IdPersona": 211402,
                "IdCarrera": 401210,
                "NombreCarrera": "DERECHO 401/2-10-10"
            }
        ],
        "pending_grades": [
            {"codeSubject": "DER-101", "subject": "INTRODUCCION AL DERECHO I", "credits": "3", "lyrics": "A", "number": "90", "semester": "ENE-ABR 2024", "observations": "", "codeRequired": "", "pensumCredit": "180"},
            {"codeSubject": "DER-102", "subject": "DERECHO ROMANO I", "credits": "3", "lyrics": "A", "number": "92", "semester": "ENE-ABR 2024", "observations": "", "codeRequired": "", "pensumCredit": "180"},
            {"codeSubject": "DER-201", "subject": "DERECHO CIVIL I", "credits": "4", "lyrics": "B", "number": "85", "semester": "MAY-AGO 2024", "observations": "", "codeRequired": "DER-101", "pensumCredit": "180"},
            {"codeSubject": "DER-202", "subject": "DERECHO PENAL GENERAL", "credits": "4", "lyrics": "A", "number": "94", "semester": "MAY-AGO 2024", "observations": "", "codeRequired": "DER-101", "pensumCredit": "180"},
            {"codeSubject": "DER-301", "subject": "DERECHO CONSTITUCIONAL I", "credits": "3", "lyrics": "A", "number": "91", "semester": "SEP-DIC 2024", "observations": "", "codeRequired": "DER-101", "pensumCredit": "180"},
            {"codeSubject": "DER-302", "subject": "DERECHO COMERCIAL I", "credits": "3", "lyrics": "B", "number": "87", "semester": "SEP-DIC 2024", "observations": "", "codeRequired": "DER-201", "pensumCredit": "180"},
            {"codeSubject": "DER-401", "subject": "DERECHO PROCESAL PENAL I", "credits": "4", "lyrics": "A", "number": "93", "semester": "ENE-ABR 2025", "observations": "", "codeRequired": "DER-202", "pensumCredit": "180"},
            # Materias Pendientes
            {"codeSubject": "DER-501", "subject": "DERECHO CONSTITUCIONAL II", "credits": "3", "lyrics": "", "number": "", "semester": "", "observations": "", "codeRequired": "DER-301", "pensumCredit": "180"},
            {"codeSubject": "DER-502", "subject": "DERECHO PROCESAL CIVIL I", "credits": "4", "lyrics": "", "number": "", "semester": "", "observations": "", "codeRequired": "DER-201", "pensumCredit": "180"}
        ],
        "semester_grades": {
            "2024-1": [{"cumulativeIndex": 3.50, "semesterIndex": 3.50, "subjectCode": "DER-101", "credits": 3, "grade": "A"}],
            "2024-2": [{"cumulativeIndex": 3.60, "semesterIndex": 3.70, "subjectCode": "DER-201", "credits": 4, "grade": "B"}],
            "2024-3": [{"cumulativeIndex": 3.62, "semesterIndex": 3.65, "subjectCode": "DER-301", "credits": 3, "grade": "A"}],
            "2025-1": [{"cumulativeIndex": 3.65, "semesterIndex": 3.72, "subjectCode": "DER-401", "credits": 4, "grade": "A"}]
        },
        "officially_enrolled": [
            {"subjectCode": "DER-501", "subjectName": "DERECHO CONSTITUCIONAL II", "credits": "3", "section": "02", "teacher": "Dra. Ruth Bader Ginsburg"},
            {"subjectCode": "DER-502", "subjectName": "DERECHO PROCESAL CIVIL I", "credits": "4", "section": "01", "teacher": "Dr. Andrés Bello"}
        ],
        "unofficial_selected": [
            {"subjectCode": "DER-503", "subjectName": "DERECHO INTERNACIONAL PUBLICO", "credits": "3", "section": "01", "teacher": "Dr. Hugo Grocio"}
        ]
    }
}
