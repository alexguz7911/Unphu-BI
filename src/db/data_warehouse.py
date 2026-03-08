from typing import Dict, Any, Optional
from src.db.connection import DBConnection
from datetime import datetime

class DataWareHouseSync:
    """
    Sincroniza la data extraída de UnphuApiService con la base de datos UnphuBI_DB.
    Insertará o actualizará información en las Dimensiones y Tablas de Hecho.
    """
    
    @staticmethod
    def sync_student_login(api_data: Dict[str, Any], raw_matricula: str, nombre_completo: str):
        """
        Punto de entrada principal para guardar la info cada vez que un estudiante inicia sesión.
        """
        conn = DBConnection.get_connection()
        if not conn:
            return
        
        try:
            cursor = conn.cursor()
            
            # --- 1. DIM_PERIODOS ---
            # Guardamos el período actual si lo tenemos
            current_period_list = api_data.get('current_period', [])
            id_periodo_actual = None
            if current_period_list and len(current_period_list) > 0:
                p_data = current_period_list[0]
                id_periodo_actual = p_data.get('idPeriodo')
                ano = p_data.get('ano', datetime.now().year)
                num_per = p_data.get('numeroPeriodo', 1)
                desc = p_data.get('periodName', f'Periodo {num_per} {ano}')
                
                cursor.execute("""
                    IF NOT EXISTS (SELECT 1 FROM Dim_Periodo WHERE IdPeriodo = ?)
                    BEGIN
                        INSERT INTO Dim_Periodo (IdPeriodo, Ano, NumeroPeriodo, Descripcion, EsPeriodoActual) 
                        VALUES (?, ?, ?, ?, 1)
                    END
                    ELSE
                    BEGIN
                        UPDATE Dim_Periodo 
                        SET Ano=?, NumeroPeriodo=?, Descripcion=?, EsPeriodoActual=1 
                        WHERE IdPeriodo=?
                    END
                """, (id_periodo_actual, id_periodo_actual, ano, num_per, desc, ano, num_per, desc, id_periodo_actual))
            
            
            # --- 2. DIM_ESTUDIANTE ---
            # Identificamos el ID único numérico (Persona) 
            # Como la data de la API lo da regado, lo ideal sería tener el IdPersona, pero al iniciar sesión tenemos email/matricula
            # Para este POC si no tenemos IdPersona usamos un hash o un ID secuencial temporal basado en la matrícula numércia
            import re
            
            id_persona_match = re.search(r'\d+', raw_matricula.replace('-',''))
            id_persona_real = int(id_persona_match.group()) if id_persona_match else 0
                
            cursor.execute("""
                IF NOT EXISTS (SELECT 1 FROM Dim_Estudiante WHERE Matricula = ?)
                BEGIN
                    INSERT INTO Dim_Estudiante (IdPersona, Matricula, NombreCompleto, EmailInstitucional)
                    VALUES (?, ?, ?, ?)
                END
                ELSE
                BEGIN
                    UPDATE Dim_Estudiante 
                    SET NombreCompleto = ?, EmailInstitucional = ?
                    WHERE Matricula = ?
                END
            """, (raw_matricula, id_persona_real, raw_matricula, nombre_completo, f"{raw_matricula}@unphu.edu.do", 
                  nombre_completo, f"{raw_matricula}@unphu.edu.do", raw_matricula))
            
            # --- 3. DIM_CARRERA ---
            # (Basado en el string de la carrera que nos llega e.g MEDICINA / 25)
            carrera_full = api_data.get('carrera', 'Carrera Genérica')
            id_carrera = hash(carrera_full) % 100000 # Demo ID
            
            cursor.execute("""
                IF NOT EXISTS (SELECT 1 FROM Dim_Carrera WHERE IdCarrera = ?)
                BEGIN
                    INSERT INTO Dim_Carrera (IdCarrera, NombreCarrera) VALUES (?, ?)
                END
            """, (id_carrera, id_carrera, carrera_full))
            
            
            # --- 4. ACTUALIZAR ESTUDIANTE CON SU CARRERA ---
            cursor.execute("UPDATE Dim_Estudiante SET IdCarreraActiva = ? WHERE Matricula = ?", (id_carrera, raw_matricula))


            # --- 5. DIM_ASIGNATURAS Y FACT_CALIFICACIONES (HISTORIAL) ---
            historial = api_data.get('history', {})
            for sem_key, sem_data in historial.items():
                
                import re
                periodo_hist_id = abs(hash(str(sem_key))) % 1000000
                ano_match = re.search(r'\d{4}', str(sem_key))
                ano_hist = int(ano_match.group()) if ano_match else datetime.now().year
                
                # Crear periodo si no existe el del historial
                if periodo_hist_id > 0:
                     cursor.execute("""
                        IF NOT EXISTS (SELECT 1 FROM Dim_Periodo WHERE IdPeriodo = ?)
                        BEGIN
                            INSERT INTO Dim_Periodo (IdPeriodo, Ano, NumeroPeriodo, Descripcion) VALUES (?, ?, ?, ?)
                        END
                     """, (periodo_hist_id, periodo_hist_id, ano_hist, 1, str(sem_key)))
                     
                for asig in sem_data:
                    code = asig.get('code', 'UNK')
                    name = asig.get('name', 'Asignatura Desconocida')
                    cred = asig.get('credits', 0)
                    id_asig = abs(hash(code)) % 1000000 # Demo ID
                    
                    # Insertar Dim_Asignatura
                    cursor.execute("""
                        IF NOT EXISTS (SELECT 1 FROM Dim_Asignatura WHERE IdAsignatura = ?)
                        BEGIN
                            INSERT INTO Dim_Asignatura (IdAsignatura, Codigo, Descripcion, Creditos) VALUES (?, ?, ?, ?)
                        END
                    """, (id_asig, id_asig, code, name, cred))
                    
                    # Insertar Fact_Calificacion (si es completada, reprobada o pendiente)
                    raw_grade = asig.get('letter') or asig.get('grade')
                    grade = str(raw_grade)[:10] if raw_grade else ''
                    estatus = 'Completada' if grade in ['A', 'B', 'C', 'D'] else 'Retirada' if grade == 'R' else 'Reprobada' if grade == 'F' else 'Pendiente'
                    aprobada = 1 if estatus == 'Completada' else 0
                    
                    if periodo_hist_id > 0:
                        indice_acumulado = api_data.get('indices', {}).get('cumulativeIndex', None)
                        cursor.execute("""
                            INSERT INTO Fact_Calificaciones 
                                (IdPersona, IdCarrera, IdAsignatura, IdPeriodo, Estatus, NotaLiteral, Aprobada, IndiceAcumulado)
                            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                        """, (id_persona_real, id_carrera, id_asig, periodo_hist_id, estatus, grade, aprobada, indice_acumulado))


            # --- 6. FACT_INSCRIPCIONES (Materias Seleccionadas Actuales) ---
            if id_periodo_actual:
                selected = api_data.get('selected_subjects', [])
                for asig in selected:
                    code = asig.get('subjectCode', 'UNK')
                    name = asig.get('subjectName', 'Desconocida')
                    cred = asig.get('credits', 0)
                    id_asig = hash(code) % 1000000
                    
                    # Asegurar Asignatura
                    cursor.execute("""
                        IF NOT EXISTS (SELECT 1 FROM Dim_Asignatura WHERE IdAsignatura = ?)
                        BEGIN
                            INSERT INTO Dim_Asignatura (IdAsignatura, Codigo, Descripcion, Creditos) VALUES (?, ?, ?, ?)
                        END
                    """, (id_asig, id_asig, code, name, cred))
                    
                    # Insertar Transacción
                    cursor.execute("""
                        INSERT INTO Fact_Inscripciones (IdPersona, IdPeriodo, IdAsignatura, Tipo)
                        VALUES (?, ?, ?, 'Seleccionada')
                    """, (id_persona_real, id_periodo_actual, id_asig))


            conn.commit()
            print(f"✅ Sincronización exitosa en SQL Server para el estudiante: {raw_matricula}")
        except Exception as e:
            conn.rollback()
            print(f"❌ Error durante sincronización DW SQL: {e}")
        finally:
            cursor.close()
            conn.close()

    @staticmethod
    def get_student_ranking(matricula: str) -> dict:
        conn = DBConnection.get_connection()
        if not conn:
            return {"rank": "--", "total": "--"}
        
        try:
            cursor = conn.cursor()
            query = """
                WITH CTE_StudentIndex AS (
                    SELECT 
                        E.Matricula,
                        MAX(F.IndiceAcumulado) as IndiceTotal
                    FROM Dim_Estudiante E
                    JOIN Fact_Calificaciones F ON E.IdPersona = F.IdPersona
                    JOIN Dim_Estudiante E2 ON E.IdCarreraActiva = E2.IdCarreraActiva 
                    WHERE E2.Matricula = ?
                    GROUP BY E.Matricula
                ),
                CTE_Ranked AS (
                    SELECT 
                        Matricula,
                        IndiceTotal,
                        RANK() OVER (ORDER BY IndiceTotal DESC) as Posicion
                    FROM CTE_StudentIndex
                    WHERE IndiceTotal IS NOT NULL
                )
                SELECT 
                    (SELECT Posicion FROM CTE_Ranked WHERE Matricula = ?) as RankEstudiante,
                    (SELECT COUNT(*) FROM CTE_Ranked) as TotalEstudiantes
            """
            cursor.execute(query, (matricula, matricula))
            row = cursor.fetchone()
            
            if row and row.RankEstudiante and row.TotalEstudiantes:
                return {"rank": row.RankEstudiante, "total": row.TotalEstudiantes}
            else:
                return {"rank": "--", "total": "--"}
        except Exception as e:
            print(f"❌ Error getting student ranking from DW SQL: {e}")
            return {"rank": "--", "total": "--"}
        finally:
            if 'cursor' in locals() and cursor:
                cursor.close()
            if conn:
                conn.close()

