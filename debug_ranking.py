
import sys
import os
sys.path.append('c:/Users/Alexander Jimenez/Desktop/Unphu-BI')
from src.config.settings import POSTGRES_CONFIG
import psycopg2

def run():
    try:
        conn = psycopg2.connect(
            host=POSTGRES_CONFIG['host'],
            database=POSTGRES_CONFIG['database'],
            user=POSTGRES_CONFIG['user'],
            password=POSTGRES_CONFIG['password'],
            port=POSTGRES_CONFIG['port']
        )
        c = conn.cursor()
        
        print("=== ESTUDIANTES EN DIM_ESTUDIANTE ===")
        c.execute("""
            SELECT E.Matricula, E.NombreCompleto, C.NombreCarrera, E.IdCarreraActiva 
            FROM Dim_Estudiante E
            LEFT JOIN Dim_Carrera C ON E.IdCarreraActiva = C.IdCarrera
        """)
        for r in c.fetchall():
            print(r)
            
        print("\n=== CONTEO DE MATRICULAS UNICAS EN FACT_CALIFICACIONES ===")
        c.execute("""
            SELECT COUNT(DISTINCT IdPersona) FROM Fact_Calificaciones
        """)
        print(f"Total Personas con Notas: {c.fetchone()[0]}")
        
        print("\n=== RANKING QUERY TEST FOR aj20-1205 ===")
        matricula = 'aj20-1205'
        query = """
                WITH CTE_StudentIndex AS (
                    SELECT 
                        E.Matricula,
                        MAX(F.IndiceAcumulado) as IndiceTotal
                    FROM Dim_Estudiante E
                    JOIN Fact_Calificaciones F ON E.IdPersona = F.IdPersona
                    JOIN Dim_Carrera C_F ON F.IdCarrera = C_F.IdCarrera
                    WHERE C_F.NombreCarrera = (
                        SELECT C.NombreCarrera 
                        FROM Dim_Estudiante E2
                        JOIN Dim_Carrera C ON E2.IdCarreraActiva = C.IdCarrera
                        WHERE E2.Matricula = %s LIMIT 1
                    )
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
                    Matricula,
                    IndiceTotal,
                    Posicion,
                    (SELECT COUNT(*) FROM CTE_Ranked) as Total
                FROM CTE_Ranked
        """
        c.execute(query, (matricula,))
        for r in c.fetchall():
            print(r)
            
        conn.close()
    except Exception as e:
        print(f"Error: {e}")

if __name__ == '__main__':
    run()
