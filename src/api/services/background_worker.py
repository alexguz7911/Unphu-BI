import threading
import queue
import time
from datetime import datetime
from src.api.services.unphu_api import UnphuApiService
from src.db.data_warehouse import DataWareHouseSync
import re

# Cola en memoria para procesar en background
sync_queue = queue.Queue()

def background_sync_task():
    print("[WORKER] Started background sync worker thread.")
    while True:
        try:
            task = sync_queue.get()
            if task is None:
                continue
            
            id_persona = task.get("id_persona")
            id_carrera = task.get("id_carrera")
            matricula = task.get("matricula")
            nombre = task.get("nombre")
            api_data = task.get("api_data", {})
            
            print(f"[WORKER] Consumiendo mensaje para {matricula}. Iniciando sincronización profunda...")
            
            # 1. Guardar la data base del estudiante primero (Perfil, Carrera, etc)
            DataWareHouseSync.sync_student_login(api_data, matricula, nombre, id_carrera)
            
            # 2. Iterar en múltiples llamadas desde el año actual hacia el principio de los tiempos
            current_year = datetime.now().year
            
            consecutive_empty = 0
            # Asumiendo que máximo nos iremos 8 años atras (Ej. hasta 2018 si estamos en 2026)
            for year in range(current_year, current_year - 8, -1):
                encontro_en_este_ano = False
                
                for period in [3, 2, 1]:  # Sept-Dic(3), May-Ago(2), Ene-Abr(1)
                    try:
                        # Extraemos notas directamente contra la API legacy para cada especifico slice
                        grades = UnphuApiService.get_semester_grades(year, period, str(id_persona), str(id_carrera))
                        
                        if grades and len(grades) > 0:
                            encontro_en_este_ano = True
                            consecutive_empty = 0
                            
                            # Insertamos estas notas historicas directas en PostgreSQL usando el DW
                            DataWareHouseSync.sync_semester_grades_direct(matricula, str(id_carrera), year, period, grades)
                            print(f"[WORKER] {matricula} - Extraído e inyectado con éxito: Periodo {year}-{period}")
                        
                        # Pequeño cooldown para no colapsar la gateway de la universidad con DDoS
                        time.sleep(0.3)
                    except Exception as e:
                        print(f"[WORKER] API Error trayendo {year}-{period}: {e}")
                
                if not encontro_en_este_ano:
                    consecutive_empty += 1
                
                # Regla de escape: Si durante 2 años seguidos (6 cuatrimestres) no encontramos ninguna nota registrada,
                # probablamente ya llegamos al "principio" de la carrera del estudiante, así que optimizamos rompiendo el ciclo.
                if consecutive_empty >= 2:
                    print(f"[WORKER] {matricula} - Llegamos al principio de sus notas (sin actividad real en los ultimos 2 años iterados). Parando búsqueda.")
                    break
                    
            print(f"[WORKER] ¡Sincronización profunda y encolada completada exitosamente para {matricula}!")
            sync_queue.task_done()
            
        except Exception as e:
            import traceback
            traceback.print_exc()
            print(f"[WORKER] Falla catastrófica en procesado de mensaje de cola: {e}")

def start_worker():
    """ Enciende el worker thread en background al iniciar la App Flask """
    t = threading.Thread(target=background_sync_task, daemon=True)
    t.start()
    
def enqueue_student_sync(id_persona: str, id_carrera: str, matricula: str, nombre: str, api_data: dict):
    """ Encola el evento de login/registro para hacer el scraping exhaustivo de notas pasadas en background """
    print(f"[QUEUE] Añadiendo evento para '{matricula}' a la cola para procesamiento en 2do plano.")
    sync_queue.put({
        "id_persona": id_persona,
        "id_carrera": id_carrera,
        "matricula": matricula,
        "nombre": nombre,
        "api_data": api_data
    })
