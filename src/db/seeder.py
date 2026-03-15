
import os
import sys

# Asegurar que el path del proyecto esté disponible para las importaciones
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))

from src.api.services.student_sync_service import StudentSyncService
import time

def run_seeder(file_path: str = None, manual_list: list = None):
    """
    Lee matrículas de un archivo o lista y sincroniza su data completa al Data Warehouse.
    """
    matriculas = []
    
    if file_path and os.path.exists(file_path):
        print(f"📖 Leyendo matrículas desde: {file_path}")
        with open(file_path, 'r', encoding='utf-8') as f:
            matriculas = [line.strip() for line in f if line.strip()]
    elif manual_list:
        matriculas = manual_list
    else:
        print("❌ No se proporcionaron matrículas para sembrar (seed).")
        return

    print(f"🚀 Iniciando proceso de Seed para {len(matriculas)} estudiantes...")
    
    success_count = 0
    fail_count = 0
    
    for i, matricula in enumerate(matriculas):
        print(f"[{i+1}/{len(matriculas)}] Procesando: {matricula}...", end="", flush=True)
        try:
            # Llamamos al servicio centralizado que hace todo el fetch y sync
            res = StudentSyncService.fetch_and_sync_all(matricula)
            
            if res and res.get('matricula'):
                print(f" ✅ [OK] - {res.get('nombre')} ({res.get('carrera')})")
                success_count += 1
            else:
                print(" ❌ [ERROR] - No se obtuvo data del API")
                fail_count += 1
                
        except Exception as e:
            print(f" ❌ [CRASH] - {str(e)}")
            fail_count += 1
            
        # Pequeño delay para no saturar la API de la UNPHU si la lista es grande
        if len(matriculas) > 1:
            time.sleep(0.5)

    print("\n" + "="*40)
    print(f"✨ Proceso Finalizado ✨")
    print(f"✅ Sincronizados: {success_count}")
    print(f"❌ Fallidos: {fail_count}")
    print("="*40)

if __name__ == "__main__":
    # Si se ejecuta directamente, busca un archivo local o usa una de prueba
    seed_file = os.path.join(os.path.dirname(__file__), 'matriculas_seed.txt')
    
    if len(sys.argv) > 1:
        # Permite pasar una matrícula manual: python seeder.py aj20-1205
        run_seeder(manual_list=[sys.argv[1]])
    elif os.path.exists(seed_file):
        run_seeder(file_path=seed_file)
    else:
        print(f"💡 No se encontró '{seed_file}'. Crea el archivo con una matrícula por línea o pasa una como argumento.")
        # Ejemplo de uso interno si se quiere probar rápido
        # run_seeder(manual_list=["aj20-1205", "ms21-2083"])
