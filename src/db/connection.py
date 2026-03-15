from typing import Optional, Any
from src.config.settings import POSTGRES_CONFIG
import os

class DBConnection:
    """Maneja la conexión a PostgreSQL usando psycopg2."""
    
    @staticmethod
    def get_connection() -> Optional[Any]:
        import psycopg2 # Import local
        
        # 1. Intentar con URL de entorno (Cloud / Docker)
        env_url = os.getenv('POSTGRES_URL') or os.getenv('DATABASE_URL')
        if env_url:
            try:
                return psycopg2.connect(env_url)
            except Exception as e:
                print(f"⚠️ Falló conexión por URL de entorno, reintentando con config local... ({e})")

        # 2. Fallback o Principal: Configuración Local (Settings)
        try:
            return psycopg2.connect(
                host=POSTGRES_CONFIG['host'],
                database=POSTGRES_CONFIG['database'],
                user=POSTGRES_CONFIG['user'],
                password=POSTGRES_CONFIG['password'],
                port=POSTGRES_CONFIG['port']
            )
        except Exception as e:
            print(f"❌ Error final conectando a PostgreSQL (Local Config): {e}")
            return None
