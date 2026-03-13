from typing import Optional, Any
from src.config.settings import POSTGRES_CONFIG
import os

class DBConnection:
    """Maneja la conexión a PostgreSQL usando psycopg2."""
    
    @staticmethod
    def get_connection() -> Optional[Any]:
        try:
            import psycopg2 # Import local
            
            # Vercel and other cloud providers usually give a POSTGRES_URL or DATABASE_URL connection string directly
            env_url = os.getenv('POSTGRES_URL') or os.getenv('DATABASE_URL')
            if env_url:
                return psycopg2.connect(env_url)
                
            return psycopg2.connect(
                host=POSTGRES_CONFIG['host'],
                database=POSTGRES_CONFIG['database'],
                user=POSTGRES_CONFIG['user'],
                password=POSTGRES_CONFIG['password'],
                port=POSTGRES_CONFIG['port']
            )
        except ImportError:
            print("❌ psycopg2 no está instalado. Ejecuta: pip install psycopg2-binary")
            return None
        except Exception as e:
            print(f"❌ Error conectando a PostgreSQL: {e}")
            return None
