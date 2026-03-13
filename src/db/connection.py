from typing import Optional, Any
from src.config.settings import POSTGRES_CONFIG

class DBConnection:
    """Maneja la conexión a PostgreSQL usando psycopg2."""
    
    @staticmethod
    def get_connection() -> Optional[Any]:
        try:
            import psycopg2 # Import local
            
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
