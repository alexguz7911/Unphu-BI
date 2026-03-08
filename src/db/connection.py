from typing import Optional, Any
from src.config.settings import SQL_SERVER_CONFIG

class DBConnection:
    """Maneja la conexión a SQL Server usando pyodbc."""
    
    @staticmethod
    def get_connection() -> Optional[Any]:
        try:
            import pyodbc # Import local para no crashear Vercel si falla la lib C++
            conn_str = (
                f"DRIVER={{{SQL_SERVER_CONFIG['driver']}}};"
                f"SERVER={SQL_SERVER_CONFIG['server']};"
                f"DATABASE={SQL_SERVER_CONFIG['database']};"
            )
            
            # Autenticación Integrada (Windows Auth) vs SQL Server Auth
            if SQL_SERVER_CONFIG.get('trusted_connection'):
                conn_str += "Trusted_Connection=yes;"
            else:
                conn_str += f"UID={SQL_SERVER_CONFIG['username']};PWD={SQL_SERVER_CONFIG['password']};"
                
            return pyodbc.connect(conn_str)
        except ImportError:
            print("❌ PyODBC no está instalado o no es compatible con este SO (ej: Vercel).")
            return None
        except Exception as e:
            print(f"❌ Error conectando a SQL Server: {e}")
            return None
