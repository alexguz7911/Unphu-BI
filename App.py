from flask import Flask # type: ignore
from flask_cors import CORS # type: ignore
import sys
import os

# Asegurar que el path raíz está en `sys.path` para que los importes absolutos desde "src." funcionen
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

def create_app() -> Flask:
    """Configura e inicializa la aplicación Flask."""
    app = Flask(__name__, static_folder='src/static', static_url_path='')
    
    # Habilitamos CORS
    CORS(app)
    
    # Registramos las Rutas (Blueprints)
    from src.api.routes.static_routes import static_bp
    from src.api.routes.auth_routes import auth_bp
    
    app.register_blueprint(static_bp)
    app.register_blueprint(auth_bp)
    
    # Iniciar procesos asíncronos en segundo plano (Desactivado para Vercel)
    # from src.api.services.background_worker import start_worker
    # start_worker()
    
    return app

app = create_app()

if __name__ == '__main__':
    app.run(debug=True, port=5000)