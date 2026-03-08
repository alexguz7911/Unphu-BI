import os
from flask import Blueprint, send_from_directory, current_app # type: ignore

static_bp = Blueprint('static_routes', __name__)

@static_bp.route('/')
def index():
    return send_from_directory(current_app.static_folder, 'index.html')

@static_bp.route('/callback')
def callback():
    return send_from_directory(current_app.static_folder, 'index.html')

@static_bp.route('/<path:path>')
def serve_static(path):
    return send_from_directory(current_app.static_folder, path)
