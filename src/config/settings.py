import os

# Tu ID de cliente (obtenido el 19/02/2026)
GOOGLE_CLIENT_ID = "88334184799-pimhmhk8fvar5mpttnko7dlvljl21o4i.apps.googleusercontent.com"

# API Base URL
UNPHU_API_BASE_URL = "https://client-api-gateway.unphusist.unphu.edu.do/legacy"
UNPHU_API_TOKEN = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJ1c2VyIjp7ImlkIjoxMjY4MTYsIm5hbWVzIjoiQUxFWEFOREVSIEpJTUVORVogR1VaTUFOIiwidXNlcm5hbWUiOiJhajIwLTEyMDUiLCJlbWFpbCI6ImFqMjAtMTIwNUB1bnBodS5lZHUuZG8iLCJwZXJtaXNzaW9ucyI6WyJhZG1pc3Npb24tcHVibGljLWFwaSIsIkVTVC1SRVRSRUFUUyIsIkVTVC1TRUxFQ1QiLCJFU1RfUkVDRUlQVCJdLCJjYXJlZXIiOiJJTkdFTklFUklBIEVOIFNJU1RFTUFTIENPTVBVVEFDSU9OQUxFUyAyNTUvMy0xNS0xNSIsImVuY2xvc3VyZSI6IlNhbnRvIERvbWluZ28iLCJlbnJvbGxtZW50IjoiMjAtMTIwNSIsInVzZXJUeXBlIjoiU1RVREVOVCJ9LCJzZXJ2aWNlIjp7ImtleSI6ImFkbWlzc2lvbktleSJ9LCJ0eXBlIjoidXNlciIsImlhdCI6MTc3MjY4MDYxMCwiZXhwIjoxNzgwNTcwNjEwfQ.hoFk4CMmUWLZU34axk0WqDr7LHllFbk4-qfnNg2dyLk"

# Configuración de Data Warehouse (SQL Server)
# Reemplazar con credenciales si SQL Server Authentication
SQL_SERVER_CONFIG = {
    'driver': 'ODBC Driver 17 for SQL Server',
    'server': 'DESKTOP-9KVG5RR\\SQLEXPRESS', # Cambiado al servidor de tu screenshot
    'database': 'UnphuBI_DB',
    'trusted_connection': True, # True para autenticación de Windows 
    'username': '',
    'password': ''
}
