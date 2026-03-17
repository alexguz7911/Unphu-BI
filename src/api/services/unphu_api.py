import urllib3 # type: ignore
import requests as req # type: ignore
from typing import Any, Dict, List, Optional
from src.config.settings import UNPHU_API_BASE_URL, UNPHU_API_TOKEN

# Disable insecure request warnings globally for this service since we're ignoring SSL verification.
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

class UnphuApiService:
    """
    Service responsible for interacting with the legacy UNPHU API.
    All methods that hit external endpoints should be enclosed here.
    """

    @staticmethod
    def _get(endpoint: str) -> Dict[str, Any]:
        """Helper to consistently handle external API requests."""
        url = f'{UNPHU_API_BASE_URL}{endpoint}'
        headers = {}
        if UNPHU_API_TOKEN:
            headers['Authorization'] = f'Bearer {UNPHU_API_TOKEN}'

        try:
            res = req.get(url, headers=headers, verify=False, timeout=5)
            if res.status_code == 200:
                return res.json()
            else:
                print(f"API Error {res.status_code} for {url} - Response: {res.text}")
        except Exception as e:
            print(f"API Request failed for {url}: {e}")
        return {}

    @staticmethod
    def get_student_data(matricula: str) -> Dict[str, Any]:
        data = UnphuApiService._get(f'/student-data/{matricula}')
        response_data = data.get('data', {})
        if not isinstance(response_data, dict):
            return {}
        return response_data

    @staticmethod
    def get_student_careers(id_persona: str) -> List[Dict[str, Any]]:
        data = UnphuApiService._get(f'/get-student-careers/?IdPersona={id_persona}')
        data_list = data.get('data', [])
        if not isinstance(data_list, list):
            return []
        return data_list

    @staticmethod
    def get_pending_grades(id_persona: str, id_carrera: str) -> List[Dict[str, Any]]:
        data = UnphuApiService._get(f'/pending-grades-students/?IdPersona={id_persona}&IdCarrera={id_carrera}')
        pending_list = data.get('data', [])
        if not isinstance(pending_list, list):
            return []
        return pending_list
        
    @staticmethod
    def get_semester_grades(year: int, period: int, id_persona: str, id_carrera: str) -> List[Dict[str, Any]]:
        data = UnphuApiService._get(f'/semester-grades/?Ano={year}&IdPersona={id_persona}&IdPeriodo={period}&IdCarrera={id_carrera}')
        grades_list = data.get('data', [])
        if not isinstance(grades_list, list):
            return []
        return grades_list

    @staticmethod
    def get_officially_enrolled(year: int, period: int, id_persona: str, id_carrera: str) -> List[Dict[str, Any]]:
        data = UnphuApiService._get(f'/officially-enrolled-subjects/?Ano={year}&IdPersona={id_persona}&IdPeriodo={period}&IdCarrera={id_carrera}')
        enrolled = data.get('data', [])
        if not isinstance(enrolled, list):
            return []
        return enrolled

    @staticmethod
    def get_unofficial_selected(year: int, period: int, id_persona: str, id_carrera: str) -> List[Dict[str, Any]]:
        data = UnphuApiService._get(f'/unofficial-selected-subjects/?Ano={year}&IdPersona={id_persona}&IdPeriodo={period}&IdCarrera={id_carrera}')
        selected = data.get('data', [])
        if not isinstance(selected, list):
            return []
        return selected
    
    @staticmethod
    def get_current_period() -> Dict[str, Any]:
        data = UnphuApiService._get(f'/get-current-period/')
        period_data = data.get('data', {})
        if not isinstance(period_data, dict):
            return {}
        return period_data
