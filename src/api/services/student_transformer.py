import re
from typing import Dict, Any, List

def calculate_credits_evaluated(historial: List[Dict[str, Any]]) -> int:
    """Calculates exactly how many credits the student has evaluated for GPA based on valid marks."""
    creditos_evaluados = 0
    for subject in historial:
        let = str(subject.get('lyrics', '')).strip()
        if let in ['A', 'B', 'C', 'D', 'F']:
            c_val = subject.get('credits', 0)
            try:
                # Convertir primero a float por si viene como "3.0" y luego a int
                added_val = int(float(c_val)) if c_val is not None else 0
            except (ValueError, TypeError):
                added_val = 0
            creditos_evaluados += added_val
    return creditos_evaluados

def parse_prerequisites(pending_list: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Assign an 'unlocks' heuristic based on code requirements for the next periods."""
    # Mapa de cuántas veces aparece una materia como prerrequisito
    prereq_counts: Dict[str, int] = {}
    
    for subject in pending_list:
        code_req = str(subject.get('codeRequired', ''))
        code_req = code_req.replace(' o ', ' y ')
        reqs = [r.strip() for r in re.split(r'[,y]', code_req) if r.strip()]
        for req in reqs:
            if req in prereq_counts:
                prereq_counts[req] += 1
            else:
                prereq_counts[req] = 1

    # Asignar peso a cada materia
    for subject in pending_list:
        code = str(subject.get('codeSubject', '')).strip()
        subject['unlocks'] = prereq_counts.get(code, 0)
        subject['code'] = code
        subject['name'] = str(subject.get('subject', '')).strip()

    # Ordenar para que las que más abren materias aparezcan primero
    pending_list.sort(key=lambda x: x.get('unlocks', 0), reverse=True)
    return pending_list

def build_history_by_period(historial: List[Dict[str, Any]]) -> Dict[str, List[Dict[str, Any]]]:
    """Converts the raw history list into a nice dictionary grouped by semester."""
    history_by_period: Dict[str, List[Dict[str, Any]]] = {}
    seen_subject_codes_in_period: Dict[str, set] = {}
    
    for subject in historial:
        per = str(subject.get('semester', '')).strip()
        if not per: continue
        
        code = str(subject.get('codeSubject', '')).strip()
        
        let = str(subject.get('lyrics', '')).strip()
        num = str(subject.get('number', '')).strip()
        obs = str(subject.get('observations', '')).strip()
        
        # Si tiene literal, número u observación, ya fue cursada
        if let or num or obs:
            if per not in history_by_period:
                history_by_period[per] = []
                seen_subject_codes_in_period[per] = set()
                
            # Deduplicar: si ya vimos este código exacto en este periodo, no lo repetimos
            if code and code in seen_subject_codes_in_period[per]:
                continue
            if code:
                seen_subject_codes_in_period[per].add(code)
                
            status = "Cursando"
            if let in ['A', 'B', 'C'] or obs in ['AP', 'EX']:
                status = "Aprobado"
            elif let in ['D', 'F', 'FI'] or obs == 'RP':
                status = "Reprobado"
            elif let == 'R':
                status = "Retirado"
            
            history_by_period[per].append({
                'code': subject.get('codeSubject', '').strip(),
                'name': subject.get('subject', '').strip(),
                'credits': int(subject.get('credits', 0)) if isinstance(subject.get('credits'), (int, str)) and str(subject.get('credits')).isdigit() else 0,
                'grade': num,
                'letter': let,
                'obs': obs,
                'status': status
            })

    def get_sort_key(period_str: str) -> tuple:
        """Parse UNPHU period strings like 'SEP-DIC-2024' or 'ENE-ABR-2025' for sorting."""
        period_upper = period_str.upper()
        match = re.search(r'(ENE|MAY|SEP|AGO|DIC|ABR).*?(\d{4})', period_upper)
        if match:
            month_str = match.group(1)
            year = int(match.group(2))
            month_weight = 0
            if month_str in ['ENE', 'ABR']: month_weight = 1
            elif month_str in ['MAY', 'AGO']: month_weight = 2
            elif month_str in ['SEP', 'DIC']: month_weight = 3
            return (year, month_weight)
        
        # Fallback numeric parse if any
        match_num = re.search(r'\d+', period_upper)
        if match_num:
            return (0, int(match_num.group()))
        return (0, 0)

    # Sort periods descending (most recent first) to match standard chronological views
    sorted_history: Dict[str, List[Dict[str, Any]]] = {}
    for key in sorted(history_by_period.keys(), key=get_sort_key, reverse=True):
        sorted_history[key] = history_by_period[key]

    return sorted_history

