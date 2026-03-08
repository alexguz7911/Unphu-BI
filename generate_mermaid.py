import sys
import re
import traceback

sys.path.append('.')
from src.api.services.unphu_api import UnphuApiService

try:
    id_persona = "126816" # Extracted from the token payload id: 126816

    pensum_data = UnphuApiService._get(f'/getting-pensums-student/?idpersona={id_persona}')
    pensum_list = pensum_data.get('data', [])
    
    # We will build a diagram of the pensum for the user
    # But only subjects that have prerequisites to avoid massive clutter
    
    print("```mermaid")
    print("graph TD;")
    has_links = False
    
    for subj in pensum_list:
        code = subj.get('codeSubject', '').replace(' ', '')
        if not code: continue
        reqs = str(subj.get('codeRequired', '') or '')
        name_clean = subj.get('subject', '').title()
        name_clean = name_clean.replace('"', '\\"') # escape quotes
        node_name = f'    {code}["{code}"]'
        
        has_reqs = False
        if reqs and reqs.strip() and reqs.strip().lower() != 'none':
            reqs = reqs.replace(' o ', ' y ')
            req_list = [r.strip().replace(' ', '') for r in re.split(r'[,y]', reqs) if r.strip()]
            for req in req_list:
                if req: # If req is not completely empty
                    print(f"    {req} --> {code};")
                    has_reqs = True
                    has_links = True

    if not has_links:
        print("    A[No hay prerequisitos registrados] --> B[Ruta Libre];")
        
    print("```")
except Exception as e:
    print(traceback.format_exc())
