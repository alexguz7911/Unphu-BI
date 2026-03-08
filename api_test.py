import requests # type: ignore
import json
import urllib3 # type: ignore
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

matricula = "aj20-1205"

try:
    url = f'https://client-api-gateway.unphusist.unphu.edu.do/legacy/student-data/{matricula}'
    res = requests.get(url, verify=False)
    data_estudiante = res.json()
    id_persona = data_estudiante['data']['id']
    with open('api_output.txt', 'w', encoding='utf-8') as f:
        f.write("Estudiante keys: " + str(data_estudiante['data'].keys()) + "\n")
        f.write("Estudiante: " + str(data_estudiante['data']) + "\n")
        
        url = f'https://client-api-gateway.unphusist.unphu.edu.do/legacy/get-student-careers/?IdPersona={id_persona}'
        res = requests.get(url, verify=False)
        data_carrera = res.json()
        id_carrera = data_carrera['data'][0]['IdCarrera']
        
        url = f'https://client-api-gateway.unphusist.unphu.edu.do/legacy/pending-grades-students/?IdPersona={id_persona}&IdCarrera={id_carrera}'
        res = requests.get(url, verify=False)
        pending_grades = res.json().get('data', [])
        f.write(f"Grades history items: {len(pending_grades)}\n")
        
        if len(pending_grades) > 0:
            f.write("Sample grade:\n" + json.dumps(pending_grades[-1], indent=2) + "\n")
            
        url = f'https://client-api-gateway.unphusist.unphu.edu.do/legacy/getting-pensums-student/?idpersona={id_persona}'
        res = requests.get(url, verify=False)
        pensum_data = res.json().get('data', [])
        f.write(f"Pensum total subjects: {len(pensum_data)}\n")
        
        if len(pensum_data) > 0:
            f.write("Sample pensum:\n" + json.dumps(pensum_data[0], indent=2) + "\n")

except Exception as e:
    import traceback
    with open('api_output.txt', 'w', encoding='utf-8') as f:
        f.write(traceback.format_exc())
