import sys
import os
import csv
import openpyxl

# Add project root to path
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(os.path.dirname(current_dir))
sys.path.append(os.path.join(project_root, 'src'))

from modules.dataloaders import scan_rem_files, get_rem_sheet_data
from modules.utils import normalize_path, log_cuarentena_valor_invalido
import config

def calcular_meta_6(agno=None, max_mes=None):
    # Parameters
    agno_eval = int(agno) if agno else config.AGNO_ACTUAL
    m_limit = int(max_mes) if max_mes else int(os.environ.get('MAX_MES', 12))

    print(f"=== Calculando Meta 6: Lactancia Materna Exclusiva (LME) ({agno_eval}, Mes {m_limit}) ===")
    
    # 1. Configuración Dinámica
    coords = config.get_meta_coordinates('Meta_6', agno_eval)
    COL = coords.get('col', 'H')
    ROW_NUM = coords.get('fila_num', 61)
    CELL_DEN = coords.get('celda_den', "H67")
    
    from config import DIR_SERIE_A_ACTUAL
    dir_a = DIR_SERIE_A_ACTUAL
    mapping = scan_rem_files(dir_a)

    numeradores = {}
    denominadores = {}
    
    for entry in mapping:
        code = entry['code'] # Already normalized
        file_path = entry['path']
        year = entry['year']
        
        # Filtro Año y Mes
        if year != agno_eval or entry['month'] > m_limit: continue

        
        if code not in numeradores:
            numeradores[code] = 0
            denominadores[code] = 0
            
        if not os.path.exists(file_path): continue
        
        # Usar la nueva función cacheada
        rows_needed = [61, 67]
        sheet_data = get_rem_sheet_data(file_path, "A03", rows_needed, [COL])
        
        # Num
        val_num = sheet_data.get((61, COL))
        if val_num is None or not isinstance(val_num, (int, float)):
            log_cuarentena_valor_invalido(file_path, "A03", f"{COL}61", val_num)
        else:
            numeradores[code] += val_num
            
        # Den
        val_den = sheet_data.get((67, COL))
        if val_den is None or not isinstance(val_den, (int, float)):
            log_cuarentena_valor_invalido(file_path, "A03", f"{COL}67", val_den)
        else:
            denominadores[code] += val_den
        
    # Reporte
    reporte = []
    all_centers = set(numeradores.keys()) | set(denominadores.keys())
    
    for c in all_centers:
        num = numeradores.get(c, 0)
        den = denominadores.get(c, 0)
        cump = (num/den*100) if den > 0 else 0
        # Reporte
        params = config.get_meta_params('Meta_6', agno_eval)
        reporte.append({
            'Centro': c, 'Meta_ID': 'Meta 6', 'Indicador': 'LME 6to Mes',
            'Numerador': num, 'Denominador': den, 'Cumplimiento': cump,
            'Meta_Fijada': params.get('fijada', 64.0), 'Meta_Nacional': params.get('nacional', 60.0)
        })
        
    return reporte

if __name__ == "__main__":
    import config
    res = calcular_meta_6(config.AGNO_ACTUAL, int(os.environ.get('MAX_MES', 12)))
    for r in res: print(r)
