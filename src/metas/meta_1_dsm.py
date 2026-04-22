import sys
import os
import csv
import openpyxl

# Absolute imports from src/
if __name__ == "__main__":
    current_dir = os.path.dirname(os.path.abspath(__file__))
    sys.path.append(os.path.dirname(current_dir)) # Add src to path


from modules.dataloaders import scan_rem_files, get_rem_value, get_rem_sheet_data
from modules.utils import normalize_path, log_cuarentena_valor_invalido
import config

def calcular_meta_1(agno=None, max_mes=None):
    # Use parameters or defaults/env
    agno_eval = int(agno) if agno else config.AGNO_ACTUAL
    m_limit = int(max_mes) if max_mes else int(os.environ.get('MAX_MES', 12))
    agno_prev = agno_eval - 1

    print(f"=== Calculando Meta 1: Recuperación DSM ({agno_eval}, Mes {m_limit}) ===")
    
    # 1. Configuración Dinámica
    coords = config.get_meta_coordinates('Meta_1', agno_eval)
    TARGET_SHEET = coords.get('hoja', 'A03')
    COLS = coords.get('columnas', ['J', 'K', 'L', 'M'])
    ROWS_NUM = coords.get('filas_num', [26, 28])
    ROWS_DEN = coords.get('filas_den', [23])
    
    # 2. Rutas estaticas (2026)
    from config import DIR_SERIE_A_ACTUAL, DIR_SERIE_A_ANTERIOR
    dir_a_actual = DIR_SERIE_A_ACTUAL
    dir_a_anterior = DIR_SERIE_A_ANTERIOR

    
    # 2. Cargar todos los REM Serie A disponibles (actual y anterior)
    mapping_actual = scan_rem_files(dir_a_actual)
    mapping_anterior = scan_rem_files(dir_a_anterior)
    mapping = mapping_actual + mapping_anterior
    print(f"Se encontraron {len(mapping)} archivos REM en total.")

    # 3. Filtrar archivos por año y mes
    # Numerador: todos los meses del año de evaluación hasta m_limit
    numerador_files = [entry for entry in mapping if entry['year'] == agno_eval and 1 <= entry['month'] <= m_limit]
    # Denominador: Oct-Dic del año anterior + Ene-Sep del año actual (limitados)
    denominador_files = [entry for entry in mapping if (
        (entry['year'] == agno_prev and entry['month'] >= 10) or
        (entry['year'] == agno_eval and entry['month'] <= min(9, m_limit))
    )]

    print(f"Archivos para numerador: {[f['filename'] for f in numerador_files]}")
    print(f"Archivos para denominador: {[f['filename'] for f in denominador_files]}")

    # Estructura para acumular por centro
    centros = {}

    # 4. Procesar archivos de forma eficiente (cargar cada archivo una sola vez)
    # Identificar todos los archivos únicos necesarios
    all_files_to_process = {} # {path: {'entry': entry, 'is_num': bool, 'is_den': bool}}
    
    for entry in numerador_files:
        path = entry['path']
        if path not in all_files_to_process:
            all_files_to_process[path] = {'entry': entry, 'is_num': True, 'is_den': False}
        else:
            all_files_to_process[path]['is_num'] = True
            
    for entry in denominador_files:
        path = entry['path']
        if path not in all_files_to_process:
            all_files_to_process[path] = {'entry': entry, 'is_num': False, 'is_den': True}
        else:
            all_files_to_process[path]['is_den'] = True

    print(f"Total de archivos únicos a procesar: {len(all_files_to_process)}")

    for file_path, info in all_files_to_process.items():
        entry = info['entry']
        code = entry['code']
        if code not in centros:
            centros[code] = {'num': 0, 'den': 0}
            
        # Usar la nueva función cacheada
        rows_needed = sorted(list(set(ROWS_NUM) | set(ROWS_DEN)))
        sheet_data = get_rem_sheet_data(file_path, TARGET_SHEET, rows_needed, COLS)
        
        for (r_idx, col_letter), val in sheet_data.items():
            cell_ref = f"{col_letter}{r_idx}"
            
            # Validar Nulo o Texto
            if val is None or not isinstance(val, (int, float)):
                # Solo loguear si no estaba ya en cache (para no ensuciar logs redundantes)
                # (get_rem_sheet_data ya lo leyó, pero el log de cuarentena vive en utils)
                from modules.utils import log_cuarentena_valor_invalido
                log_cuarentena_valor_invalido(file_path, TARGET_SHEET, cell_ref, val)
                continue
                
            # If valid, sum
            if info['is_num'] and r_idx in ROWS_NUM:
                centros[code]['num'] += val
            if info['is_den'] and r_idx in ROWS_DEN:
                centros[code]['den'] += val

    reporte = []
    
    # Generar reporte final
    total_num = 0
    total_den = 0
    
    for code, data in centros.items():
        num = data['num']
        den = data['den']
        total_num += num
        total_den += den
        
        cumplimiento = (num / den * 100) if den > 0 else 0
        
        params = config.get_meta_params('Meta_1', agno_eval)
        reporte.append({
            'Centro': code, 'Meta_ID': 'Meta 1', 'Indicador': 'Recuperación DSM',
            'Numerador': num, 'Denominador': den, 'Cumplimiento': cumplimiento,
            'Meta_Fijada': params.get('fijada', 90.0), 'Meta_Nacional': params.get('nacional', 90.0)
        })

    # 4. Resultado Final Global
    cumplimiento_global = (total_num / total_den * 100) if total_den > 0 else 0
    
    print("\n=== RESULTADOS GLOBALES META 1 ===")
    print(f"Numerador Total (Recuperados): {total_num}")
    print(f"Denominador Total (Riesgo): {total_den}")
    print(f"Cumplimiento Actual: {cumplimiento_global:.2f}%")
    print(f"Meta Fijada: 90.0%")
    
    # Guardar reporte
    return reporte

if __name__ == "__main__":
    import config
    res = calcular_meta_1(config.AGNO_ACTUAL, int(os.environ.get('MAX_MES', 12)))
    for r in res: print(r)
