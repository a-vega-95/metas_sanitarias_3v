import sys
import os
import csv
import openpyxl
import pyarrow.parquet as pq

# Add project root to path
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(os.path.dirname(current_dir))
sys.path.append(os.path.join(project_root, 'src'))

from modules.dataloaders import scan_rem_files, load_piv_for_year, get_rem_sheet_data, load_poblacion_a_cargo
from modules.utils import normalize_path, log_cuarentena_valor_invalido
import config

def calcular_meta_4(agno=None, max_mes=None):
    # Parameters
    agno_eval = int(agno) if agno else config.AGNO_ACTUAL
    m_limit = int(max_mes) if max_mes else int(os.environ.get('MAX_MES', 12))
    agno_piv = agno_eval - 1

    print(f"=== Calculando Meta 4: Diabetes Mellitus Tipo 2 (DM2) ({agno_eval}, Mes {m_limit}) ===")
    
    # Configuración
    DATA_DIR = config.get_serie_p_corte(m_limit, agno_eval)
    if DATA_DIR is None:
        print(f"[Meta 4] Sin corte Serie P disponible para m_limit={m_limit}. Se reportaran denominadores (PIV) con numeradores en 0.")
    
    # 3. Cargar parámetros y prevalencias
    params_4a = config.get_meta_params('Meta_4A', agno_eval)
    META_FIJADA_4A = params_4a.get('fijada', 29.0)
    META_NACIONAL_4A = params_4a.get('nacional', 29.0)
    
    PREV_DM2_15_24 = config.get_prevalencia('DM2', '15-24', agno_eval)
    PREV_DM2_25_44 = config.get_prevalencia('DM2', '25-44', agno_eval)
    PREV_DM2_45_64 = config.get_prevalencia('DM2', '45-64', agno_eval)
    PREV_DM2_65_MAS = config.get_prevalencia('DM2', '65_mas', agno_eval)
    
    # 4. Configuración Dinámica Coordenadas
    coords_4a = config.get_meta_coordinates('Meta_4A', agno_eval)
    coords_4b = config.get_meta_coordinates('Meta_4B', agno_eval)
    
    SHEET = coords_4a.get('hoja', 'P4')
    CELLS_4A_NUM = coords_4a.get('celdas_num', ["C30", "C31"])
    CELLS_4B_NUM = coords_4b.get('celdas_num', ["C61", "C62", "C63", "C64"])
    CELLS_4B_DEN = coords_4b.get('celdas_den', ["C17"])
    
    # 5. Cargar archivo PIV
    piv_data = load_piv_for_year(agno_piv)
    if not piv_data:
        print(f"[WARNING] No se encontraron datos PIV para {agno_piv}. Los denominadores serán 0.")
        piv_data = [] # Continuar con ceros

    mapping = scan_rem_files(DATA_DIR) if DATA_DIR else []


    denominadores_4a_acum = {}
    
    for row in piv_data:
        centro = row.get('COD_CENTRO', '')
        edad = row.get('EDAD_EN_FECHA_CORTE')
        if edad is None: edad = -1
        estado = row.get('ACEPTADO_RECHAZADO', '')
        
        if estado == 'ACEPTADO':
            if centro not in denominadores_4a_acum:
                denominadores_4a_acum[centro] = 0.0
            
            factor = 0.0
            if 15 <= edad <= 24:
                factor = PREV_DM2_15_24
            elif 25 <= edad <= 44:
                factor = PREV_DM2_25_44
            elif 45 <= edad <= 64:
                factor = PREV_DM2_45_64
            elif edad >= 65:
                factor = PREV_DM2_65_MAS
                
            if factor > 0:
                denominadores_4a_acum[centro] += (1 * factor)
                
    denominadores_4a = {k: round(v) for k, v in denominadores_4a_acum.items()}
    
    # Aplicar Override Manual (Población a Cargo) si el denominador PIV es 0
    poblacion_manual = load_poblacion_a_cargo(agno_eval)
    centros_en_rem = set()
    for entry in mapping:
        c = entry['code']
        if c[-1].isalpha() and c[:-1].isdigit(): c = c[:-1]
        centros_en_rem.add(c)
        
    for c in (set(denominadores_4a.keys()) | centros_en_rem):
        if denominadores_4a.get(c, 0) == 0 and c in poblacion_manual and 'Meta_4A' in poblacion_manual[c]:
            denominadores_4a[c] = poblacion_manual[c]['Meta_4A']
    
    # 2. Numeradores y Denominadores REM
    numeradores_4a = {}
    numeradores_4b = {}
    denominadores_4b = {}
    
    for entry in mapping:
        raw_code = entry['code']
        real_code = raw_code
        if raw_code[-1].isalpha() and raw_code[:-1].isdigit():
             real_code = raw_code[:-1]
             
        file_path = entry['path']
        
        if real_code not in numeradores_4a:
            numeradores_4a[real_code] = 0
            numeradores_4b[real_code] = 0
            denominadores_4b[real_code] = 0
            
        if not os.path.exists(file_path): continue
        
        # Usar la nueva función cacheada
        all_cells = sorted(list(set(CELLS_4A_NUM) | set(CELLS_4B_NUM) | set(CELLS_4B_DEN)))
        rows_needed = sorted(list(set(int(c[1:]) for c in all_cells)))
        cols_needed = sorted(list(set(c[0] for c in all_cells)))
        
        sheet_data = get_rem_sheet_data(file_path, SHEET, rows_needed, cols_needed)
        
        for r_idx in rows_needed:
            for c_letter in cols_needed:
                cell_ref = f"{c_letter}{r_idx}"
                val = sheet_data.get((r_idx, c_letter))
                
                if val is None or not isinstance(val, (int, float)):
                    if cell_ref in all_cells:
                        log_cuarentena_valor_invalido(file_path, SHEET, cell_ref, val)
                    continue
                    
                if cell_ref in CELLS_4A_NUM: numeradores_4a[real_code] += val
                if cell_ref in CELLS_4B_NUM: numeradores_4b[real_code] += val
                if cell_ref in CELLS_4B_DEN: denominadores_4b[real_code] += val

            
    # Reporte
    all_centers = set(denominadores_4a.keys()) | set(numeradores_4a.keys())
    reporte = []
    
    for c in all_centers:
        # Meta 4A
        num_4a = numeradores_4a.get(c, 0)
        den_4a = denominadores_4a.get(c, 0)
        cump_4a = (num_4a/den_4a*100) if den_4a > 0 else 0
        
        params_4a = config.get_meta_params('Meta_4A', cod_centro=c, agno=agno_eval)
        reporte.append({
            'Centro': c, 'Meta_ID': 'Meta 4A', 'Indicador': 'Compensación DM2',
            'Numerador': num_4a, 'Denominador': den_4a, 'Cumplimiento': cump_4a,
            'Meta_Fijada': params_4a.get('fijada', 29.0), 'Meta_Nacional': params_4a.get('nacional', 29.0)
        })
        
        # Meta 4B
        num_4b = numeradores_4b.get(c, 0)
        den_4b = denominadores_4b.get(c, 0)
        cump_4b = (num_4b/den_4b*100) if den_4b > 0 else 0
        
        params_4b = config.get_meta_params('Meta_4B', cod_centro=c, agno=agno_eval)
        reporte.append({
            'Centro': c, 'Meta_ID': 'Meta 4B', 'Indicador': 'Pie Diabético',
            'Numerador': num_4b, 'Denominador': den_4b, 'Cumplimiento': cump_4b,
            'Meta_Fijada': params_4b.get('fijada', 90.0), 'Meta_Nacional': params_4b.get('nacional', 90.0)
        })
    
    return reporte

if __name__ == "__main__":
    import config
    res = calcular_meta_4(config.AGNO_ACTUAL, int(os.environ.get('MAX_MES', 12)))
    for r in res: print(r)
