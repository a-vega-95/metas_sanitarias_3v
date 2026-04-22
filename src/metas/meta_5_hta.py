import sys
import os
import csv
import openpyxl
import pyarrow.parquet as pq

# Add project root to path
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(os.path.dirname(current_dir))
sys.path.append(os.path.join(project_root, 'SRC'))

from modules.dataloaders import scan_rem_files, load_piv_for_year, get_rem_sheet_data, load_poblacion_a_cargo
from modules.utils import normalize_path, log_cuarentena_valor_invalido
import config

def calcular_meta_5(agno=None, max_mes=None):
    # Parameters
    agno_eval = int(agno) if agno else config.AGNO_ACTUAL
    m_limit = int(max_mes) if max_mes else int(os.environ.get('MAX_MES', 12))
    agno_piv = agno_eval - 1

    print(f"=== Calculando Meta 5: Hipertensión Arterial (HTA) ({agno_eval}, Mes {m_limit}) ===")
    
    # 3. Cargar prevalencias
    PREVALENCIA_HTA_15_24 = config.get_prevalencia('HTA', '15-24', agno_eval)
    PREVALENCIA_HTA_25_44 = config.get_prevalencia('HTA', '25-44', agno_eval)
    PREVALENCIA_HTA_45_64 = config.get_prevalencia('HTA', '45-64', agno_eval)
    PREVALENCIA_HTA_65_MAS = config.get_prevalencia('HTA', '65_mas', agno_eval)

    # 4. Configuración Coordenadas
    coords = config.get_meta_coordinates('Meta_5', agno_eval)
    SHEET = coords.get('hoja', 'P4')
    CELLS_NUM = coords.get('celdas_num', ["C28", "C29"])
    
    # 5. Cargar archivo PIV
    piv_data = load_piv_for_year(agno_piv)
    if not piv_data:
        print(f"[WARNING] No se encontraron datos PIV para {agno_piv}. Los denominadores serán 0.")
        piv_data = [] # Continuar con ceros

    dir_p = config.get_serie_p_corte(m_limit, agno_eval)
    if dir_p is None:
        print(f"[Meta 5] Sin corte Serie P disponible para m_limit={m_limit}. Se reportaran denominadores (PIV) con numeradores en 0.")
    mapping = scan_rem_files(dir_p) if dir_p else []


    # 1. Denominadores Estimados (PIV Estratificado)
    # Res. Exenta 650:
    # 15-24: 0.7%
    # 25-44: 10.6%
    # 45-64: 45.1%
    # 65+:   73.3%
    
    denominadores = {}
    
    for row in piv_data:
        centro = row.get('COD_CENTRO', '')
        edad = row.get('EDAD_EN_FECHA_CORTE')
        if edad is None: edad = -1
        estado = row.get('ACEPTADO_RECHAZADO', '')
        
        if estado == 'ACEPTADO':
            if centro not in denominadores:
                denominadores[centro] = 0
            
            factor = 0.0
            if 15 <= edad <= 24:
                factor = PREVALENCIA_HTA_15_24
            elif 25 <= edad <= 44:
                factor = PREVALENCIA_HTA_25_44
            elif 45 <= edad <= 64:
                factor = PREVALENCIA_HTA_45_64
            elif edad >= 65:
                factor = PREVALENCIA_HTA_65_MAS
                
            if factor > 0:
                denominadores[centro] += (1 * factor)
                
    # Redondear denominadores
    denominadores = {k: round(v) for k, v in denominadores.items()}
    
    # Aplicar Override Manual (Población a Cargo) si el denominador PIV es 0
    poblacion_manual = load_poblacion_a_cargo(agno_eval)
    centros_en_rem = set()
    for entry in mapping:
        c = entry['code']
        if c[-1].isalpha() and c[:-1].isdigit(): c = c[:-1]
        centros_en_rem.add(c)
        
    for c in (set(denominadores.keys()) | centros_en_rem):
        if denominadores.get(c, 0) == 0 and c in poblacion_manual and 'Meta_5' in poblacion_manual[c]:
            denominadores[c] = poblacion_manual[c]['Meta_5']
    
    # 2. Numeradores (REM)
    numeradores = {}
    
    for entry in mapping:
        # Normalize to base code
        raw_code = entry['code']
        # Try to strip trailing letters if numeric part exists
        real_code = raw_code
        if raw_code[-1].isalpha() and raw_code[:-1].isdigit():
             real_code = raw_code[:-1]
             
        file_path = entry['path']
        
        if real_code not in numeradores:
            numeradores[real_code] = 0
            
        if not os.path.exists(file_path): continue
        
        # Usar la nueva función cacheada
        rows_needed = sorted(list(set(int(c[1:]) for c in CELLS_NUM)))
        cols_needed = ["C"]
        
        sheet_data = get_rem_sheet_data(file_path, SHEET, rows_needed, cols_needed)
        
        for r_idx in rows_needed:
            val = sheet_data.get((r_idx, "C"))
            cell_ref = f"C{r_idx}"
            
            if val is None or not isinstance(val, (int, float)):
                log_cuarentena_valor_invalido(file_path, SHEET, cell_ref, val)
                continue
                
            numeradores[real_code] += val

    # Reporte
    all_centers = set(denominadores.keys()) | set(numeradores.keys())
    reporte = []
    
    total_num = 0
    total_den = 0
    
    for c in all_centers:
        n = numeradores.get(c, 0)
        d = denominadores.get(c, 0)
        cump = (n/d*100) if d > 0 else 0
        
        total_num += n
        total_den += d
        
        params = config.get_meta_params('Meta_5', cod_centro=c, agno=agno_eval)
        reporte.append({
            'Centro': c, 
            'Meta_ID': 'Meta 5',
            'Indicador': 'Cobertura HTA',
            'Numerador': n, 
            'Denominador': d, 
            'Cumplimiento': cump,
            'Meta_Fijada': params.get('fijada', 40.0),
            'Meta_Nacional': params.get('nacional', 45.0)
        })
        
    print("\n=== RESULTADOS GLOBALES META 5 (HTA) ===")
    print(f"Numerador: {total_num}")
    print(f"Denominador (Est. por Factores): {total_den}")
    if total_den > 0:
        print(f"Cumplimiento: {total_num/total_den*100:.2f}%")
    print("Meta Fijada: 40.0%")
        
    return reporte

if __name__ == "__main__":
    import config
    res = calcular_meta_5(config.AGNO_ACTUAL, int(os.environ.get('MAX_MES', 12)))
    for r in res: print(r)
