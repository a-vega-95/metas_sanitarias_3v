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

def calcular_meta_2(agno=None, max_mes=None):
    # Parameters
    agno_eval = int(agno) if agno else config.AGNO_ACTUAL
    m_limit = int(max_mes) if max_mes else int(os.environ.get('MAX_MES', 12))
    agno_piv = agno_eval - 1

    print(f"=== Calculando Meta 2: Papanicolaou (PAP) o Test VPH ({agno_eval}, Mes {m_limit}) ===")
    
    # 1. Configuración
    DATA_DIR = config.get_serie_p_corte(m_limit, agno_eval)
    if DATA_DIR is None:
        print(f"[Meta 2] Sin corte Serie P disponible para m_limit={m_limit}. Se reportaran denominadores (PIV) con numeradores en 0.")
    
    
    # 1. Configuración Dinámica
    coords = config.get_meta_coordinates('Meta_2', agno_eval)
    SHEET_P12 = coords.get('hoja', 'P12')
    COLS_REM = coords.get('columnas', ['B', 'C'])
    ROWS_REM = list(coords.get('filas', range(12, 20))) # Convert range to list for list operations
    
    # 2. Cargar Datos
    piv_data = load_piv_for_year(agno_piv)
    if not piv_data:
        print(f"[WARNING] No se encontraron datos PIV para {agno_piv}. Los denominadores serán 0.")
        piv_data = [] # Continuar con ceros

    mapping = scan_rem_files(DATA_DIR) if DATA_DIR else []
    print(f"Cargados {len(mapping)} archivos REM P y {len(piv_data)} registros PIV.")


    # 3. Procesar Denominadores (PIV)
    denominadores = {} # {cod_centro: count}
    
    for row in piv_data:
        centro = row.get('COD_CENTRO', '')
        edad = row.get('EDAD_EN_FECHA_CORTE')
        if edad is None: edad = -1
        estado = row.get('ACEPTADO_RECHAZADO', '')
        genero = row.get('GENERO', '') 
        
        if estado != 'ACEPTADO':
            continue
            
        if 25 <= edad <= 64:
            # Filter Logic: "Personas". User notes say "Test VPH o PAP vigente en personas...".
            # Usually strict filter for women (MUJER) or inclusive. 
            # I will filter by Female or check Genero Normalized to be safe as previously decided.
            if 'MUJER' in str(genero).upper() or 'FEMENINO' in str(row.get('GENERO_NORMALIZADO', '')).upper():
                if centro not in denominadores:
                    denominadores[centro] = 0
                denominadores[centro] += 1

    # 3.5 Aplicar Override Manual (Población a Cargo) si el denominador PIV es 0
    poblacion_manual = load_poblacion_a_cargo(agno_eval)
    
    # Para asegurar que evaluamos centros que solo están en el REM y no en PIV:
    centros_en_rem = set()
    for entry in mapping:
        c = entry['code']
        if c[-1].isalpha() and c[:-1].isdigit(): c = c[:-1]
        centros_en_rem.add(c)
        
    for c in (set(denominadores.keys()) | centros_en_rem):
        if denominadores.get(c, 0) == 0:
            if c in poblacion_manual and 'Meta_2' in poblacion_manual[c]:
                denominadores[c] = poblacion_manual[c]['Meta_2']

    # 4. Procesar Numeradores (REM P12)
    numeradores = {} # {cod_centro: 0}
    
    for entry in mapping:
        raw_code = entry['code']
        real_code = raw_code
        if raw_code[-1].isalpha() and raw_code[:-1].isdigit():
             real_code = raw_code[:-1]
        file_path = entry['path']
        print(f"Procesando REM P: {file_path} (Centro: {real_code})")
        if real_code not in numeradores:
            numeradores[real_code] = 0
        # Usar la nueva función cacheada
        sheet_data = get_rem_sheet_data(file_path, SHEET_P12, ROWS_REM, COLS_REM)
        
        for r_idx in ROWS_REM:
            for c_letter in COLS_REM:
                val = sheet_data.get((r_idx, c_letter))
                cell_ref = f"{c_letter}{r_idx}"
                
                if val is None or not isinstance(val, (int, float)):
                    log_cuarentena_valor_invalido(file_path, SHEET_P12, cell_ref, val)
                    continue
                
                numeradores[real_code] += val

    # 5. Generar Reporte
    all_centers = set(denominadores.keys()) | set(numeradores.keys())
    reporte = []
    
    total_num = 0
    total_den = 0
    
    for code in all_centers:
        den = denominadores.get(code, 0)
        num = numeradores.get(code, 0)
        
        cumplimiento = (num / den * 100) if den > 0 else 0
        
        total_num += num
        total_den += den
        
        params = config.get_meta_params('Meta_2', cod_centro=code, agno=agno_eval)
        reporte.append({
            'Centro': code, 'Meta_ID': 'Meta 2', 'Indicador': 'PAP/VPH',
            'Numerador': num, 'Denominador': den, 'Cumplimiento': cumplimiento,
            'Meta_Fijada': params.get('fijada', 63.0), 'Meta_Nacional': params.get('nacional', 80.0)
        })

    cumplimiento_global = (total_num / total_den * 100) if total_den > 0 else 0
    
    print("\n=== RESULTADOS GLOBALES META 2 (PAP/VPH) ===")
    print(f"Numerador Total: {total_num}")
    print(f"Denominador Total (Mujeres 25-64): {total_den}")
    print(f"Cumplimiento Actual: {cumplimiento_global:.2f}%")
    print(f"Meta Fijada: 63.0%")
    
    return reporte

if __name__ == "__main__":
    import config
    res = calcular_meta_2(config.AGNO_ACTUAL, int(os.environ.get('MAX_MES', 12)))
    for r in res: print(r)
