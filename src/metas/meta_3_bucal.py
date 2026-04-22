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

def calcular_meta_3(agno=None, max_mes=None):
    # Parameters
    agno_eval = int(agno) if agno else config.AGNO_ACTUAL
    m_limit = int(max_mes) if max_mes else int(os.environ.get('MAX_MES', 12))
    agno_piv = agno_eval - 1

    print(f"=== Calculando Meta 3: Salud Bucal ({agno_eval}, Mes {m_limit}) ===")
    
    # 1. Configuración
    from config import DIR_SERIE_A_ACTUAL
    DATA_DIR_A = DIR_SERIE_A_ACTUAL
    
    # Meta 3A: CERO (0-9 años)
    coords_3a = config.get_meta_coordinates('Meta_3A', agno_eval)
    SHEET_3A = coords_3a.get('hoja', 'A03')
    CELL_RANGE_3A = (coords_3a.get('col_ini', 'F'), coords_3a.get('col_fin', 'Y'), sorted(list(coords_3a.get('filas', [206, 207]))))
    
    # Meta 3B: Libre de Caries (6 años)
    coords_3b = config.get_meta_coordinates('Meta_3B', agno_eval)
    SHEET_3B = coords_3b.get('hoja', 'A09')
    CELLS_3B = coords_3b.get('celdas', ["S51", "T51"])
    
    # Buscar archivo PIV (Corte Año Anterior)
    piv_data = load_piv_for_year(agno_piv)
    if not piv_data:
        print(f"[WARNING] No se encontraron datos PIV para {agno_piv}. Los denominadores serán 0.")
        piv_data = [] # Continuar con ceros

    mapping_a = scan_rem_files(DATA_DIR_A)
    # Filtrar por meses
    mapping_a = [f for f in mapping_a if f['year'] == agno_eval and f['month'] <= m_limit]
    print(f"Archivos REM A validos para meta 3: {len(mapping_a)}")


    # Inicializar diccionarios de numeradores y denominadores
    num_3a = {}
    num_3b = {}
    den_3a = {}  # PIV: Inscritos 0-9 años
    den_3b = {}  # PIV: Inscritos 6 años

    # Procesar Denominadores (PIV)
    for row in piv_data:
        centro = row.get('COD_CENTRO', '')
        edad = row.get('EDAD_EN_FECHA_CORTE')
        if edad is None: edad = -1
        estado = row.get('ACEPTADO_RECHAZADO', '')
        
        if estado != 'ACEPTADO':
            continue
            
        # 3A: 0 a 9 años
        if 0 <= edad <= 9:
            if centro not in den_3a:
                den_3a[centro] = 0
            den_3a[centro] += 1
            
        # 3B: 6 años
        if edad == 6:
            if centro not in den_3b:
                den_3b[centro] = 0
            den_3b[centro] += 1

    # Aplicar Override Manual (Población a Cargo) si el denominador PIV es 0
    poblacion_manual = load_poblacion_a_cargo(agno_eval)
    centros_en_rem = set()
    for entry in mapping_a:
        c = entry['code']
        if c[-1].isalpha() and c[:-1].isdigit(): c = c[:-1]
        centros_en_rem.add(c)
        
    for c in (set(den_3a.keys()) | set(den_3b.keys()) | centros_en_rem):
        if den_3a.get(c, 0) == 0 and c in poblacion_manual and 'Meta_3A' in poblacion_manual[c]:
            den_3a[c] = poblacion_manual[c]['Meta_3A']
        if den_3b.get(c, 0) == 0 and c in poblacion_manual and 'Meta_3B' in poblacion_manual[c]:
            den_3b[c] = poblacion_manual[c]['Meta_3B']

    # Procesar Numeradores (REM)
    for entry in mapping_a:
        code = entry['code']
        real_code = code
        if code[-1].isalpha() and code[:-1].isdigit():
            real_code = code[:-1]
        file_path = entry['path']
        year = entry['year']
        print(f"Procesando REM A: {file_path} (Centro: {real_code})")
        if year != agno_eval or entry['month'] > m_limit: continue
        if real_code not in num_3a:
            num_3a[real_code] = 0
            num_3b[real_code] = 0
        if not os.path.exists(file_path):
            print(f"Archivo no existe: {file_path}")
            continue
        from openpyxl.utils import get_column_letter, column_index_from_string
        
        # --- Meta 3A ---
        col_ini, col_fin, filas_3a = CELL_RANGE_3A
        cols_3a = [get_column_letter(i) for i in range(column_index_from_string(col_ini), column_index_from_string(col_fin) + 1)]
        sheet_data_3a = get_rem_sheet_data(file_path, SHEET_3A, filas_3a, cols_3a)
        
        for r in filas_3a:
            for c in cols_3a:
                v = sheet_data_3a.get((r, c))
                if v is None or not isinstance(v, (int, float)):
                    log_cuarentena_valor_invalido(file_path, SHEET_3A, f"{c}{r}", v)
                else:
                    num_3a[real_code] += v

        # --- Meta 3B ---
        filas_3b = [51]
        cols_3b = ["S", "T"]
        sheet_data_3b = get_rem_sheet_data(file_path, SHEET_3B, filas_3b, cols_3b)
        
        for c in cols_3b:
            v = sheet_data_3b.get((51, c))
            if v is None or not isinstance(v, (int, float)):
                log_cuarentena_valor_invalido(file_path, SHEET_3B, f"{c}51", v)
            else:
                num_3b[real_code] += v

    # Reporte
    all_centers = set(den_3a.keys()) | set(den_3b.keys()) | set(num_3a.keys())
    reporte = []
    
    for c in all_centers:
        # Meta 3A
        n_3a = num_3a.get(c, 0)
        d_3a = den_3a.get(c, 0)
        cump_3a = (n_3a / d_3a * 100) if d_3a > 0 else 0
        params_3a = config.get_meta_params('Meta_3A', cod_centro=c, agno=agno_eval)
        
        reporte.append({
            'Centro': c, 'Meta_ID': 'Meta 3A', 'Indicador': 'CERO (0-9a)',
            'Numerador': n_3a, 'Denominador': d_3a, 'Cumplimiento': cump_3a,
            'Meta_Fijada': params_3a.get('fijada', 48.0), 'Meta_Nacional': params_3a.get('nacional', 48.0)
        })
        
        # Meta 3B
        n_3b = num_3b.get(c, 0)
        d_3b = den_3b.get(c, 0)
        cump_3b = (n_3b / d_3b * 100) if d_3b > 0 else 0
        params_3b = config.get_meta_params('Meta_3B', cod_centro=c, agno=agno_eval)

        reporte.append({
            'Centro': c, 'Meta_ID': 'Meta 3B', 'Indicador': 'Lidre Caries (6a)',
            'Numerador': n_3b, 'Denominador': d_3b, 'Cumplimiento': cump_3b,
            'Meta_Fijada': params_3b.get('fijada', 21.0), 'Meta_Nacional': params_3b.get('nacional', 22.0)
        })
        
    return reporte

if __name__ == "__main__":
    import config
    res = calcular_meta_3(config.AGNO_ACTUAL, int(os.environ.get('MAX_MES', 12)))
    for r in res: print(r)
