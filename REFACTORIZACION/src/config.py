import os
from datetime import datetime

# Postas Rurales adscritas
PSR_PARENT_MAP = {
    '121460': '121352',  # COLLIMALLIN -> M. VALECH
    '121463': '121352',  # CONOCO      -> M. VALECH
}

# Rutas Base
if os.environ.get("METAS_BASE_DIR"):
    BASE_DIR = os.environ["METAS_BASE_DIR"]
else:
    BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    
DATOS_DIR = os.path.join(BASE_DIR, "data")
ENTRADA_DIR = os.path.join(DATOS_DIR, "raw", "rem")

def get_year_from_rem_folder(serie_dir):
    if not os.path.exists(serie_dir):
        return None
    for item in os.listdir(serie_dir):
        if os.path.isdir(os.path.join(serie_dir, item)):
            tokens = item.replace('_', ' ').replace('-', ' ').split()
            for t in tokens:
                if t.isdigit() and len(t) == 4 and t.startswith('20'):
                    return int(t)
    return None

# Años de Evaluación
def get_available_years():
    """Escanea DATOS/ENTRADA buscando carpetas de años."""
    years = []
    if os.path.exists(ENTRADA_DIR):
        for item in os.listdir(ENTRADA_DIR):
            sub = os.path.join(ENTRADA_DIR, item)
            if not os.path.isdir(sub): continue
            
            # Buscar año de 4 dígitos en el nombre de la carpeta (ej: 2025 o REM_2025)
            import re
            m = re.search(r'20\d{2}', item)
            if m:
                years.append(int(m.group(0)))
    return sorted(list(set(years)))

def _detectar_agno_base():
    """Detecta el año actual basado en EV_AGNO o el año más reciente disponible en ENTRADA_DIR."""
    if os.environ.get("EV_AGNO"):
        return int(os.environ["EV_AGNO"])
        
    years = get_available_years()
    return years[-1] if years else 2026

AGNO_ACTUAL = _detectar_agno_base()
AGNO_ANTERIOR = AGNO_ACTUAL - 1

# Rutas Dinámicas basadas en Año
DIR_REM_ACTUAL = os.path.join(ENTRADA_DIR, str(AGNO_ACTUAL))
DIR_REM_ANTERIOR = os.path.join(ENTRADA_DIR, str(AGNO_ANTERIOR))

DIR_SERIE_A_ACTUAL = os.path.join(DIR_REM_ACTUAL, "SERIE_A")
DIR_SERIE_A_ANTERIOR = os.path.join(DIR_REM_ANTERIOR, "SERIE_A")

DIR_SERIE_P_ACTUAL = os.path.join(DIR_REM_ACTUAL, "SERIE_P")
DIR_SERIE_P_ANTERIOR = os.path.join(DIR_REM_ANTERIOR, "SERIE_P")

# PIV_FILE ya no se usa como constante única, se carga dinámicamente en los scripts de metas

def get_serie_p_corte(max_mes=12, agno_eval=None):
    """
    Retorna la ruta del corte semestral Serie P apropiado para max_mes.
    Meses 1-5: Usa corte Diciembre (12) del AGNO_ANTERIOR.
    Meses 6-11: Usa corte Junio (06) del AGNO_ACTUAL.
    Mes 12: Usa corte Diciembre (12) del AGNO_ACTUAL.
    """
    if agno_eval is None:
        agno_eval = AGNO_ACTUAL
    agno_ant = agno_eval - 1
    
    if max_mes < 6:
        # Ene a May: usar Dic del año pasado
        target_dir = os.path.join(ENTRADA_DIR, str(agno_ant), "SERIE_P")
        target_month = 12
        agno_ref = agno_ant
    elif max_mes < 12:
        # Jun a Nov: usar Jun del año actual
        target_dir = os.path.join(ENTRADA_DIR, str(agno_eval), "SERIE_P")
        target_month = 6
        agno_ref = agno_eval
    else:
        # Dic: usar Dic del año actual
        target_dir = os.path.join(ENTRADA_DIR, str(agno_eval), "SERIE_P")
        target_month = 12
        agno_ref = agno_eval

    if not target_dir:
        return None

    # Estructura limpia (SERIE_P/06)
    carpeta = os.path.join(target_dir, f"{target_month:02d}")
    
    # Estructura legacy (SERIE_P/REM_P_2026/06) - por tolerancia a versiones pasadas
    carpeta_alt = os.path.join(target_dir, f"REM_P_{agno_ref}", f"{target_month:02d}")

    if os.path.exists(carpeta):
        return carpeta
    if os.path.exists(carpeta_alt):
        return carpeta_alt
        
    return None

import json

# --- CARGA DINÁMICA DE DICCIONARIOS EXTERNOS ---
DICCIONARIOS_DIR = os.path.join(DATOS_DIR, "dictionaries")
PARAMETROS_JSON_PATH = os.path.join(DICCIONARIOS_DIR, "parametros_metas.json")

PARAMETROS_ANUALES = {}

if os.path.exists(PARAMETROS_JSON_PATH):
    try:
        with open(PARAMETROS_JSON_PATH, 'r', encoding='utf-8') as f:
            raw_data = json.load(f)
            # Convertimos las llaves a int para coincidir con AGNO_ACTUAL
            PARAMETROS_ANUALES = {int(k): v for k, v in raw_data.items()}
    except Exception as e:
        print(f"[ERROR] Falló la carga del JSON de parámetros: {e}")
else:
    print(f"[WARNING] No se encontró el diccionario en: {PARAMETROS_JSON_PATH}")

# --- AYUDANTES PARA VINCULAR REGLAS DE NEGOCIO ---
def get_meta_params(meta_id, agno=None):
    if agno is None: agno = AGNO_ACTUAL
    base = PARAMETROS_ANUALES.get(agno, PARAMETROS_ANUALES.get(2025, {}))
    return base.get('metas', {}).get(meta_id, {})

def get_prevalencia(key, subkey=None, agno=None):
    if agno is None: agno = AGNO_ACTUAL
    base = PARAMETROS_ANUALES.get(agno, PARAMETROS_ANUALES.get(2025, {}))
    prevs = base.get('prevalencias', {}).get(key, {})
    if subkey and isinstance(prevs, dict):
        return prevs.get(subkey, 0)
    if subkey and isinstance(prevs, dict):
        return prevs.get(subkey, 0)
    return prevs if not isinstance(prevs, dict) else 0

def get_meta_coordinates(meta_id, agno=None, mes=None):
    """
    Retorna las coordenadas de extracción para una meta, año y mes.
    Busca por mes específico, sino usa 'default'.
    """
    if agno is None: agno = AGNO_ACTUAL
    base = PARAMETROS_ANUALES.get(agno, PARAMETROS_ANUALES.get(2025, {}))
    coords_meta = base.get('coordenadas', {}).get(meta_id, {})
    
    # Intenta mes específico (ej: "01")
    if mes is not None:
        mes_str = f"{mes:02d}"
        if mes_str in coords_meta:
            return coords_meta[mes_str]
            
    # Fallback a default
    return coords_meta.get('default', {})

# RETROCOMPATIBILIDAD (Opcional pero recomendado para evitar romper scripts existentes)
PREVALENCIA_DM2 = get_prevalencia('DM2') # Si es un dict, devolverá el dict o 0 según se use
# Para DM2 en meta_4, se suele usar un valor global o estratificado. 
# Si el JSON tiene dict, los scripts deben adaptarse.
