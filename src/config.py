import os
import json
from modules.dataloaders import get_exigencia_centro_csv

# =============================================================================
# POSTAS RURALES ADSCRITAS
# =============================================================================
PSR_PARENT_MAP = {
    '121460': '121352',  # COLLIMALLIN -> M. VALECH
    '121463': '121352',  # CONOCO      -> M. VALECH
}

# =============================================================================
# RUTAS BASE
# =============================================================================
if os.environ.get("METAS_BASE_DIR"):
    BASE_DIR = os.environ["METAS_BASE_DIR"]
else:
    BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

DATOS_DIR      = os.path.join(BASE_DIR, "data")
ENTRADA_DIR    = os.path.join(DATOS_DIR, "raw", "rem")
DICCIONARIOS_DIR = os.path.join(DATOS_DIR, "dictionaries")

# =============================================================================
# AÑO EN CURSO — CONFIGURACION ESTATICA 2026
# =============================================================================
AGNO_ACTUAL  = 2026
AGNO_ANTERIOR = 2025

# Rutas fijas que apuntan a las carpetas genericas ANNIO_CURSO / ANNIO_PASADO
DIR_REM_ACTUAL   = os.path.join(ENTRADA_DIR, "ANNIO_CURSO")
DIR_REM_ANTERIOR = os.path.join(ENTRADA_DIR, "ANNIO_PASADO")

DIR_SERIE_A_ACTUAL   = os.path.join(DIR_REM_ACTUAL,   "SERIE_A")
DIR_SERIE_A_ANTERIOR = os.path.join(DIR_REM_ANTERIOR,  "SERIE_A")

DIR_SERIE_P_ACTUAL   = os.path.join(DIR_REM_ACTUAL,   "SERIE_P")
DIR_SERIE_P_ANTERIOR = os.path.join(DIR_REM_ANTERIOR,  "SERIE_P")

# =============================================================================
# CORTE SERIE P (fotografico semestral)
# =============================================================================
def get_serie_p_corte(max_mes=12, agno_eval=None):
    """
    Retorna la ruta del corte semestral Serie P del MISMO anno de evaluacion.

    Regla de negocio:
      - Meses 1-5  : Sin corte disponible aun -> retorna None.
                     (El DEIS publica el primer corte en junio.)
      - Meses 6-11 : Usa el corte de Junio (06) de ANNIO_CURSO.
      - Mes 12     : Usa el corte de Diciembre (12) de ANNIO_CURSO.

    Se descarta la herencia del corte Dic(anno-1) para los meses Ene-May
    del anno siguiente, porque produciria numeradores 'fantasma'.
    """
    if max_mes < 6:
        return None

    target_month = 6 if max_mes < 12 else 12

    # Estructura limpia   -> SERIE_P/06 o SERIE_P/12
    carpeta = os.path.join(DIR_SERIE_P_ACTUAL, f"{target_month:02d}")

    if os.path.exists(carpeta) and os.listdir(carpeta):
        return carpeta

    return None

# =============================================================================
# CARGA DEL JSON DE PARAMETROS (bloqueado a 2026)
# =============================================================================
PARAMETROS_JSON_PATH = os.path.join(DICCIONARIOS_DIR, "parametros_metas.json")
PARAMETROS_ANUALES   = {}

if os.path.exists(PARAMETROS_JSON_PATH):
    try:
        with open(PARAMETROS_JSON_PATH, 'r', encoding='utf-8') as f:
            raw_data = json.load(f)
        PARAMETROS_ANUALES = {int(k): v for k, v in raw_data.items()}
    except Exception as e:
        print(f"[ERROR] Fallo la carga del JSON de parametros: {e}")
else:
    print(f"[WARNING] No se encontro el diccionario en: {PARAMETROS_JSON_PATH}")

# Referencia directa al bloque 2026 (acceso O(1))
_PARAMS_2026 = PARAMETROS_ANUALES.get(AGNO_ACTUAL, {})

# =============================================================================
# HELPERS DE REGLAS DE NEGOCIO
# =============================================================================
def get_meta_params(meta_id, cod_centro=None, agno=None):
    """
    Retorna los parametros de la meta. Si se envía cod_centro, 
    busca en los CSV para sobrescribir la meta fijada localmente.
    """
    # Obtener copia de los parametros base desde el JSON
    base_params = _PARAMS_2026.get('metas', {}).get(meta_id, {}).copy()

    # Si estamos evaluando un centro específico, buscar si tiene un trato especial en el CSV
    if cod_centro:
        exigencia_especifica = get_exigencia_centro_csv(meta_id, cod_centro)
        if exigencia_especifica is not None:
            base_params['fijada'] = exigencia_especifica  # Sobrescribe el porcentaje comunal

    return base_params

def get_prevalencia(key, subkey=None, agno=None):
    """Retorna la prevalencia de una enfermedad, opcionalmente filtrada por rango etario."""
    prevs = _PARAMS_2026.get('prevalencias', {}).get(key, {})
    if subkey and isinstance(prevs, dict):
        return prevs.get(subkey, 0)
    return prevs if not isinstance(prevs, dict) else 0

def get_meta_coordinates(meta_id, agno=None, mes=None):
    """Retorna las coordenadas de extraccion para una meta (siempre usa 'default')."""
    coords_meta = _PARAMS_2026.get('coordenadas', {}).get(meta_id, {})
    if mes is not None:
        mes_str = f"{mes:02d}"
        if mes_str in coords_meta:
            return coords_meta[mes_str]
    return coords_meta.get('default', {})

# Retrocompatibilidad
PREVALENCIA_DM2 = get_prevalencia('DM2')
