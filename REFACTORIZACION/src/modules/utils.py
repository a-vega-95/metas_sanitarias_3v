import os
import sys
import csv
import logging
from datetime import datetime

def get_project_root():
    """Returns the root directory of the project."""
    if os.environ.get("METAS_BASE_DIR"):
        return os.environ["METAS_BASE_DIR"]
    # Assuming this file is in SRC/modules/
    return os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

def normalize_path(path):
    """Normalizes a path to be absolute and use correct separators."""
    if not os.path.isabs(path):
        path = os.path.join(get_project_root(), path)
    return os.path.normpath(path)

_SESSION_TIMESTAMP = None

def setup_audit_logger():
    """Configures and returns the Main Audit Logger (MET_SANIT). Use for network summaries."""
    global _SESSION_TIMESTAMP
    if _SESSION_TIMESTAMP is None:
        _SESSION_TIMESTAMP = datetime.now().strftime("%Y%m%d_%H%M%S")
        
    log_dir = normalize_path("logs/auditoria")
    if not os.path.exists(log_dir):
        os.makedirs(log_dir)
        
    log_file = os.path.join(log_dir, f"MET_SANIT_{_SESSION_TIMESTAMP}.LOG")
    
    logger = logging.getLogger("audit_logger")
    logger.setLevel(logging.INFO)
    
    if not logger.handlers:
        file_handler = logging.FileHandler(log_file, encoding='utf-8')
        console_handler = logging.StreamHandler(sys.stdout)
        
        formatter = logging.Formatter('[%(asctime)s] %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
        file_handler.setFormatter(formatter)
        console_handler.setFormatter(formatter)
        
        logger.addHandler(file_handler)
        logger.addHandler(console_handler)
        
    return logger



def load_center_names():
    """Carga los nombres de los centros desde data/dictionaries/COD_CENTROS_SALUD.CSV"""
    mapping_names = {}
    csv_path = normalize_path("data/dictionaries/COD_CENTROS_SALUD.CSV")
    
    if os.path.exists(csv_path):
        try:
            with open(csv_path, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    code = row['COD_CENTRO'].strip()
                    name = row['NOMBRE'].strip()
                    mapping_names[code] = name
                    if code[-1].isalpha():
                        mapping_names[code[:-1]] = name
        except Exception as e:
            print(f"Error cargando nombres de centros: {e}")
    return mapping_names

# ── Logger de Cuarentena ─────────────────────────────────────────────────────

def setup_quarantine_logger():
    """Configura y retorna el logger de cuarentena. Escribe en logs/cuarentena."""
    global _SESSION_TIMESTAMP
    if _SESSION_TIMESTAMP is None:
        _SESSION_TIMESTAMP = datetime.now().strftime("%Y%m%d_%H%M%S")
        
    cuarentena_dir = normalize_path("logs/cuarentena")
    if not os.path.exists(cuarentena_dir):
        os.makedirs(cuarentena_dir)

    log_file = os.path.join(cuarentena_dir, f"CUARENTENA_{_SESSION_TIMESTAMP}.LOG")

    logger = logging.getLogger("quarantine_logger")
    logger.setLevel(logging.WARNING)

    if not logger.handlers:
        fh = logging.FileHandler(log_file, encoding='utf-8')
        fmt = logging.Formatter('[%(asctime)s] %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
        fh.setFormatter(fmt)
        logger.addHandler(fh)

    return logger


def log_cuarentena_denegado(ruta_archivo, codigo_detectado, motivo="Codigo no autorizado"):
    """Archivos Denegados: [DENEGADO] | Codigo: X | Motivo: Y | Ruta: Z"""
    logger = setup_quarantine_logger()
    logger.warning(f"[DENEGADO] | Codigo: {codigo_detectado} | Motivo: {motivo} | Ruta: {ruta_archivo}")



def log_cuarentena_valor_invalido(ruta_archivo, hoja, celda, valor):
    """Valores Invalidos: [NULO] o [TEXTO] | Hoja: X | Celda: Y | Valor: Z | Ruta: W"""
    logger = setup_quarantine_logger()
    tipo = "[NULO]" if valor is None else "[TEXTO]"
    val_str = "None" if valor is None else str(valor)
    logger.warning(f"{tipo} | Hoja: {hoja} | Celda: {celda} | Valor: {val_str} | Ruta: {ruta_archivo}")

