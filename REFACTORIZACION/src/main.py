import sys
import os
import csv
import subprocess
import pyarrow as pa
import pyarrow.parquet as pq
import pyarrow.compute as pc
from datetime import datetime

# Add project root to path
current_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.append(current_dir) # Add src to path


from modules.utils import normalize_path, load_center_names, setup_audit_logger
from config import PSR_PARENT_MAP, AGNO_ACTUAL

# Importar funciones de cálculo de metas
from metas.meta_1_dsm import calcular_meta_1
from metas.meta_2_pap import calcular_meta_2
from metas.meta_3_bucal import calcular_meta_3
from metas.meta_4_dm2 import calcular_meta_4
from metas.meta_5_hta import calcular_meta_5
from metas.meta_6_lactancia import calcular_meta_6
from metas.meta_7_resp import calcular_meta_7

def run_meta_scripts(agno=None, mes=None):
    """Ejecuta todos los cálculos de metas de forma SECUENCIAL e IMPORTADA, retornando métricas consolidadas."""
    logger = setup_audit_logger()
    logger.info(f"[{datetime.now().strftime('%H:%M:%S')}] Iniciando lote de calculos para {agno}-{mes}...")
    
    consolidado = []
    
    res1 = calcular_meta_1(agno, mes)
    res2 = calcular_meta_2(agno, mes)
    res3 = calcular_meta_3(agno, mes)
    res4 = calcular_meta_4(agno, mes)
    res5 = calcular_meta_5(agno, mes)
    res6 = calcular_meta_6(agno, mes)
    res7 = calcular_meta_7(agno, mes)
    
    if res1: consolidado.extend(res1)
    if res2: consolidado.extend(res2)
    if res3: consolidado.extend(res3)
    if res4: consolidado.extend(res4)
    if res5: consolidado.extend(res5)
    if res6: consolidado.extend(res6)
    if res7: consolidado.extend(res7)
    
    logger.info(f"[{datetime.now().strftime('%H:%M:%S')}] Lote de calculos finalizado.")
    
    return consolidado

def generar_resumen_auditoria(consolidado, periodo_str, map_nombres):
    """Genera el resumen tabular final por Meta con Totales de Red en el Log de Auditoria."""
    logger = setup_audit_logger()
    
    logger.info("\n" + "="*80)
    logger.info(f"=== RESUMEN EXTRACCION METAS | PERIODO: {periodo_str} ===")
    logger.info("="*80)
    
    metas_data = {}
    for row in consolidado:
        m_id = row.get('Meta_ID', 'Desconocida')
        if m_id not in metas_data: metas_data[m_id] = []
        metas_data[m_id].append(row)
        
    for m_id, rows in sorted(metas_data.items()):
        logger.info(f"\n--- {m_id.upper()} ---")
        logger.info(f"{'Codigo':<8} | {'Nombre Centro':<35} | {'Num':<8} | {'Den':<8} | {'% Cump':<8}")
        logger.info("-" * 75)
        
        tot_num = 0.0
        tot_den = 0.0
        
        for r in sorted(rows, key=lambda x: str(x.get('COD_CENTRO',''))):
            c = r.get('COD_CENTRO', '')
            nombre = map_nombres.get(c, map_nombres.get(f"{c}A", "Desconocido"))[:35]
            num = float(r.get('Numerador_Actual', 0) or 0)
            den = float(r.get('Denominador_Actual', 0) or 0)
            cump = (num / den * 100.0) if den > 0 else 0.0
            
            tot_num += num
            tot_den += den
            
            logger.info(f"{c:<8} | {nombre:<35} | {num:<8.1f} | {den:<8.1f} | {cump:>6.1f}%")
            
        logger.info("-" * 75)
        tot_cump = (tot_num / tot_den * 100.0) if tot_den > 0 else 0.0
        logger.info(f"{'RED':<8} | {'TOTAL CONSOLIDADO':<35} | {tot_num:<8.1f} | {tot_den:<8.1f} | {tot_cump:>6.1f}%")
    
    logger.info("="*80 + "\n")



def _formatear_consolidado(datos_crudos, map_nombres):
    """Filtra y normaliza la lista en memoria (reemplazo del lector de CSVs)."""
    codigos_autorizados = set()
    for cod in map_nombres.keys():
        base = cod[:-1] if cod and cod[-1].isalpha() and cod[:-1].isdigit() else cod
        codigos_autorizados.add(base)

    consolidado = []
    for row in datos_crudos:
        centro = row.get('Centro', '')
        base_centro = centro[:-1] if centro and centro[-1].isalpha() and centro[:-1].isdigit() else centro
        
        if base_centro not in codigos_autorizados:
            continue
            
        try:
            num = float(row.get('Numerador', 0))
            den = float(row.get('Denominador', 0))
        except (ValueError, TypeError):
            continue
            
        consolidado.append({
            'Meta_ID': row.get('Meta_ID', ''),
            'COD_CENTRO': base_centro,
            'Numerador_Actual': num,
            'Denominador_Actual': den
        })
    return consolidado

def _aplicar_psr(consolidado):
    """Absorbe las Postas Rurales en su Centro Padre."""
    filas_psr = [r for r in consolidado if r['COD_CENTRO'] in PSR_PARENT_MAP]
    filas_normales = [r for r in consolidado if r['COD_CENTRO'] not in PSR_PARENT_MAP]

    idx_padre = {}
    for i, r in enumerate(filas_normales):
        idx_padre[(r['Meta_ID'], r['COD_CENTRO'])] = i

    for psr_row in filas_psr:
        padre_cod = PSR_PARENT_MAP[psr_row['COD_CENTRO']]
        key = (psr_row['Meta_ID'], padre_cod)
        if key in idx_padre:
            p = filas_normales[idx_padre[key]]
            p['Numerador_Actual'] += psr_row['Numerador_Actual']
            p['Denominador_Actual'] += psr_row['Denominador_Actual']

    return filas_normales

def _obtener_meta_id_bi(meta_raw):
    """Normaliza IDs para Parquet."""
    m = meta_raw.upper().replace('META', '').replace('_', '').replace(' ', '').strip()
    mapping = {
        '1': 'M_01', '2': 'M_02', '3A': 'M_03A', '3B': 'M_03B',
        '4A': 'M_04A', '4B': 'M_04B', '5': 'M_05', '6': 'M_06',
        '7': 'M_07', '8': 'M_08'
    }
    return mapping.get(m, f"M_{m.zfill(2)}" if m.isdigit() else m)

def exportar_dimensiones_bi(bi_dir, map_nombres=None):
    """Genera DIM_Metas, DIM_Establecimientos y DIM_Calendario."""
    from config import PARAMETROS_ANUALES
    
    os.makedirs(bi_dir, exist_ok=True)
    os.makedirs(normalize_path("data/processed/temp"), exist_ok=True)
    
    if not map_nombres:
        map_nombres = load_center_names()

    # DIM_METAS
    metas_data = PARAMETROS_ANUALES.get(AGNO_ACTUAL, PARAMETROS_ANUALES.get(2025, {})).get('metas', {})
    filas_dim_metas = []
    for mid_raw in ['Meta_1', 'Meta_2', 'Meta_3A', 'Meta_3B', 'Meta_4A', 'Meta_4B', 'Meta_5', 'Meta_6', 'Meta_7', 'Meta_8']:
        m_params = metas_data.get(mid_raw, {})
        mid_bi = _obtener_meta_id_bi(mid_raw)
        filas_dim_metas.append({
            'Meta_ID': mid_bi,
            'Nombre_Meta': mid_raw.replace('_', ' '),
            'Peso_Relativo': float(m_params.get('peso', 0)) / 100.0,
            'Meta_Nacional': float(m_params.get('nacional', 0)) / 100.0,
            'Meta_Fijada_Temuco': float(m_params.get('fijada', 0)) / 100.0
        })
    pq.write_table(pa.Table.from_pylist(filas_dim_metas), os.path.join(bi_dir, "DIM_Metas.parquet"))

    # DIM_ESTABLECIMIENTOS
    filas_dim_est = []
    csv_path = normalize_path("data/dictionaries/COD_CENTROS_SALUD.CSV")
    if os.path.exists(csv_path):
        with open(csv_path, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                cod = row['COD_CENTRO'].strip()
                if cod.endswith('A'): cod = cod[:-1]
                filas_dim_est.append({
                    'Establecimiento_ID': cod,
                    'Nombre_Centro': row['NOMBRE'].strip(),
                    'Tipo_Centro': row['TIPO_CENTRO'].strip(),
                    'Comuna': 'Temuco'
                })
    visto = set()
    uniq = []
    for f in filas_dim_est:
        if f['Establecimiento_ID'] not in visto:
            uniq.append(f)
            visto.add(f['Establecimiento_ID'])
    pq.write_table(pa.Table.from_pylist(uniq), os.path.join(bi_dir, "DIM_Establecimientos.parquet"))

    # DIM_CALENDARIO
    from config import get_available_years
    years = get_available_years()
    if not years: years = [2025, 2026]
    
    meses_nombres = ["", "Enero", "Febrero", "Marzo", "Abril", "Mayo", "Junio", "Julio", "Agosto", "Septiembre", "Octubre", "Noviembre", "Diciembre"]
    filas_cal = []
    from datetime import date
    for agno in years:
        for mes in range(1, 13):
            pid = agno * 100 + mes
            filas_cal.append({
                'Periodo_ID': pid,
                'Año': agno,
                'Mes_Numero': mes,
                'Mes_Nombre': meses_nombres[mes],
                'Semestre': 'S1' if mes <= 6 else 'S2',
                'Fecha': date(agno, mes, 1)
            })
    pq.write_table(pa.Table.from_pylist(filas_cal), os.path.join(bi_dir, "DIM_Calendario.parquet"))
    print("[BI] Dimensiones generadas en data/processed/bi/")

def _guardar_snapshot(consolidado, periodo_str):
    """Guarda en FCT_Produccion evitando duplicados del periodo."""
    bi_dir = normalize_path("data/processed/bi")
    os.makedirs(bi_dir, exist_ok=True)
    
    periodo_id = int(periodo_str.replace('-', ''))
    filas = []
    for row in consolidado:
        filas.append({
            'Periodo_ID': periodo_id,
            'Establecimiento_ID': row.get('COD_CENTRO', ''),
            'Meta_ID': _obtener_meta_id_bi(row.get('Meta_ID', '')),
            'Numerador': float(row.get('Numerador_Actual', 0) or 0),
            'Denominador': float(row.get('Denominador_Actual', 0) or 0)
        })

    if not filas:
        print(f"[BI] Sin datos que guardar para snapshot {periodo_str}.")
        return

    path_fct = os.path.join(bi_dir, "FCT_Produccion.parquet")
    tabla_nueva = pa.Table.from_pylist(filas)
    
    if os.path.exists(path_fct):
        existente = pq.read_table(path_fct)
        # Limpia duplicados en caso de re-corrida
        existente = existente.filter(pc.not_equal(existente.column('Periodo_ID'), periodo_id))
        tabla_hist = pa.concat_tables([existente, tabla_nueva])
    else:
        tabla_hist = tabla_nueva

    pq.write_table(tabla_hist, path_fct, compression='snappy')
    print(f"[BI] Hechos (FCT_Produccion) actualizados para periodo: {periodo_str} ({len(tabla_nueva)} filas generadas)")

def detectar_periodos_disponibles():
    """Escanea dinámicamente data/raw/rem buscando (Año, Mes)."""
    from config import ENTRADA_DIR, get_available_years
    periodos = []
    for y in get_available_years():
        for item in os.listdir(ENTRADA_DIR):
            if str(y) in item:
                serie_a = os.path.join(ENTRADA_DIR, item, "SERIE_A")
                if os.path.isdir(serie_a):
                    for sub in os.listdir(serie_a):
                        if sub.isdigit() and len(sub) == 2:
                            periodos.append((y, int(sub)))
    return sorted(list(set(periodos)))

def leer_periodos_existentes():
    path_fct = normalize_path("data/processed/bi/FCT_Produccion.parquet")
    if not os.path.exists(path_fct): return set()
    try:
        t = pq.read_table(path_fct, columns=['Periodo_ID'])
        return set(t.column('Periodo_ID').to_pylist())
    except Exception: return set()

def generar_historico_bi():
    logger = setup_audit_logger()
    logger.info("=== INICIANDO PIPELINE ELT - METAS SANITARIAS ===")
    
    # 1. Asegurar directorios base antes de empezar
    bi_dir = normalize_path("data/processed/bi")
    temp_dir = normalize_path("data/processed/temp")
    os.makedirs(bi_dir, exist_ok=True)
    os.makedirs(temp_dir, exist_ok=True)
    
    # 2. Validacion de Entorno Inicial
    map_nombres = load_center_names()
    if not map_nombres:
        logger.error("[CRITICO] Faltan los diccionarios de centros autorizados (COD_CENTROS_SALUD.CSV). Abortando.")
        return
        
    logger.info(f"[OK] Cargado diccionario con {len(map_nombres)} posibles identidades de centros.")

    periodos = detectar_periodos_disponibles()
    ya_guardados = leer_periodos_existentes()
    
    pendientes = [(y, m) for (y, m) in periodos if (y * 100 + m) not in ya_guardados]

    if not pendientes:
        logger.info("[BI] El data warehouse esta al dia. No hay periodos nuevos que consolidar.")
        # Aun asi exportamos dimensiones por si hubo cambios en parametros
        exportar_dimensiones_bi(bi_dir, map_nombres)
        return

    logger.info(f"[ELT] Iteraciones detectadas: {len(pendientes)} periodos pendientes.")

    for y, m in pendientes:
        periodo_str = f"{y}-{m:02d}"
        print(f"\n==========================================")
        print(f"=== PROCESANDO CORTE: {periodo_str} ===")
        print(f"==========================================")
        
        try:
            datos_crudos = run_meta_scripts(agno=y, mes=m)
            consolidado = _formatear_consolidado(datos_crudos, map_nombres)
            consolidado = _aplicar_psr(consolidado)
            
            # Generar Auditoria Tabular Requerida
            generar_resumen_auditoria(consolidado, periodo_str, map_nombres)
            
            _guardar_snapshot(consolidado, periodo_str)
        except Exception as e:
            logger.error(f"[ERROR] Fallo critico procesando {periodo_str}: {e}")
            import traceback
            logger.error(traceback.format_exc())


    # Limpieza de variables de entorno
    os.environ.pop('EV_AGNO', None)
    os.environ.pop('MAX_MES', None)
    
    bi_dir = normalize_path("data/processed/bi")
    exportar_dimensiones_bi(bi_dir, map_nombres)
    
    # Eliminar completamente la produccion temporal
    import shutil
    temp_dir = normalize_path("data/processed/temp")
    if os.path.exists(temp_dir):
        try:
            shutil.rmtree(temp_dir)
            logger.info(f"[BI] Se ha eliminado permanentemente la produccion intermedia en temp.")
        except Exception as e:
            logger.warning(f"[BI] No se pudo eliminar la carpeta temp: {e}")
            
    print("\n[BI] Carga ELT finalizada con éxito. Modelo estrella disponible en data/processed/bi/.")

if __name__ == "__main__":
    generar_historico_bi()
