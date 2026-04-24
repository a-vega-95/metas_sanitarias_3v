import sys
import os
import csv
import subprocess
import pyarrow as pa
import pyarrow.parquet as pq
import pyarrow.compute as pc
import pyarrow.csv as pv
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
    """Filtra, normaliza e inyecta ceros para centros faltantes (integridad)."""
    codigos_autorizados = set()
    for cod in map_nombres.keys():
        base = cod[:-1] if cod[-1].isalpha() and cod[:-1].isdigit() else cod
        codigos_autorizados.add(base)

    # Agrupar lo que ya vino crudo
    consolidado_dict = {}
    metas_procesadas = set()
    
    for row in datos_crudos:
        m_id = row.get('Meta_ID', '')
        centro = row.get('Centro', '')
        base_centro = centro[:-1] if centro and centro[-1].isalpha() and centro[:-1].isdigit() else centro
        
        if base_centro not in codigos_autorizados and not ('-' in base_centro and 'Meta 7' in m_id):
            continue
            
        # Para Meta 7, descartar centros individuales que ya se reportan agrupados
        # Esto evita que aparezcan filas con 0.0 que confunden el reporte
        if 'Meta 7' in m_id and base_centro in ['121307', '121788', '121347', '121780', '121305', '121782']:
            continue
            
        try:
            num = float(row.get('Numerador', 0))
            den = float(row.get('Denominador', 0))
        except (ValueError, TypeError):
            continue
            
        key = (m_id, base_centro)
        consolidado_dict[key] = {
            'Meta_ID': m_id,
            'COD_CENTRO': base_centro,
            'Numerador_Actual': num,
            'Denominador_Actual': den,
            'Meta_Fijada': float(row.get('Meta_Fijada', 0) or 0) / 100.0,
            'Meta_Nacional': float(row.get('Meta_Nacional', 0) or 0) / 100.0
        }
        metas_procesadas.add(m_id)

    # Rellenar faltantes con 0 para mantener consistencia 100%
    # Try-except conceptual: si falla la captura natural, lo forzamos a 0
    from config import get_meta_params
    for m_id in metas_procesadas:
        params = get_meta_params(m_id.replace('Meta ', 'Meta_'))
        fijada = float(params.get('fijada', 0)) / 100.0
        nacional = float(params.get('nacional', 0)) / 100.0
        
        for cod in codigos_autorizados:
            # Para Meta 7, no rellenar con ceros los centros individuales que ya estan en el grupo
            if 'Meta 7' in m_id and cod in ['121307', '121788', '121347', '121780', '121305', '121782']:
                continue
                
            if (m_id, cod) not in consolidado_dict:
                consolidado_dict[(m_id, cod)] = {
                    'Meta_ID': m_id,
                    'COD_CENTRO': cod,
                    'Numerador_Actual': 0.0,
                    'Denominador_Actual': 0.0,
                    'Meta_Fijada': fijada,
                    'Meta_Nacional': nacional
                }
                
    return list(consolidado_dict.values())

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
    """Genera DIM_Metas, DIM_Establecimientos y DIM_Calendario (bloqueado a 2026)."""
    from config import PARAMETROS_ANUALES, AGNO_ACTUAL

    os.makedirs(bi_dir, exist_ok=True)
    bi_csv_dir = normalize_path("data/processed/bi_csv")
    os.makedirs(bi_csv_dir, exist_ok=True)

    if not map_nombres:
        map_nombres = load_center_names()

    # DIM_METAS
    metas_data = PARAMETROS_ANUALES.get(AGNO_ACTUAL, {}).get('metas', {})
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
    tabla_metas = pa.Table.from_pylist(filas_dim_metas)
    pq.write_table(tabla_metas, os.path.join(bi_dir, "DIM_Metas.parquet"))
    _guardar_copia_csv(tabla_metas, bi_csv_dir, "DIM_Metas.csv")

    # DIM_ESTABLECIMIENTOS
    filas_dim_est = []
    csv_path = normalize_path("data/dictionaries/COD_CENTROS_SALUD.CSV")
    if os.path.exists(csv_path):
        with open(csv_path, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                cod = row['COD_CENTRO'].strip()
                if cod.endswith('A'): cod = cod[:-1]
                nombre = row['NOMBRE'].strip()
                if cod == '121352':
                    nombre = 'M. VALECH + PSRs (Collimallin y Conoco)'
                    
                filas_dim_est.append({
                    'Establecimiento_ID': cod,
                    'Nombre_Centro': nombre,
                    'Tipo_Centro': row['TIPO_CENTRO'].strip(),
                    'Comuna': 'Temuco'
                })
    
    # Inyectar centros agrupados para Meta 7
    filas_dim_est.append({
        'Establecimiento_ID': '121307-121788',
        'Nombre_Centro': 'Amanecer + Las Quilas',
        'Tipo_Centro': 'AGRUPACION',
        'Comuna': 'Temuco'
    })
    filas_dim_est.append({
        'Establecimiento_ID': '121347-121780',
        'Nombre_Centro': 'Pedro de Valdivia + El Salar',
        'Tipo_Centro': 'AGRUPACION',
        'Comuna': 'Temuco'
    })
    filas_dim_est.append({
        'Establecimiento_ID': '121305-121782',
        'Nombre_Centro': 'Villa Alegre + Arquenco',
        'Tipo_Centro': 'AGRUPACION',
        'Comuna': 'Temuco'
    })

    visto = set()
    uniq = []
    for f in filas_dim_est:
        if f['Establecimiento_ID'] not in visto:
            uniq.append(f)
            visto.add(f['Establecimiento_ID'])
    tabla_est = pa.Table.from_pylist(uniq)
    pq.write_table(tabla_est, os.path.join(bi_dir, "DIM_Establecimientos.parquet"))
    _guardar_copia_csv(tabla_est, bi_csv_dir, "DIM_Establecimientos.csv")

    # DIM_CALENDARIO — solo anno 2026
    from datetime import date
    meses_nombres = ["", "Enero", "Febrero", "Marzo", "Abril", "Mayo", "Junio",
                     "Julio", "Agosto", "Septiembre", "Octubre", "Noviembre", "Diciembre"]
    filas_cal = []
    for mes in range(1, 13):
        filas_cal.append({
            'Periodo_ID': AGNO_ACTUAL * 100 + mes,
            'Anno': AGNO_ACTUAL,
            'Mes_Numero': mes,
            'Mes_Nombre': meses_nombres[mes],
            'Semestre': 'S1' if mes <= 6 else 'S2',
            'Fecha': date(AGNO_ACTUAL, mes, 1)
        })
    tabla_cal = pa.Table.from_pylist(filas_cal)
    pq.write_table(tabla_cal, os.path.join(bi_dir, "DIM_Calendario.parquet"))
    _guardar_copia_csv(tabla_cal, bi_csv_dir, "DIM_Calendario.csv")
    print("[BI] Dimensiones generadas en data/processed/bi/ y bi_csv/")

def _guardar_copia_csv(tabla, bi_csv_dir, nombre_archivo):
    """Guarda una copia de la tabla PyArrow en formato CSV."""
    csv_path = os.path.join(bi_csv_dir, nombre_archivo.replace(".parquet", ".csv"))
    pv.write_csv(tabla, csv_path)

def _acumular_hechos(todos_los_hechos, consolidado, periodo_str):
    """
    Acumula las filas de un periodo en la lista maestra en memoria.
    El guardado a disco se realiza una sola vez al final (procesamiento total).
    """
    periodo_id = int(periodo_str.replace('-', ''))
    for row in consolidado:
        todos_los_hechos.append({
            'Periodo_ID': periodo_id,
            'Establecimiento_ID': row.get('COD_CENTRO', ''),
            'Meta_ID': _obtener_meta_id_bi(row.get('Meta_ID', '')),
            'Numerador': float(row.get('Numerador_Actual', 0) or 0),
            'Denominador': float(row.get('Denominador_Actual', 0) or 0),
            'Meta_Fijada': float(row.get('Meta_Fijada', 0)),
            'Meta_Nacional': float(row.get('Meta_Nacional', 0))
        })

def detectar_periodos_disponibles():
    """
    Escanea ANNIO_CURSO buscando periodos disponibles (Anno=2026, Mes).

    Logica:
    - SERIE_A : cada subcarpeta mensual (01..12) no vacia es un periodo.
    - SERIE_P : corte 06 -> meses 6-11 | corte 12 -> mes 12.
    """
    from config import DIR_SERIE_A_ACTUAL, DIR_SERIE_P_ACTUAL, AGNO_ACTUAL
    periodos = set()
    y = AGNO_ACTUAL

    # --- SERIE_A ---
    if os.path.isdir(DIR_SERIE_A_ACTUAL):
        for sub in os.listdir(DIR_SERIE_A_ACTUAL):
            sub_path = os.path.join(DIR_SERIE_A_ACTUAL, sub)
            if sub.isdigit() and len(sub) == 2 and os.path.isdir(sub_path):
                if os.listdir(sub_path):
                    periodos.add((y, int(sub)))

    # --- SERIE_P ---
    if os.path.isdir(DIR_SERIE_P_ACTUAL):
        corte_jun = os.path.join(DIR_SERIE_P_ACTUAL, "06")
        if os.path.isdir(corte_jun) and os.listdir(corte_jun):
            for m in range(6, 12):
                periodos.add((y, m))

        corte_dic = os.path.join(DIR_SERIE_P_ACTUAL, "12")
        if os.path.isdir(corte_dic) and os.listdir(corte_dic):
            periodos.add((y, 12))

    return sorted(periodos)

def generar_historico_bi():
    """
    Pipeline de procesamiento TOTAL para el anno 2026.
    Cada ejecucion reescribe FCT_Produccion.parquet desde cero con todos
    los periodos disponibles en ANNIO_CURSO. Sin logica incremental.
    """
    import shutil
    logger = setup_audit_logger()
    logger.info("=== INICIANDO PIPELINE ELT 2026 (PROCESAMIENTO TOTAL) ===")

    bi_dir = normalize_path("data/processed/bi")
    bi_csv_dir = normalize_path("data/processed/bi_csv")
    os.makedirs(bi_dir, exist_ok=True)
    os.makedirs(bi_csv_dir, exist_ok=True)

    # Validacion de entorno
    map_nombres = load_center_names()
    if not map_nombres:
        logger.error("[CRITICO] Faltan los diccionarios de centros autorizados. Abortando.")
        return
    logger.info(f"[OK] Diccionario cargado: {len(map_nombres)} identidades de centros.")

    periodos = detectar_periodos_disponibles()
    if not periodos:
        logger.error("[CRITICO] No se detectaron periodos en ANNIO_CURSO/SERIE_A. Revisa las carpetas.")
        return

    logger.info(f"[ELT] Periodos a procesar: {len(periodos)} -> {periodos}")

    # Lista maestra en memoria — se escribe a disco una sola vez al final
    todos_los_hechos = []

    for y, m in periodos:
        periodo_str = f"{y}-{m:02d}"
        print(f"\n==========================================")
        print(f"=== PROCESANDO CORTE: {periodo_str} ===")
        print(f"==========================================")
        try:
            datos_crudos = run_meta_scripts(agno=y, mes=m)
            consolidado  = _formatear_consolidado(datos_crudos, map_nombres)
            consolidado  = _aplicar_psr(consolidado)
            generar_resumen_auditoria(consolidado, periodo_str, map_nombres)
            _acumular_hechos(todos_los_hechos, consolidado, periodo_str)
        except Exception as e:
            logger.error(f"[ERROR] Fallo critico procesando {periodo_str}: {e}")
            import traceback
            logger.error(traceback.format_exc())

    # Escritura total a disco (reemplaza el parquet completo)
    if todos_los_hechos:
        path_fct = os.path.join(bi_dir, "FCT_Produccion.parquet")
        tabla = pa.Table.from_pylist(todos_los_hechos)
        pq.write_table(tabla, path_fct, compression='snappy')
        _guardar_copia_csv(tabla, bi_csv_dir, "FCT_Produccion.csv")
        
        logger.info(f"[BI] FCT_Produccion.parquet escrito: {len(todos_los_hechos)} filas.")
        print(f"[BI] Hechos escritos: {len(todos_los_hechos)} filas -> {path_fct}")
    else:
        logger.warning("[BI] No se generaron filas. FCT_Produccion no fue modificado.")

    exportar_dimensiones_bi(bi_dir, map_nombres)

    # Limpiar temp si existe
    temp_dir = normalize_path("data/processed/temp")
    if os.path.exists(temp_dir):
        try:
            shutil.rmtree(temp_dir)
        except Exception:
            pass

    print("\n[BI] Pipeline 2026 finalizado. Modelo estrella disponible en data/processed/bi/.")

if __name__ == "__main__":
    generar_historico_bi()
