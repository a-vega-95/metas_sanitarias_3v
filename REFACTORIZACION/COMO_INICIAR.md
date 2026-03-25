# Guía de Inicio Rápido (Versión Data Warehouse / BI)

Este repositorio ha sido optimizado estructuralmente para funcionar exclusivamente como un pipeline de extracción (ELT) que alimenta directamente a Power BI a través de archivos **Parquet** (`DIM_*` y `FCT_Produccion`). Se ha eliminado toda la generación de reportes obsoletos en Excel y chequeos bloqueantes para garantizar un procesamiento continuo.

## 1. Obtención de Datos

### A. Archivos REM (Registro Estadístico Mensual)
- **Fuente oficial**: [Departamento de Estadísticas Araucanía Sur](https://estadistica.araucaniasur.cl/?page_id=9837)
- **Qué descargar**: Archivos Excel (`.xlsm` o `.xlsx`) de **Serie A** y **Serie P** para cada centro, por mes.

### B. Archivo PIV (Población Inscrita Validada)
- **Fuente**: FONASA / Encargado de estadística o per cápita del DESAM.
- **Formato**: `.parquet` con columnas `COD_CENTRO`, `EDAD_EN_FECHA_CORTE`, `GENERO`, `ACEPTADO_RECHAZADO`.

## 2. Estructura de Carpetas

La estructura jerárquica detecta automáticamente los años y meses:

```text
data/
  raw/
    rem/
      2025/                 ← Carpeta del año
        SERIE_A/
          01/               ← mes Enero
            121305A.xlsm
        SERIE_P/
          06/               ← mes Junio
      2026/
    piv/
      2025/
        PIV_2025_09.parquet
      2026/
        PIV_2026_09.parquet
```

## 3. Ejecución del Pipeline Histórico/Mensual

El orquestador está diseñado para ejecutarse de manera **secuencial** (previniendo cuellos de botella y memory leaks en Windows) procesando todos los meses disponibles y generando automáticamente las dimensiones y la tabla de hechos en la carpeta de BI.

```bash
# Desde la carpeta raíz del proyecto
python src/main.py
```

**Flujo interno:**
1. Escanea todos los meses disponibles en `data/raw/rem/` y evalúa qué periodos aún no están en la tabla de hechos (`FCT_Produccion.parquet`).
2. Para cada nuevo mes, ejecuta de forma aislada y tolerante a fallos los 7 scripts de metas (ej. si para Enero 2024 falta el PIV 2023, el sistema continúa).
3. Consolida los CSVs intermedios y absorbe los PSRs en los CESFAM correspondientes.
4. Genera el Modelo Estrella completo (`DIM_Metas`, `DIM_Establecimientos`, `DIM_Calendario`, `FCT_Produccion`) nativo en `data/processed/bi/`.

## 4. Resultado (Data Warehouse)

La carpeta `data/processed/bi/` contendrá:
- `DIM_Calendario.parquet` (Dinámico según los años detectados)
- `DIM_Establecimientos.parquet`
- `DIM_Metas.parquet` (Dinámico usando `data/dictionaries/parametros_metas.json`)
- `FCT_Produccion.parquet` (La tabla de hechos transaccional histórica consolidada)
