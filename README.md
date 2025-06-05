#  Generador de CSVs por Región e Indicador (process_1.py)

Este script forma parte de un sistema de procesamiento de indicadores educativos y tiene como objetivo generar archivos CSV por región y por tipo de indicador educativo (ej. inscritos, admitidos, etc.), usando como base un archivo maestro de programas académicos (programas_benchmarking.xlsx) y archivos de indicadores en formato CSV convertidos previamente desde archivos Excel.

## Propósito
Procesar múltiples archivos de indicadores educativos por año y consolidar la información filtrada por región, generando salidas estructuradas que luego pueden ser consolidadas en un único archivo Excel mediante el script consolidado.py


## Estructura de Carpetas
´´´yaml
├── files/
│   ├── programas_benchmarking.xlsx
│   └── csv_convertidos/
│       ├── 2020/
│       │   └── INSCRITOS.csv
│       ├── 2021/
│       └── ...
├── resultados/
│   └── por_region/
│       └── ANDINA_INSCRITOS.csv
│       └── ...
´´´

## Requisitos
Requisitos
Python 3.7+
Librerías:
  pandas
  openpyxl

## Parámetros y Configuración
- Entrada principal: ´ files/programas_benchmarking.xlsx´ (hoja: "Programas")
- Entradas CSV: Subcarpetas anuales dentro de ´files/csv_convertidos/´ que contienen archivos por indicador.
- Indicadores procesados:
´´´python
INDICADORES = ['INSCRITOS', 'ADMITIDOS', 'MATRICULADOS', 'PRIMER_CURSO', 'GRADUADOS']
´´´

- Columnas clave del maestro:
´´´python
COLUMNAS_BASE_PROGRAMAS = [
    'CODIGO_SNIES_PROGRAMA',
    'INSTITUCION_EDUCACION_SUPERIOR',
    'NOMBRE_DEL_PROGRAMA',
    'REGION',
    'MUNICIPIO_OFERTA_PROGRAMA'
]

´´´
## Qué hace el script
1. Carga el archivo maestro de programas.
2. Normaliza nombres de columnas y códigos SNIES.
3. Itera por cada región detectada en el maestro.
4. Lee archivos ´CSV´  anuales por indicador.
5. Filtra por los programas pertenecientes a la región.
6. Construye periodos tipo ´YYYY_S´ a partir de columnas o combinaciones ´AÑO + SEMESTRE´.
7. Genera una tabla pivote con valores por período.
8. Combina y exporta un ´CSV´ por indicador y por región.

## Ejecución
Desde la raíz del proyecto:
´´´bash
python process_1.py

´´´

Esto generará archivos en ´resultados/por_region/´ como:

´´´
ANDINA_INSCRITOS.csv
CARIBE_GRADUADOS.csv
´´´

También se genera un archivo de log en ´resultados/´ con información detallada del proceso:

´´´
log_procesamiento_por_region_YYYYMMDD_HHMMSS.txt
´´´

## Integración con ´consolidado.py´
Los archivos generados con process_1.py son insumo para ´consolidado.py´, que unifica todos los datos por región en un solo archivo Excel consolidado con formato avanzado.

