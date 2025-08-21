"""
Configuración global del proyecto de benchmarking.

Este módulo contiene las configuraciones y constantes utilizadas
en todo el proyecto.
"""

import os
from pathlib import Path

# Directorios base
PROJECT_ROOT = Path(__file__).parent.parent
DATA_DIR = PROJECT_ROOT / "data"
RAW_DATA_DIR = DATA_DIR / "raw"
PROCESSED_DATA_DIR = DATA_DIR / "processed"
OUTPUT_DIR = PROJECT_ROOT / "output"
GRAPHICS_DIR = OUTPUT_DIR / "graphics"
REPORTS_DIR = OUTPUT_DIR / "reports"
LOGS_DIR = OUTPUT_DIR / "logs"

# Archivos de datos principales
PROGRAMAS_BENCHMARKING_FILE = "files/programas_benchmarking.xlsx"
CONSOLIDADO_ACTIVOS_FILE = "resultados/Consolidado_Programas_ACTIVOS.xlsx"
CONSOLIDADO_INACTIVOS_FILE = "resultados/Consolidado_Programas_INACTIVOS.xlsx"
DATOS_IES_FILE = "resultados/datos_consolidados_ies.csv"

# Configuración de gráficos
GRAPHICS_CONFIG = {
    'dpi': 300,
    'figsize': (15, 10),
    'style': 'whitegrid',
    'color_palette': 'viridis'
}

# Configuración de regiones
REGIONES_COLOMBIA = {
    'REGION_AMAZONICA': ['Amazonas', 'Caquetá', 'Guainía', 'Guaviare', 'Putumayo', 'Vaupés'],
    'REGION_ANDINA': ['Antioquia', 'Boyacá', 'Caldas', 'Cundinamarca', 'Huila', 'Norte de Santander', 
                      'Quindío', 'Risaralda', 'Santander', 'Tolima'],
    'REGION_CARIBE': ['Atlántico', 'Bolívar', 'Cesar', 'Córdoba', 'La Guajira', 'Magdalena', 'Sucre'],
    'REGION_INSULAR': ['San Andrés y Providencia'],
    'REGION_ORINOQUIA': ['Arauca', 'Casanare', 'Meta', 'Vichada'],
    'REGION_PACIFICA': ['Cauca', 'Chocó', 'Nariño', 'Valle del Cauca']
}

# Configuración de logging
LOGGING_CONFIG = {
    'level': 'INFO',
    'format': '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    'date_format': '%Y%m%d_%H%M%S'
}

# Configuración de Excel
EXCEL_CONFIG = {
    'engine': 'openpyxl',
    'sheet_name': 'Hoja1'
}
