"""
Archivo de configuración para pytest.
"""

import pytest
import sys
from pathlib import Path

# Agregar src al path para que pytest pueda importar los módulos
sys.path.insert(0, str(Path(__file__).parent / "src"))

@pytest.fixture
def sample_program_data():
    """Fixture con datos de ejemplo para pruebas."""
    import pandas as pd
    return pd.DataFrame({
        'CODIGO_SNIES_PROGRAMA': [12345, 67890, 11111, 22222],
        'INSTITUCION_EDUCACION_SUPERIOR': ['Universidad A', 'Universidad B', 'Universidad C', 'Universidad D'],
        'NOMBRE_DEL_PROGRAMA': ['Ingeniería de Sistemas', 'Administración', 'Derecho', 'Medicina'],
        'REGION': ['REGION_ANDINA', 'REGION_CARIBE', 'REGION_ANDINA', 'REGION_PACIFICA'],
        'MUNICIPIO_OFERTA_PROGRAMA': ['Bogotá', 'Cartagena', 'Medellín', 'Cali'],
        'ESTADO': ['ACTIVO', 'ACTIVO', 'INACTIVO', 'ACTIVO']
    })

@pytest.fixture
def temp_output_dir(tmp_path):
    """Fixture que proporciona un directorio temporal para pruebas."""
    output_dir = tmp_path / "test_output"
    output_dir.mkdir()
    return output_dir
