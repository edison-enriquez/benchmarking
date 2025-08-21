"""
Utilidades para carga y manejo de datos.

Este módulo proporciona funciones para cargar diferentes tipos de archivos
de datos y realizar operaciones comunes de preprocesamiento.
"""

import pandas as pd
import logging
from pathlib import Path
from typing import Optional, Dict, Any, List
from .config import EXCEL_CONFIG, PROJECT_ROOT

logger = logging.getLogger(__name__)

class DataLoader:
    """Clase para cargar diferentes tipos de archivos de datos."""
    
    @staticmethod
    def load_excel(file_path: str, sheet_name: Optional[str] = None, **kwargs) -> pd.DataFrame:
        """
        Carga un archivo Excel.
        
        Args:
            file_path: Ruta al archivo Excel
            sheet_name: Nombre de la hoja (opcional)
            **kwargs: Argumentos adicionales para pandas.read_excel
            
        Returns:
            DataFrame con los datos cargados
        """
        try:
            full_path = PROJECT_ROOT / file_path
            logger.info(f"Cargando archivo Excel: {full_path}")
            
            excel_params = {
                'engine': EXCEL_CONFIG['engine'],
                'sheet_name': sheet_name or EXCEL_CONFIG['sheet_name'],
                **kwargs
            }
            
            df = pd.read_excel(full_path, **excel_params)
            logger.info(f"Archivo cargado exitosamente. Forma: {df.shape}")
            return df
            
        except Exception as e:
            logger.error(f"Error al cargar archivo Excel {file_path}: {str(e)}")
            raise
    
    @staticmethod
    def load_csv(file_path: str, **kwargs) -> pd.DataFrame:
        """
        Carga un archivo CSV.
        
        Args:
            file_path: Ruta al archivo CSV
            **kwargs: Argumentos adicionales para pandas.read_csv
            
        Returns:
            DataFrame con los datos cargados
        """
        try:
            full_path = PROJECT_ROOT / file_path
            logger.info(f"Cargando archivo CSV: {full_path}")
            
            df = pd.read_csv(full_path, **kwargs)
            logger.info(f"Archivo cargado exitosamente. Forma: {df.shape}")
            return df
            
        except Exception as e:
            logger.error(f"Error al cargar archivo CSV {file_path}: {str(e)}")
            raise
    
    @staticmethod
    def save_dataframe(df: pd.DataFrame, file_path: str, file_type: str = 'csv', **kwargs) -> None:
        """
        Guarda un DataFrame en el formato especificado.
        
        Args:
            df: DataFrame a guardar
            file_path: Ruta donde guardar el archivo
            file_type: Tipo de archivo ('csv', 'excel')
            **kwargs: Argumentos adicionales para la función de guardado
        """
        try:
            full_path = PROJECT_ROOT / file_path
            full_path.parent.mkdir(parents=True, exist_ok=True)
            
            if file_type.lower() == 'csv':
                df.to_csv(full_path, index=False, **kwargs)
            elif file_type.lower() in ['excel', 'xlsx']:
                df.to_excel(full_path, index=False, engine=EXCEL_CONFIG['engine'], **kwargs)
            else:
                raise ValueError(f"Tipo de archivo no soportado: {file_type}")
                
            logger.info(f"DataFrame guardado en: {full_path}")
            
        except Exception as e:
            logger.error(f"Error al guardar archivo {file_path}: {str(e)}")
            raise

def clean_column_names(df: pd.DataFrame) -> pd.DataFrame:
    """
    Limpia los nombres de las columnas eliminando espacios y caracteres especiales.
    
    Args:
        df: DataFrame a limpiar
        
    Returns:
        DataFrame con nombres de columnas limpios
    """
    df_clean = df.copy()
    df_clean.columns = [
        col.strip().replace(' ', '_').replace('-', '_').replace('(', '').replace(')', '')
        for col in df_clean.columns
    ]
    return df_clean

def validate_required_columns(df: pd.DataFrame, required_columns: List[str]) -> bool:
    """
    Valida que el DataFrame contenga las columnas requeridas.
    
    Args:
        df: DataFrame a validar
        required_columns: Lista de columnas requeridas
        
    Returns:
        True si todas las columnas están presentes, False en caso contrario
    """
    missing_columns = set(required_columns) - set(df.columns)
    if missing_columns:
        logger.warning(f"Columnas faltantes: {missing_columns}")
        return False
    return True

def get_data_summary(df: pd.DataFrame) -> Dict[str, Any]:
    """
    Genera un resumen de los datos del DataFrame.
    
    Args:
        df: DataFrame a resumir
        
    Returns:
        Diccionario con estadísticas del DataFrame
    """
    return {
        'shape': df.shape,
        'columns': list(df.columns),
        'dtypes': df.dtypes.to_dict(),
        'null_values': df.isnull().sum().to_dict(),
        'memory_usage': df.memory_usage(deep=True).sum()
    }
