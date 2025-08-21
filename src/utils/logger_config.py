"""
Configuración de logging para el proyecto.

Este módulo configura el sistema de logging centralizado para todo el proyecto.
"""

import logging
import sys
from datetime import datetime
from pathlib import Path
from .config import LOGGING_CONFIG, LOGS_DIR

def setup_logger(name: str, log_file: str = None, level: str = None) -> logging.Logger:
    """
    Configura un logger para el proyecto.
    
    Args:
        name: Nombre del logger
        log_file: Nombre del archivo de log (opcional)
        level: Nivel de logging (opcional)
        
    Returns:
        Logger configurado
    """
    logger = logging.getLogger(name)
    
    # Evitar configurar el mismo logger múltiples veces
    if logger.handlers:
        return logger
    
    # Configurar nivel
    log_level = getattr(logging, (level or LOGGING_CONFIG['level']).upper())
    logger.setLevel(log_level)
    
    # Crear formatter
    formatter = logging.Formatter(
        LOGGING_CONFIG['format'],
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    
    # Handler para consola
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(log_level)
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)
    
    # Handler para archivo (si se especifica)
    if log_file:
        # Crear directorio de logs si no existe
        LOGS_DIR.mkdir(parents=True, exist_ok=True)
        
        # Crear nombre de archivo con timestamp
        timestamp = datetime.now().strftime(LOGGING_CONFIG['date_format'])
        log_filename = f"{log_file}_{timestamp}.log"
        log_path = LOGS_DIR / log_filename
        
        file_handler = logging.FileHandler(log_path)
        file_handler.setLevel(log_level)
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)
        
        logger.info(f"Log file created: {log_path}")
    
    return logger

def get_logger(name: str, log_file: str = None) -> logging.Logger:
    """
    Obtiene un logger configurado para el módulo especificado.
    
    Args:
        name: Nombre del módulo/logger
        log_file: Nombre base del archivo de log (opcional)
        
    Returns:
        Logger configurado
    """
    return setup_logger(name, log_file)

# Logger principal del proyecto
main_logger = get_logger('benchmarking', 'benchmarking_main')
