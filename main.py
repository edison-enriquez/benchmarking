#!/usr/bin/env python3
"""
Aplicación principal para el análisis de programas académicos.

Este script proporciona una interfaz de línea de comandos para ejecutar
diferentes tipos de análisis sobre los datos de programas académicos.
"""

import argparse
import sys
from pathlib import Path

# Agregar el directorio src al path para importar módulos
sys.path.insert(0, str(Path(__file__).parent / "src"))

from src.utils.logger_config import get_logger
from src.utils.config import PROJECT_ROOT

logger = get_logger(__name__, "main_execution")

def main():
    """Función principal de la aplicación."""
    parser = argparse.ArgumentParser(
        description="Herramienta de análisis de programas académicos",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Ejemplos de uso:
  python main.py --analyze programs --region all
  python main.py --visualize maps --output-dir custom_output
  python main.py --report comprehensive --format markdown
        """
    )
    
    # Argumentos principales
    parser.add_argument(
        "--analyze", 
        choices=["programs", "regions", "temporal", "ies"],
        help="Tipo de análisis a realizar"
    )
    
    parser.add_argument(
        "--visualize",
        choices=["maps", "distributions", "trends"],
        help="Tipo de visualización a generar"
    )
    
    parser.add_argument(
        "--report",
        choices=["comprehensive", "summary", "regional"],
        help="Tipo de reporte a generar"
    )
    
    # Argumentos opcionales
    parser.add_argument(
        "--region",
        help="Región específica para el análisis (ej: REGION_ANDINA, REGION_CARIBE)"
    )
    
    parser.add_argument(
        "--year",
        type=int,
        help="Año específico para el análisis temporal"
    )
    
    parser.add_argument(
        "--format",
        choices=["markdown", "html", "pdf"],
        default="markdown",
        help="Formato de salida para reportes"
    )
    
    parser.add_argument(
        "--output-dir",
        default="output",
        help="Directorio de salida para los resultados"
    )
    
    parser.add_argument(
        "--verbose", "-v",
        action="store_true",
        help="Activar logging detallado"
    )
    
    parser.add_argument(
        "--list-regions",
        action="store_true",
        help="Listar todas las regiones disponibles"
    )
    
    args = parser.parse_args()
    
    # Configurar nivel de logging
    if args.verbose:
        logger.setLevel("DEBUG")
    
    logger.info("Iniciando aplicación de análisis de programas académicos")
    logger.info(f"Directorio de proyecto: {PROJECT_ROOT}")
    
    try:
        # Listar regiones si se solicita
        if args.list_regions:
            from src.utils.config import REGIONES_COLOMBIA
            print("Regiones disponibles:")
            for region, departamentos in REGIONES_COLOMBIA.items():
                print(f"  {region}: {', '.join(departamentos)}")
            return 0
        
        # Ejecutar análisis
        if args.analyze:
            logger.info(f"Ejecutando análisis: {args.analyze}")
            run_analysis(args.analyze, args)
        
        # Generar visualizaciones
        if args.visualize:
            logger.info(f"Generando visualización: {args.visualize}")
            run_visualization(args.visualize, args)
        
        # Generar reportes
        if args.report:
            logger.info(f"Generando reporte: {args.report}")
            run_report(args.report, args)
        
        # Si no se especifica ninguna acción, mostrar ayuda
        if not any([args.analyze, args.visualize, args.report, args.list_regions]):
            parser.print_help()
            return 1
        
        logger.info("Ejecución completada exitosamente")
        return 0
        
    except Exception as e:
        logger.error(f"Error durante la ejecución: {str(e)}")
        if args.verbose:
            logger.exception("Detalles del error:")
        return 1

def run_analysis(analysis_type: str, args):
    """Ejecuta el tipo de análisis especificado."""
    logger.info(f"Ejecutando análisis de tipo: {analysis_type}")
    
    if analysis_type == "programs":
        # Aquí se importaría y ejecutaría el analizador de programas
        print("Ejecutando análisis de programas...")
        # from src.analyzers.program_analyzer import ProgramAnalyzer
        # analyzer = ProgramAnalyzer()
        # analyzer.run_analysis()
        
    elif analysis_type == "regions":
        print("Ejecutando análisis regional...")
        # Implementar análisis regional
        
    elif analysis_type == "temporal":
        print("Ejecutando análisis temporal...")
        # Implementar análisis temporal
        
    elif analysis_type == "ies":
        print("Ejecutando análisis de IES...")
        # Implementar análisis de IES

def run_visualization(viz_type: str, args):
    """Genera el tipo de visualización especificado."""
    logger.info(f"Generando visualización de tipo: {viz_type}")
    
    if viz_type == "maps":
        print("Generando mapas...")
        # Implementar generación de mapas
        
    elif viz_type == "distributions":
        print("Generando gráficos de distribución...")
        # Implementar gráficos de distribución
        
    elif viz_type == "trends":
        print("Generando gráficos de tendencias...")
        # Implementar gráficos de tendencias

def run_report(report_type: str, args):
    """Genera el tipo de reporte especificado."""
    logger.info(f"Generando reporte de tipo: {report_type}")
    
    if report_type == "comprehensive":
        print("Generando reporte comprensivo...")
        # Implementar reporte comprensivo
        
    elif report_type == "summary":
        print("Generando reporte resumen...")
        # Implementar reporte resumen
        
    elif report_type == "regional":
        print("Generando reporte regional...")
        # Implementar reporte regional

if __name__ == "__main__":
    sys.exit(main())
