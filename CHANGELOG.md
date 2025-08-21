# Changelog

Todas las modificaciones notables de este proyecto serán documentadas en este archivo.

El formato está basado en [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
y este proyecto adhiere a [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [1.0.0] - 2025-08-21

### Added
- Reestructuración completa del proyecto en forma de paquete Python profesional
- Módulo `src/analyzers/` con analizadores especializados:
  - `program_analyzer.py` - Análisis de programas académicos
  - `active_inactive_classifier.py` - Clasificación automática de programas
  - `ies_analyzer.py` - Análisis de instituciones educativas superiores
  - `temporal_analyzer.py` - Análisis de evolución temporal
- Módulo `src/visualizers/` con herramientas de visualización:
  - `map_visualizer.py` - Mapas interactivos de Colombia
  - `detailed_map_visualizer.py` - Mapas detallados por departamento
- Módulo `src/utils/` con utilidades comunes:
  - `config.py` - Configuración global del proyecto
  - `data_loader.py` - Carga y validación de datos
  - `logger_config.py` - Sistema de logging centralizado
  - `excel_to_csv.py` - Conversión de formatos
  - `data_consolidator.py` - Consolidación de datos
- Módulo `src/reports/` con generadores de reportes automáticos
- Aplicación principal `main.py` con interfaz de línea de comandos
- Archivo `setup.py` para instalación como paquete Python
- Archivo `requirements.txt` con dependencias actualizadas
- Estructura de directorios organizada:
  - `data/raw/` - Datos originales
  - `data/processed/` - Datos procesados
  - `output/graphics/` - Gráficos generados
  - `output/reports/` - Reportes automáticos
  - `output/logs/` - Archivos de log
  - `tests/` - Pruebas unitarias
  - `docs/` - Documentación
- README.md completamente actualizado con nueva documentación

### Changed
- Reorganización de todos los scripts existentes en módulos especializados
- Migración de archivos de datos a estructura organizada
- Actualización del .gitignore para nueva estructura
- Mejora en el sistema de logging con configuración centralizada

### Moved
- `analizar_programas_activos.py` → `src/analyzers/program_analyzer.py`
- `clasificador_activos_inactivos.py` → `src/analyzers/active_inactive_classifier.py`
- `analizar_datos_ies.py` → `src/analyzers/ies_analyzer.py`
- `evolucion_occidente.py` → `src/analyzers/temporal_analyzer.py`
- `generar_mapa_colombia.py` → `src/visualizers/map_visualizer.py`
- `generar_mapas_detallados.py` → `src/visualizers/detailed_map_visualizer.py`
- `xslTocsv.py` → `src/utils/excel_to_csv.py`
- `consolidado.py` → `src/utils/data_consolidator.py`
- `generar_reporte_*.py` → `src/reports/`
- `files/` → `data/raw/`
- `resultados/` → `data/processed/` y `output/`
- `analisis_distribucion/` → `output/graphics/` y `output/reports/`

## [0.9.0] - 2025-08-20

### Added
- Análisis de evolución temporal para la región Occidente
- Gráficos de similitud entre programas académicos
- Mapas detallados de Colombia con distribución institucional
- Sistema de logs mejorado con timestamps
- Clasificación automática de programas activos e inactivos

### Fixed
- Corrección en nombres de regiones después de actualización
- Mejoras en la generación de gráficos de alta resolución
- Optimización de uso de memoria en procesamiento de datos grandes

## [0.8.0] - 2025-08-19

### Added
- Análisis completo de distribución por regiones
- Generación automática de reportes en Markdown
- Gráficos de distribución de programas académicos
- Análisis de programas activos por región
- Mapas interactivos de Colombia con datos educativos

### Changed
- Actualización de nombres de regiones según nueva clasificación
- Mejoras en la estructura de datos de salida
- Optimización de algoritmos de análisis

## [0.7.0] - 2025-05-07

### Added
- Procesamiento inicial de datos de benchmarking
- Consolidación de archivos Excel por región
- Sistema básico de logging
- Conversión automática de Excel a CSV

### Changed
- Primera versión funcional del sistema de análisis
- Estructura básica de directorios establecida
