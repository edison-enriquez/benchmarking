# 📚 Benchmarking de Programas Académicos - UNIAJC

Sistema integral para el análisis y benchmarking de programas académicos en Colombia, desarrollado por la Universidad Antonio José de Camacho (UNIAJC).

## 🎯 Propósito

Esta herramienta proporciona análisis comprehensivos de programas académicos colombianos, incluyendo:

- **Análisis de distribución regional** de programas académicos
- **Mapas geográficos interactivos** con datos de instituciones educativas
- **Clasificación automática** de programas activos e inactivos
- **Análisis temporal** de tendencias educativas
- **Reportes automáticos** en múltiples formatos
- **Visualizaciones estadísticas** de alto nivel

## 🏗️ Arquitectura del Proyecto

```
benchmarking/
├── src/                          # Código fuente principal
│   ├── analyzers/               # Módulos de análisis de datos
│   │   ├── program_analyzer.py         # Análisis de programas
│   │   ├── active_inactive_classifier.py  # Clasificador activos/inactivos
│   │   ├── ies_analyzer.py            # Análisis de IES
│   │   └── temporal_analyzer.py       # Análisis temporal
│   ├── visualizers/             # Herramientas de visualización
│   │   ├── map_visualizer.py          # Mapas de Colombia
│   │   └── detailed_map_visualizer.py # Mapas detallados
│   ├── utils/                   # Utilidades comunes
│   │   ├── config.py                  # Configuración global
│   │   ├── data_loader.py            # Carga de datos
│   │   ├── logger_config.py          # Configuración de logging
│   │   ├── excel_to_csv.py           # Conversión Excel a CSV
│   │   └── data_consolidator.py      # Consolidación de datos
│   └── reports/                 # Generadores de reportes
│       ├── generar_reporte_*.py       # Varios generadores
├── data/                        # Datos del proyecto
│   ├── raw/                     # Datos originales
│   └── processed/               # Datos procesados
├── output/                      # Resultados generados
│   ├── graphics/                # Gráficos y visualizaciones
│   ├── reports/                 # Reportes en Markdown/HTML
│   └── logs/                    # Archivos de log
├── tests/                       # Pruebas unitarias
├── docs/                        # Documentación
├── main.py                      # Aplicación principal
├── setup.py                     # Configuración de instalación
└── requirements.txt             # Dependencias

## � Instalación y Configuración

### Prerrequisitos

- Python 3.8 o superior
- Git (para clonar el repositorio)

### Instalación

1. **Clonar el repositorio:**
```bash
git clone https://github.com/edison-enriquez/benchmarking.git
cd benchmarking
```

2. **Crear entorno virtual:**
```bash
python -m venv venv
source venv/bin/activate  # En Linux/Mac
# o
venv\Scripts\activate     # En Windows
```

3. **Instalar dependencias:**
```bash
pip install -r requirements.txt
```

4. **Instalar el paquete en modo desarrollo:**
```bash
pip install -e .
```

## 🎯 Uso Rápido

### Interfaz de Línea de Comandos

```bash
# Listar regiones disponibles
python main.py --list-regions

# Ejecutar análisis de programas
python main.py --analyze programs --region REGION_ANDINA

# Generar mapas interactivos
python main.py --visualize maps --output-dir custom_output

# Crear reporte comprensivo
python main.py --report comprehensive --format markdown
```

### Uso Programático

```python
from src.analyzers.program_analyzer import ProgramAnalyzer
from src.visualizers.map_visualizer import MapVisualizer

# Análisis de programas
analyzer = ProgramAnalyzer()
results = analyzer.analyze_by_region('REGION_ANDINA')

# Generar mapa
visualizer = MapVisualizer()
visualizer.create_colombia_map(results)
```

## 🔧 Configuración

El archivo `src/utils/config.py` contiene todas las configuraciones principales:

- **Directorios de datos**: Rutas a archivos de entrada y salida
- **Regiones de Colombia**: Mapeo de departamentos a regiones
- **Configuración de gráficos**: Resolución, estilo, paleta de colores
- **Parámetros de logging**: Nivel y formato de logs

## 📊 Funcionalidades Principales

### 1. Análisis de Distribución
- Distribución de programas por región
- Análisis de concentración geográfica
- Estadísticas descriptivas por área de conocimiento

### 2. Mapas Geográficos
- Mapas de Colombia con distribución de programas
- Visualización por departamentos
- Mapas interactivos con Folium
- Mapas estáticos de alta resolución

### 3. Clasificación de Programas
- Algoritmo automático para clasificar programas activos/inactivos
- Análisis de tendencias temporales
- Métricas de desempeño por institución

### 4. Análisis Temporal
- Evolución de inscritos y graduados por región
- Tendencias a lo largo del tiempo
- Proyecciones estadísticas

### 5. Reportes Automáticos
- Reportes en Markdown con gráficos embebidos
- Exportación a HTML y PDF
- Documentación automática de resultados

## 📁 Estructura de Datos

### Datos de Entrada
```
data/raw/
├── programas_benchmarking.xlsx    # Archivo maestro de programas
├── 2018/ ... 2024/               # Datos anuales por año
└── csv_convertidos/              # Archivos CSV convertidos
```

### Datos Procesados
```
data/processed/
├── Consolidado_Programas_ACTIVOS.xlsx
├── Consolidado_Programas_INACTIVOS.xlsx
├── datos_consolidados_ies.csv
└── *.csv                         # Archivos procesados por región
```

### Salidas Generadas
```
output/
├── graphics/                     # Gráficos PNG de alta resolución
├── reports/                      # Reportes en Markdown/HTML
└── logs/                         # Archivos de log del sistema
```

## 🔍 Scripts Heredados

Los siguientes scripts han sido reorganizados y están disponibles en sus nuevas ubicaciones:

| Script Original | Nueva Ubicación | Función |
|---|---|---|
| `process_1.py` | `src/utils/` | Procesamiento base de indicadores |
| `consolidado.py` | `src/utils/data_consolidator.py` | Consolidación de datos Excel |
| `xslTocsv.py` | `src/utils/excel_to_csv.py` | Conversión Excel a CSV |
| `analizar_programas_activos.py` | `src/analyzers/program_analyzer.py` | Análisis de programas |
| `generar_mapa_colombia.py` | `src/visualizers/map_visualizer.py` | Generación de mapas |

## 🧪 Testing

```bash
# Ejecutar todas las pruebas
python -m pytest tests/

# Ejecutar con cobertura
python -m pytest tests/ --cov=src --cov-report=html
```

## 📈 Contribución

1. **Fork del repositorio**
2. **Crear rama de feature**: `git checkout -b feature/nueva-funcionalidad`
3. **Commit de cambios**: `git commit -am 'Agregar nueva funcionalidad'`
4. **Push a la rama**: `git push origin feature/nueva-funcionalidad`
5. **Crear Pull Request**

## 📄 Licencia

Este proyecto está licenciado bajo la Licencia MIT - ver el archivo [LICENSE](LICENSE) para detalles.

## 👥 Autores

- **Universidad Antonio José de Camacho** - *Desarrollo inicial* - [UNIAJC](https://www.uniajc.edu.co)

## 📞 Soporte

Para soporte técnico o consultas:

- 📧 Email: soporte.ptei@uniajc.edu.co
- 🌐 Web: [https://www.uniajc.edu.co](https://www.uniajc.edu.co)
- 📱 Issues: [GitHub Issues](https://github.com/edison-enriquez/benchmarking/issues)

## 🔄 Actualizaciones

Ver [CHANGELOG.md](CHANGELOG.md) para un historial detallado de cambios.

---

**Última actualización:** Agosto 2025  
**Versión:** 1.0.0






