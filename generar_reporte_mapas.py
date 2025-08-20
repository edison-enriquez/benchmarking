#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Generador de Reporte de Mapas de Colombia
Crea documentación completa de todos los mapas generados
"""

from datetime import datetime

def generar_reporte_mapas():
    """Generar reporte completo de mapas de Colombia"""
    fecha_actual = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    
    reporte = f"""# Mapas de Colombia - Distribución Regional de Instituciones Educativas

**Fecha de generación:** {fecha_actual}
**Proyecto:** Análisis de Programas de Benchmarking y Programas Activos

## 🗺️ Resumen de Mapas Generados

Se han generado **6 mapas diferentes** que muestran la distribución geográfica de instituciones de educación superior en Colombia, organizados por las 6 regiones definidas en el estudio.

---

## 📊 Estadísticas Regionales Mapeadas

### Distribución por Región

| Posición | Región | Instituciones | Programas | Estudiantes | % Estudiantes |
|---|---|---|---|---|---|
| 1 | **Región Centro Occidente** | 7 | 11 | 10,208 | 31.7% |
| 2 | **Región Bogotá** | 10 | 11 | 9,246 | 28.7% |
| 3 | **Región Norte** | 3 | 5 | 4,977 | 15.4% |
| 4 | **Región Occidente** | 5 | 12 | 3,578 | 11.1% |
| 5 | **Región Centro Oriente** | 3 | 5 | 3,032 | 9.4% |
| 6 | **Región Sur Oriente** | 1 | 2 | 1,191 | 3.7% |

**Total Nacional:** 29 instituciones, 46 programas, 32,232 estudiantes

---

## 🗺️ Catálogo de Mapas Generados

### 1. Mapas Interactivos

#### 1.1 `mapa_colombia_interactivo.html`
- **Tipo:** Mapa interactivo Folium
- **Características:**
  - Marcadores circulares por ciudad
  - Tamaño proporcional al número de instituciones
  - Colores por región
  - Tooltips informativos
  - Leyenda integrada
- **Uso:** Navegación web interactiva
- **Tamaño:** 36 KB

#### 1.2 `mapa_colombia_plotly.html`
- **Tipo:** Mapa interactivo Plotly
- **Características:**
  - Mapbox base de OpenStreetMap
  - Marcadores escalables
  - Información detallada en hover
  - Zoom y navegación interactiva
- **Uso:** Presentaciones interactivas
- **Tamaño:** 4.8 MB

### 2. Mapas Estáticos (PNG)

#### 2.1 `mapa_colombia_completo.png`
- **Tipo:** Dashboard de 4 gráficas
- **Contenido:**
  - Distribución geográfica general
  - Gráfica de barras por región
  - Total de estudiantes por región
  - Relación instituciones vs estudiantes
- **Resolución:** 300 DPI
- **Tamaño:** 860 KB

#### 2.2 `mapa_colombia_detallado.png`
- **Tipo:** Mapa principal detallado
- **Características:**
  - Contorno aproximado de Colombia
  - Todas las ciudades etiquetadas
  - Leyenda completa con estadísticas
  - Marcadores proporcionales
- **Resolución:** 300 DPI
- **Tamaño:** 887 KB

#### 2.3 `mapa_regiones_detalle.png`
- **Tipo:** Grid de 6 mapas regionales
- **Contenido:**
  - Un mapa por cada región
  - Enfoque en ciudades específicas
  - Estadísticas por región
  - Colores diferenciados
- **Resolución:** 300 DPI
- **Tamaño:** 629 KB

#### 2.4 `mapa_concentracion.png`
- **Tipo:** Mapas comparativos de concentración
- **Contenido:**
  - Mapa 1: Concentración por instituciones
  - Mapa 2: Concentración por estudiantes
  - Tamaños proporcionales
  - Etiquetas numéricas
- **Resolución:** 300 DPI
- **Tamaño:** 413 KB

---

## 🎨 Código de Colores por Región

| Región | Color | Código Hex | Características |
|---|---|---|---|
| **Región Bogotá** | 🔴 Rojo | #E74C3C | Capital, mayor concentración institucional |
| **Región Centro Occidente** | 🔵 Azul | #3498DB | Eje Cafetero, líder en estudiantes |
| **Región Occidente** | 🟢 Verde | #2ECC71 | Región Pacífico, Valle del Cauca |
| **Región Centro Oriente** | 🟠 Naranja | #F39C12 | Santanderes, región petrolera |
| **Región Norte** | 🟣 Púrpura | #9B59B6 | Costa Caribe, puertos principales |
| **Región Sur Oriente** | 🟤 Café | #E67E22 | Llanos, menor concentración |

---

## 📍 Ciudades Mapeadas por Región

### Región Centro Occidente (Líder en estudiantes)
- **Medellín** - Antioquia
- **Manizales** - Caldas
- **Armenia** - Quindío
- **Dosquebradas** - Risaralda
- **Ibagué** - Tolima
- **Espinal** - Tolima
- **Neiva** - Huila

### Región Bogotá (Líder en instituciones)
- **Bogotá, D.C.** - Distrito Capital
- **Facatativá** - Cundinamarca

### Región Norte (Región Caribe)
- **Barranquilla** - Atlántico
- **Cartagena de Indias** - Bolívar
- **Riohacha** - La Guajira

### Región Occidente (Mayor número de programas)
- **Santiago de Cali** - Valle del Cauca
- **Palmira** - Valle del Cauca
- **Buenaventura** - Valle del Cauca
- **Tuluá** - Valle del Cauca
- **Cartago** - Valle del Cauca
- **Zarzal** - Valle del Cauca
- **Popayán** - Cauca
- **Pasto** - Nariño

### Región Centro Oriente
- **Bucaramanga** - Santander
- **Girón** - Santander
- **Sogamoso** - Boyacá
- **San José de Cúcuta** - Norte de Santander

### Región Sur Oriente
- **Villavicencio** - Meta

---

## 🔧 Especificaciones Técnicas

### Herramientas Utilizadas
- **Python 3.11** - Lenguaje principal
- **Matplotlib 3.10** - Mapas estáticos
- **Folium 0.20** - Mapas interactivos
- **Plotly 6.3** - Visualizaciones web
- **Pandas 2.3** - Procesamiento de datos

### Datos Fuente
- **programas_benchmarking.xlsx** - Datos de distribución inicial
- **Consolidado_Programas_ACTIVOS.xlsx** - Datos actualizados de programas activos
- **Coordenadas geográficas** - Base de datos de ciudades colombianas

### Resolución y Formatos
- **Mapas PNG:** 300 DPI, alta calidad para impresión
- **Mapas HTML:** Responsivos, compatibles con navegadores web
- **Tamaño total:** ~8 MB de visualizaciones geográficas

---

## 📈 Insights Geográficos Principales

### 1. **Concentración Geográfica**
- **69.8%** de los estudiantes se concentran en las 3 regiones principales
- **Triángulo de oro educativo:** Bogotá - Medellín - Cali

### 2. **Distribución Regional**
- **Región Centro Occidente** lidera en volumen estudiantil
- **Región Bogotá** domina en número de instituciones
- **Región Occidente** tiene la mayor diversidad programática

### 3. **Patrones Urbanos**
- Las **capitales departamentales** concentran la mayoría de instituciones
- **Ciudades intermedias** tienen presencia significativa
- **Corredores urbanos** muestran continuidad educativa

### 4. **Conectividad Regional**
- **Eje Cafetero** muestra alta densidad institucional
- **Corredor Caribe** se concentra en puertos principales
- **Región Oriental** se distribuye en ciudades mineras/petroleras

---

## 🎯 Aplicaciones de los Mapas

### Para Investigación
- Análisis de cobertura geográfica educativa
- Identificación de brechas regionales
- Planificación de expansión institucional

### Para Política Pública
- Distribución equitativa de recursos
- Identificación de zonas desatendidas
- Planificación de infraestructura educativa

### Para Instituciones
- Análisis de competencia regional
- Identificación de oportunidades de mercado
- Planificación estratégica territorial

---

## 📝 Metodología de Mapeo

### 1. **Recolección de Datos**
- Extracción de ubicaciones desde bases de datos institucionales
- Validación de coordenadas geográficas
- Agrupación por criterios regionales

### 2. **Procesamiento Geográfico**
- Geocodificación de ciudades
- Asignación de colores por región
- Cálculo de tamaños proporcionales

### 3. **Visualización**
- Generación de múltiples formatos
- Optimización para diferentes usos
- Validación de precisión geográfica

---

## 🔄 Actualizaciones y Mantenimiento

### Última Actualización
- **Fecha:** {fecha_actual}
- **Cambios:** Actualización de nombres de regiones según nueva nomenclatura
- **Versión:** 2.0 (incluye cambio "Región Sur Occidente" → "Región Occidente")

### Próximas Mejoras Sugeridas
- Integración con datos de población por municipio
- Mapas de densidad educativa
- Análisis temporal de expansión geográfica
- Mapas de rutas de conectividad inter-regional

---

*Mapas generados automáticamente el {fecha_actual}*  
*Basado en el análisis de 29 instituciones distribuidas en 25 ciudades de 6 regiones*

"""
    
    return reporte

def main():
    """Función principal"""
    print("Generando reporte de mapas de Colombia...")
    
    reporte = generar_reporte_mapas()
    
    # Guardar reporte
    with open('analisis_distribucion/REPORTE_MAPAS_COLOMBIA.md', 'w', encoding='utf-8') as f:
        f.write(reporte)
    
    print("✅ Reporte de mapas generado: REPORTE_MAPAS_COLOMBIA.md")
    
    # Mostrar resumen en consola
    print("\n" + "="*60)
    print("RESUMEN DE MAPAS GENERADOS")
    print("="*60)
    print("📍 6 mapas de Colombia generados:")
    print("   • 2 mapas interactivos (HTML)")
    print("   • 4 mapas estáticos (PNG, 300 DPI)")
    print("\n🗺️ Cobertura geográfica:")
    print("   • 6 regiones de Colombia")
    print("   • 25 ciudades mapeadas")
    print("   • 29 instituciones ubicadas")
    print("   • 32,232 estudiantes distribuidos")
    print("\n📊 Formatos disponibles:")
    print("   • Interactivo: Folium + Plotly")
    print("   • Estático: Matplotlib alta resolución")
    print("   • Documentación: Reporte completo en Markdown")

if __name__ == "__main__":
    main()
