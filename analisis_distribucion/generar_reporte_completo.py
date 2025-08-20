#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Generador de Reporte de Análisis de Distribución
Crea un reporte completo en formato Markdown con todas las estadísticas
"""

import pandas as pd
from datetime import datetime

def cargar_datos():
    """Cargar datos del archivo Excel"""
    return pd.read_excel('../files/programas_benchmarking.xlsx')

def generar_reporte_markdown(df):
    """Generar reporte completo en formato Markdown"""
    
    fecha_actual = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    
    reporte = f"""# Reporte de Análisis de Distribución de Programas de Benchmarking

**Fecha de generación:** {fecha_actual}
**Total de programas analizados:** {len(df)}

## 📊 Resumen Ejecutivo

- **Total de Programas:** {len(df)}
- **Instituciones Únicas:** {df['NOMBRE_INSTITUCIÓN'].nunique()}
- **Regiones Analizadas:** {df['REGION'].nunique()}
- **Municipios:** {df['MUNICIPIO_OFERTA_PROGRAMA'].nunique()}
- **Modalidades de Estudio:** {df['MODALIDAD'].nunique()}
- **Sectores:** {df['SECTOR'].nunique()}

---

## 🎯 Distribución Nacional

### Reconocimiento del Ministerio
"""
    
    # Reconocimiento del Ministerio
    reconocimiento = df['RECONOCIMIENTO_DEL_MINISTERIO'].value_counts()
    reporte += "\n| Reconocimiento | Cantidad | Porcentaje |\n"
    reporte += "|---|---|---|\n"
    for idx, (tipo, cantidad) in enumerate(reconocimiento.items()):
        porcentaje = (cantidad / len(df)) * 100
        reporte += f"| {tipo} | {cantidad} | {porcentaje:.1f}% |\n"
    
    reporte += "\n### Distribución por Sector\n"
    
    # Sector
    sector = df['SECTOR'].value_counts()
    reporte += "\n| Sector | Cantidad | Porcentaje |\n"
    reporte += "|---|---|---|\n"
    for sector_tipo, cantidad in sector.items():
        porcentaje = (cantidad / len(df)) * 100
        reporte += f"| {sector_tipo} | {cantidad} | {porcentaje:.1f}% |\n"
    
    reporte += "\n### Modalidad de Estudio\n"
    
    # Modalidad
    modalidad = df['MODALIDAD'].value_counts()
    reporte += "\n| Modalidad | Cantidad | Porcentaje |\n"
    reporte += "|---|---|---|\n"
    for modalidad_tipo, cantidad in modalidad.items():
        porcentaje = (cantidad / len(df)) * 100
        reporte += f"| {modalidad_tipo} | {cantidad} | {porcentaje:.1f}% |\n"
    
    reporte += "\n---\n\n## 🗺️ Distribución Geográfica\n\n### Por Regiones\n"
    
    # Regiones
    regiones = df['REGION'].value_counts()
    reporte += "\n| Región | Cantidad | Porcentaje |\n"
    reporte += "|---|---|---|\n"
    for region, cantidad in regiones.items():
        porcentaje = (cantidad / len(df)) * 100
        reporte += f"| {region} | {cantidad} | {porcentaje:.1f}% |\n"
    
    reporte += "\n### Top 10 Ciudades\n"
    
    # Top ciudades
    ciudades = df['MUNICIPIO_OFERTA_PROGRAMA'].value_counts().head(10)
    reporte += "\n| Ciudad | Cantidad | Porcentaje |\n"
    reporte += "|---|---|---|\n"
    for ciudad, cantidad in ciudades.items():
        porcentaje = (cantidad / len(df)) * 100
        reporte += f"| {ciudad} | {cantidad} | {porcentaje:.1f}% |\n"
    
    reporte += "\n---\n\n## 🎓 Análisis de la Región Occidente\n"
    
    # Región Occidente
    occidente = df[df['REGION'] == 'Región Occidente']
    reporte += f"\n**Total de programas en Región Occidente:** {len(occidente)} ({(len(occidente)/len(df)*100):.1f}% del total nacional)\n"
    
    reporte += "\n### Distribución por Ciudades en Región Occidente\n"
    
    if len(occidente) > 0:
        ciudades_occidente = occidente['MUNICIPIO_OFERTA_PROGRAMA'].value_counts()
        reporte += "\n| Ciudad | Cantidad | Porcentaje (del total Occidente) |\n"
        reporte += "|---|---|---|\n"
        for ciudad, cantidad in ciudades_occidente.items():
            porcentaje = (cantidad / len(occidente)) * 100
            reporte += f"| {ciudad} | {cantidad} | {porcentaje:.1f}% |\n"
        
        reporte += "\n### Modalidad en Región Occidente\n"
        modalidad_occidente = occidente['MODALIDAD'].value_counts()
        reporte += "\n| Modalidad | Cantidad | Porcentaje |\n"
        reporte += "|---|---|---|\n"
        for modalidad_tipo, cantidad in modalidad_occidente.items():
            porcentaje = (cantidad / len(occidente)) * 100
            reporte += f"| {modalidad_tipo} | {cantidad} | {porcentaje:.1f}% |\n"
        
        reporte += "\n### Sector en Región Occidente\n"
        sector_occidente = occidente['SECTOR'].value_counts()
        reporte += "\n| Sector | Cantidad | Porcentaje |\n"
        reporte += "|---|---|---|\n"
        for sector_tipo, cantidad in sector_occidente.items():
            porcentaje = (cantidad / len(occidente)) * 100
            reporte += f"| {sector_tipo} | {cantidad} | {porcentaje:.1f}% |\n"
    
    reporte += "\n---\n\n## 🏛️ Análisis Institucional\n\n### Top 10 Instituciones\n"
    
    # Top instituciones
    instituciones = df['NOMBRE_INSTITUCIÓN'].value_counts().head(10)
    reporte += "\n| Institución | Cantidad de Programas |\n"
    reporte += "|---|---|\n"
    for institucion, cantidad in instituciones.items():
        reporte += f"| {institucion} | {cantidad} |\n"
    
    reporte += "\n### Distribución de Instituciones por Región\n"
    
    # Instituciones por región
    inst_por_region = df.groupby('REGION')['NOMBRE_INSTITUCIÓN'].nunique().sort_values(ascending=False)
    reporte += "\n| Región | Número de Instituciones |\n"
    reporte += "|---|---|\n"
    for region, cantidad in inst_por_region.items():
        reporte += f"| {region} | {cantidad} |\n"
    
    reporte += "\n---\n\n## 📈 Estadísticas Adicionales\n"
    
    # Estadísticas adicionales
    if 'PERIODICIDAD' in df.columns:
        reporte += "\n### Periodicidad de Programas\n"
        periodicidad = df['PERIODICIDAD'].value_counts()
        reporte += "\n| Periodicidad | Cantidad |\n"
        reporte += "|---|---|\n"
        for periodo, cantidad in periodicidad.items():
            reporte += f"| {periodo} | {cantidad} |\n"
    
    if 'NÚMERO_CRÉDITOS' in df.columns:
        creditos = df['NÚMERO_CRÉDITOS'].dropna()
        if len(creditos) > 0:
            reporte += f"\n### Análisis de Créditos\n"
            reporte += f"- **Promedio de créditos:** {creditos.mean():.1f}\n"
            reporte += f"- **Mediana de créditos:** {creditos.median():.1f}\n"
            reporte += f"- **Mínimo de créditos:** {creditos.min()}\n"
            reporte += f"- **Máximo de créditos:** {creditos.max()}\n"
    
    reporte += "\n---\n\n## 📊 Gráficas Generadas\n\n"
    reporte += "Las siguientes gráficas han sido generadas en la carpeta `graficas/`:\n\n"
    reporte += "1. **distribucion_reconocimiento_ministerio.png** - Distribución por reconocimiento del ministerio\n"
    reporte += "2. **distribucion_modalidad.png** - Distribución por modalidad de estudio\n"
    reporte += "3. **distribucion_sector.png** - Distribución por sector (oficial/privado)\n"
    reporte += "4. **distribucion_regiones.png** - Distribución por regiones\n"
    reporte += "5. **distribucion_ciudades_occidente.png** - Distribución por ciudades en Región Occidente\n"
    reporte += "6. **distribucion_modalidad_occidente.png** - Modalidad en Región Occidente\n"
    reporte += "7. **distribucion_sector_occidente.png** - Sector en Región Occidente\n"
    reporte += "8. **resumen_completo_distribucion.png** - Resumen visual completo\n"
    reporte += "9. **dashboard_nacional_completo.png** - Dashboard nacional detallado\n"
    reporte += "10. **analisis_institucional.png** - Análisis de instituciones\n"
    reporte += "11. **matriz_distribucion_regiones.png** - Matriz de correlación regiones vs características\n"
    
    reporte += "\n---\n\n## 🔍 Conclusiones Principales\n\n"
    
    # Conclusiones automáticas
    region_dominante = regiones.index[0]
    modalidad_dominante = modalidad.index[0]
    sector_dominante = sector.index[0]
    ciudad_dominante = ciudades.index[0]
    
    reporte += f"1. **Región Dominante:** {region_dominante} concentra {regiones.iloc[0]} programas ({(regiones.iloc[0]/len(df)*100):.1f}% del total)\n"
    reporte += f"2. **Modalidad Predominante:** {modalidad_dominante} representa el {(modalidad.iloc[0]/len(df)*100):.1f}% de los programas\n"
    reporte += f"3. **Sector Mayoritario:** {sector_dominante} con {sector.iloc[0]} programas ({(sector.iloc[0]/len(df)*100):.1f}%)\n"
    reporte += f"4. **Ciudad Líder:** {ciudad_dominante} alberga {ciudades.iloc[0]} programas\n"
    reporte += f"5. **Región Occidente:** Representa el {(len(occidente)/len(df)*100):.1f}% del total nacional con {len(occidente)} programas\n"
    
    if len(occidente) > 0:
        ciudad_occ_dominante = ciudades_occidente.index[0]
        reporte += f"6. **En Región Occidente:** {ciudad_occ_dominante} lidera con {ciudades_occidente.iloc[0]} programas\n"
    
    reporte += f"\n---\n\n*Reporte generado automáticamente el {fecha_actual}*\n"
    
    return reporte

def main():
    """Función principal"""
    print("Generando reporte completo...")
    
    # Cargar datos
    df = cargar_datos()
    
    # Generar reporte
    reporte = generar_reporte_markdown(df)
    
    # Guardar reporte
    with open('REPORTE_ANALISIS_DISTRIBUCION.md', 'w', encoding='utf-8') as f:
        f.write(reporte)
    
    print("✅ Reporte completo generado: REPORTE_ANALISIS_DISTRIBUCION.md")
    print(f"📊 Analizados {len(df)} programas de {df['NOMBRE_INSTITUCIÓN'].nunique()} instituciones")

if __name__ == "__main__":
    main()
