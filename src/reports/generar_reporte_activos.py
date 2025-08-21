#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Generador de Reporte de Programas Activos por Región
Crea un reporte detallado en formato Markdown del análisis de programas activos
"""

import pandas as pd
from datetime import datetime

def analizar_programas_activos():
    """Analizar el archivo de programas activos y generar estadísticas"""
    df = pd.read_excel('resultados/Consolidado_Programas_ACTIVOS.xlsx')
    
    # Identificar regiones y procesar datos
    regiones_data = {}
    current_region = None
    
    for idx, row in df.iterrows():
        codigo = str(row['CODIGO_SNIES_PROGRAMA'])
        
        if 'REGIÓN' in codigo:
            current_region = codigo.replace('REGIÓN: ', '')
            regiones_data[current_region] = []
        elif current_region and not pd.isna(row['CODIGO_SNIES_PROGRAMA']) and codigo != 'nan':
            regiones_data[current_region].append(row)
    
    # Procesar estadísticas por región
    resultados_region = {}
    periodos = ['2018_1', '2018_2', '2019_1', '2019_2', '2020_1', '2020_2', 
                '2021_1', '2021_2', '2022_1', '2022_2', '2023_1', '2023_2', 
                '2024_1', '2024_2']
    
    for region, programas in regiones_data.items():
        if not programas:
            continue
            
        df_region = pd.DataFrame(programas)
        
        # Calcular estadísticas
        sumas_periodo = {}
        for periodo in periodos:
            if periodo in df_region.columns:
                valores = pd.to_numeric(df_region[periodo], errors='coerce').fillna(0)
                sumas_periodo[periodo] = valores.sum()
        
        # Calcular tendencias
        primera_mitad = sum(sumas_periodo.get(p, 0) for p in periodos[:8])  # 2018-2021
        segunda_mitad = sum(sumas_periodo.get(p, 0) for p in periodos[8:])  # 2022-2024
        
        resultados_region[region] = {
            'num_programas': len(programas),
            'num_instituciones': df_region['INSTITUCION_EDUCACION_SUPERIOR'].nunique(),
            'sumas_periodo': sumas_periodo,
            'total_general': sum(sumas_periodo.values()),
            'primera_mitad': primera_mitad,
            'segunda_mitad': segunda_mitad,
            'instituciones': df_region['INSTITUCION_EDUCACION_SUPERIOR'].unique().tolist()
        }
    
    return resultados_region

def generar_reporte_programas_activos():
    """Generar reporte completo de programas activos"""
    fecha_actual = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    resultados_region = analizar_programas_activos()
    
    # Calcular totales generales
    total_programas = sum(r['num_programas'] for r in resultados_region.values())
    total_instituciones = sum(r['num_instituciones'] for r in resultados_region.values())
    total_estudiantes = sum(r['total_general'] for r in resultados_region.values())
    
    reporte = f"""# Análisis de Programas Activos por Región (2018-2024)

**Fecha de análisis:** {fecha_actual}
**Fuente:** Consolidado_Programas_ACTIVOS.xlsx

## 📊 Resumen Ejecutivo

- **Total de Regiones Analizadas:** {len(resultados_region)}
- **Total de Programas Activos:** {total_programas}
- **Total de Instituciones:** {total_instituciones}
- **Total de Estudiantes (Período 2018-2024):** {int(total_estudiantes):,}

---

## 🎯 Ranking de Regiones por Volumen de Estudiantes

"""
    
    # Ordenar regiones por total de estudiantes
    regiones_ordenadas = sorted(resultados_region.items(), 
                               key=lambda x: x[1]['total_general'], reverse=True)
    
    reporte += "| Posición | Región | Programas | Instituciones | Total Estudiantes | % del Total | Promedio/Programa |\n"
    reporte += "|---|---|---|---|---|---|---|\n"
    
    for i, (region, datos) in enumerate(regiones_ordenadas, 1):
        porcentaje = (datos['total_general'] / total_estudiantes) * 100
        promedio_programa = datos['total_general'] / datos['num_programas']
        
        reporte += f"| {i} | {region} | {datos['num_programas']} | {datos['num_instituciones']} | {int(datos['total_general']):,} | {porcentaje:.1f}% | {promedio_programa:.0f} |\n"
    
    reporte += "\n---\n\n## 📈 Análisis Detallado por Región\n"
    
    for i, (region, datos) in enumerate(regiones_ordenadas, 1):
        reporte += f"\n### {i}. {region}\n\n"
        
        # Estadísticas básicas
        reporte += f"**Estadísticas Generales:**\n"
        reporte += f"- Programas activos: {datos['num_programas']}\n"
        reporte += f"- Instituciones participantes: {datos['num_instituciones']}\n"
        reporte += f"- Total de estudiantes: {int(datos['total_general']):,}\n"
        reporte += f"- Promedio estudiantes por programa: {datos['total_general']/datos['num_programas']:.1f}\n"
        
        # Análisis temporal
        cambio_porcentual = ((datos['segunda_mitad'] - datos['primera_mitad']) / datos['primera_mitad'] * 100) if datos['primera_mitad'] > 0 else 0
        reporte += f"- Estudiantes 2018-2021: {int(datos['primera_mitad']):,}\n"
        reporte += f"- Estudiantes 2022-2024: {int(datos['segunda_mitad']):,}\n"
        reporte += f"- Cambio porcentual: {cambio_porcentual:+.1f}%\n"
        
        # Instituciones participantes
        reporte += f"\n**Instituciones en {region}:**\n"
        for inst in sorted(datos['instituciones']):
            reporte += f"- {inst}\n"
        
        # Tendencia por períodos
        reporte += f"\n**Evolución por Períodos:**\n"
        reporte += "| Período | Estudiantes |\n"
        reporte += "|---|---|\n"
        for periodo, valor in datos['sumas_periodo'].items():
            if valor > 0:  # Solo mostrar períodos con datos
                año = periodo[:4]
                semestre = "I" if periodo.endswith('_1') else "II"
                reporte += f"| {año} - {semestre} | {int(valor):,} |\n"
        
        reporte += "\n---\n"
    
    reporte += "\n## 📊 Análisis Comparativo\n\n"
    
    # Análisis de concentración
    top_3_estudiantes = sum(datos['total_general'] for _, datos in regiones_ordenadas[:3])
    concentracion_top3 = (top_3_estudiantes / total_estudiantes) * 100
    
    reporte += f"### Concentración Regional\n"
    reporte += f"- Las **3 regiones principales** concentran **{concentracion_top3:.1f}%** del total de estudiantes\n"
    reporte += f"- **{regiones_ordenadas[0][0]}** lidera con **{(regiones_ordenadas[0][1]['total_general']/total_estudiantes*100):.1f}%** del total nacional\n"
    
    # Análisis de eficiencia (estudiantes por programa)
    eficiencias = [(region, datos['total_general']/datos['num_programas']) for region, datos in resultados_region.items()]
    eficiencias.sort(key=lambda x: x[1], reverse=True)
    
    reporte += f"\n### Eficiencia por Programa (Estudiantes/Programa)\n"
    reporte += "| Región | Promedio Estudiantes/Programa |\n"
    reporte += "|---|---|\n"
    for region, eficiencia in eficiencias:
        reporte += f"| {region} | {eficiencia:.0f} |\n"
    
    # Análisis temporal general
    total_primera_mitad = sum(datos['primera_mitad'] for datos in resultados_region.values())
    total_segunda_mitad = sum(datos['segunda_mitad'] for datos in resultados_region.values())
    cambio_general = ((total_segunda_mitad - total_primera_mitad) / total_primera_mitad * 100) if total_primera_mitad > 0 else 0
    
    reporte += f"\n### Tendencia Temporal Nacional\n"
    reporte += f"- **2018-2021:** {int(total_primera_mitad):,} estudiantes\n"
    reporte += f"- **2022-2024:** {int(total_segunda_mitad):,} estudiantes\n"
    reporte += f"- **Cambio porcentual:** {cambio_general:+.1f}%\n"
    
    if cambio_general > 0:
        reporte += f"- ✅ **Tendencia positiva:** Crecimiento del {cambio_general:.1f}% en la segunda mitad del período\n"
    else:
        reporte += f"- ⚠️ **Tendencia negativa:** Reducción del {abs(cambio_general):.1f}% en la segunda mitad del período\n"
    
    reporte += "\n---\n\n## 📈 Visualizaciones Generadas\n\n"
    reporte += "Se han generado las siguientes gráficas en la carpeta `analisis_distribucion/graficas/`:\n\n"
    reporte += "1. **programas_activos_por_region.png** - Resumen general por región\n"
    reporte += "   - Número de programas por región\n"
    reporte += "   - Número de instituciones por región\n"
    reporte += "   - Total de estudiantes por región\n"
    reporte += "   - Distribución porcentual de programas\n\n"
    reporte += "2. **evolucion_temporal_regiones.png** - Evolución temporal 2018-2024\n"
    reporte += "   - Líneas de tendencia por región\n"
    reporte += "   - Mapa de calor por período y región\n\n"
    reporte += "3. **analisis_comparativo_regiones.png** - Análisis comparativo\n"
    reporte += "   - Ranking por total de estudiantes\n"
    reporte += "   - Promedio de estudiantes por programa\n"
    reporte += "   - Comparación primera vs segunda mitad del período\n"
    reporte += "   - Ranking final de regiones\n\n"
    
    reporte += "---\n\n## 🔍 Conclusiones Principales\n\n"
    
    region_lider = regiones_ordenadas[0][0]
    programa_mas_eficiente = eficiencias[0][0]
    
    reporte += f"1. **Región Líder:** {region_lider} domina con {regiones_ordenadas[0][1]['total_general']/total_estudiantes*100:.1f}% del total de estudiantes\n\n"
    reporte += f"2. **Mayor Eficiencia:** {programa_mas_eficiente} tiene el mayor promedio de estudiantes por programa ({eficiencias[0][1]:.0f})\n\n"
    reporte += f"3. **Concentración:** Las 3 regiones principales concentran {concentracion_top3:.1f}% del total de estudiantes\n\n"
    
    if cambio_general > 0:
        reporte += f"4. **Crecimiento:** El sistema muestra una tendencia positiva con {cambio_general:.1f}% de crecimiento\n\n"
    else:
        reporte += f"4. **Tendencia:** El sistema presenta una reducción del {abs(cambio_general):.1f}% en el período reciente\n\n"
    
    # Identificar regiones con mayor crecimiento
    crecimientos = [(region, ((datos['segunda_mitad'] - datos['primera_mitad']) / datos['primera_mitad'] * 100) if datos['primera_mitad'] > 0 else 0) 
                   for region, datos in resultados_region.items()]
    crecimientos.sort(key=lambda x: x[1], reverse=True)
    
    if crecimientos[0][1] > 0:
        reporte += f"5. **Mayor Crecimiento:** {crecimientos[0][0]} lidera el crecimiento con {crecimientos[0][1]:+.1f}%\n\n"
    
    reporte += f"6. **Diversidad Institucional:** {total_instituciones} instituciones participan en {total_programas} programas activos\n\n"
    
    reporte += f"\n---\n\n*Reporte generado automáticamente el {fecha_actual}*\n"
    reporte += f"*Basado en el análisis de {total_programas} programas activos de {total_instituciones} instituciones*\n"
    
    return reporte

def main():
    """Función principal"""
    print("Generando reporte de programas activos...")
    
    reporte = generar_reporte_programas_activos()
    
    # Guardar reporte
    with open('analisis_distribucion/REPORTE_PROGRAMAS_ACTIVOS.md', 'w', encoding='utf-8') as f:
        f.write(reporte)
    
    print("✅ Reporte de programas activos generado: REPORTE_PROGRAMAS_ACTIVOS.md")

if __name__ == "__main__":
    main()
