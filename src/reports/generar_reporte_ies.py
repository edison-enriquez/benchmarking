#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Generador de Reporte de Análisis IES Santiago de Cali
Crea un reporte detallado del análisis de desempeño de instituciones
"""

import pandas as pd
from datetime import datetime

def analizar_datos_ies():
    """Analizar los datos consolidados IES y generar estadísticas"""
    df = pd.read_csv('files/datos_consolidados_ies.csv')
    
    # Limpiar nombres de instituciones
    df['institucion_corta'] = df['institucion'].str.replace('FUNDACION CENTRO COLOMBIANO DE ESTUDIOS PROFESIONALES,', 'FCCP')
    df['institucion_corta'] = df['institucion_corta'].str.replace('INSTITUCION UNIVERSITARIA ANTONIO JOSE CAMACHO', 'UNIAJC')
    df['institucion_corta'] = df['institucion_corta'].str.replace('UNIVERSIDAD DEL VALLE', 'UNIVALLE')
    df['institucion_corta'] = df['institucion_corta'].str.replace('FUNDACION TECNOLOGICA AUTONOMA DEL PACIFICO', 'FATAP')
    
    return df

def generar_reporte_ies():
    """Generar reporte completo del análisis IES"""
    fecha_actual = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    df = analizar_datos_ies()
    
    # Calcular estadísticas
    ranking = df.groupby('institucion_corta')['valor_institucion'].agg(['mean', 'std', 'min', 'max', 'count']).round(2)
    ranking = ranking.sort_values('mean', ascending=False)
    
    mejor_año = df.groupby('año')['valor_institucion'].mean().idxmax()
    peor_año = df.groupby('año')['valor_institucion'].mean().idxmin()
    mejor_valor = df.groupby('año')['valor_institucion'].mean().max()
    peor_valor = df.groupby('año')['valor_institucion'].mean().min()
    
    reporte = f"""# Análisis de Desempeño Institucional - Santiago de Cali (2008-2022)

**Fecha de análisis:** {fecha_actual}
**Fuente:** datos_consolidados_ies.csv

## 📊 Resumen Ejecutivo

Este análisis examina el desempeño de **4 instituciones de educación superior** en Santiago de Cali durante un período de **15 años (2008-2022)**. Los datos incluyen valores de desempeño institucional y comparaciones con el promedio nacional.

### Datos Analizados:
- **Período:** 2008-2022 ({df['año'].nunique()} años)
- **Instituciones:** {df['institucion'].nunique()} instituciones
- **Registros totales:** {len(df)}
- **Ciudad:** Santiago de Cali
- **Rango de valores:** {df['valor_institucion'].min():.1f} - {df['valor_institucion'].max():.1f}

---

## 🏆 Ranking Histórico de Instituciones

| Posición | Institución | Promedio | Desv. Estándar | Mínimo | Máximo | Años con Datos |
|---|---|---|---|---|---|---|
"""
    
    for i, (inst, datos) in enumerate(ranking.iterrows(), 1):
        reporte += f"| {i} | **{inst}** | {datos['mean']:.1f} | {datos['std']:.1f} | {datos['min']:.1f} | {datos['max']:.1f} | {int(datos['count'])} |\n"
    
    reporte += "\n---\n\n## 📈 Análisis Detallado por Institución\n"
    
    # Análisis por institución
    for i, (inst, datos) in enumerate(ranking.iterrows(), 1):
        # Datos específicos de la institución
        datos_inst = df[df['institucion_corta'] == inst]
        nombre_completo = datos_inst['institucion'].iloc[0]
        
        # Calcular años sobre el promedio nacional
        sobre_promedio = len(datos_inst[datos_inst['valor_institucion'] > datos_inst['total_nacional']])
        total_años = len(datos_inst)
        porcentaje_sobre = (sobre_promedio / total_años) * 100
        
        # Calcular tendencia
        if len(datos_inst) > 1:
            import numpy as np
            x = np.arange(len(datos_inst.sort_values('año')))
            y = datos_inst.sort_values('año')['valor_institucion'].values
            tendencia = np.polyfit(x, y, 1)[0]
            tendencia_texto = f"+{tendencia:.2f}/año" if tendencia > 0 else f"{tendencia:.2f}/año"
        else:
            tendencia_texto = "N/A"
        
        # Mejor y peor año de la institución
        mejor_año_inst = datos_inst.loc[datos_inst['valor_institucion'].idxmax(), 'año']
        peor_año_inst = datos_inst.loc[datos_inst['valor_institucion'].idxmin(), 'año']
        mejor_valor_inst = datos_inst['valor_institucion'].max()
        peor_valor_inst = datos_inst['valor_institucion'].min()
        
        reporte += f"\n### {i}. {inst}\n"
        reporte += f"**Nombre completo:** {nombre_completo}\n\n"
        
        reporte += f"**Estadísticas de Desempeño:**\n"
        reporte += f"- Promedio histórico: **{datos['mean']:.1f}**\n"
        reporte += f"- Desviación estándar: {datos['std']:.1f}\n"
        reporte += f"- Valor más alto: {datos['max']:.1f} (año {mejor_año_inst})\n"
        reporte += f"- Valor más bajo: {datos['min']:.1f} (año {peor_año_inst})\n"
        reporte += f"- Tendencia: {tendencia_texto}\n"
        
        reporte += f"\n**Comparación con Promedio Nacional:**\n"
        reporte += f"- Años sobre el promedio: {sobre_promedio}/{total_años} ({porcentaje_sobre:.1f}%)\n"
        
        if porcentaje_sobre >= 70:
            desempeño = "🏆 **Excelente** - Consistentemente superior al promedio nacional"
        elif porcentaje_sobre >= 50:
            desempeño = "✅ **Bueno** - Mayormente sobre el promedio nacional"
        elif porcentaje_sobre >= 30:
            desempeño = "⚠️ **Regular** - Parcialmente sobre el promedio nacional"
        else:
            desempeño = "🔴 **Bajo** - Principalmente bajo el promedio nacional"
        
        reporte += f"- Evaluación: {desempeño}\n"
        
        # Evolución temporal
        reporte += f"\n**Evolución Temporal:**\n"
        periodos = {
            "2008-2012": datos_inst[datos_inst['año'].between(2008, 2012)]['valor_institucion'].mean(),
            "2013-2017": datos_inst[datos_inst['año'].between(2013, 2017)]['valor_institucion'].mean(),
            "2018-2022": datos_inst[datos_inst['año'].between(2018, 2022)]['valor_institucion'].mean()
        }
        
        for periodo, promedio in periodos.items():
            if not pd.isna(promedio):
                reporte += f"- {periodo}: {promedio:.1f}\n"
        
        reporte += "\n---\n"
    
    reporte += "\n## 📅 Análisis Temporal Nacional\n\n"
    
    reporte += f"### Evolución del Promedio Nacional\n"
    reporte += f"- **Mejor año:** {mejor_año} (promedio: {mejor_valor:.1f})\n"
    reporte += f"- **Peor año:** {peor_año} (promedio: {peor_valor:.1f})\n"
    reporte += f"- **Diferencia:** {mejor_valor - peor_valor:.1f} puntos\n"
    
    # Análisis por períodos del promedio nacional
    promedio_2008_2012 = df[df['año'].between(2008, 2012)].groupby('año')['total_nacional'].first().mean()
    promedio_2013_2017 = df[df['año'].between(2013, 2017)].groupby('año')['total_nacional'].first().mean()
    promedio_2018_2022 = df[df['año'].between(2018, 2022)].groupby('año')['total_nacional'].first().mean()
    
    reporte += f"\n### Evolución por Períodos (Promedio Nacional)\n"
    reporte += f"- **2008-2012:** {promedio_2008_2012:.1f}\n"
    reporte += f"- **2013-2017:** {promedio_2013_2017:.1f}\n"
    reporte += f"- **2018-2022:** {promedio_2018_2022:.1f}\n"
    
    # Determinar tendencia general
    if promedio_2018_2022 > promedio_2008_2012:
        tendencia_general = f"📈 **Tendencia positiva:** Mejora de {promedio_2018_2022 - promedio_2008_2012:.1f} puntos en el período"
    else:
        tendencia_general = f"📉 **Tendencia negativa:** Reducción de {promedio_2008_2012 - promedio_2018_2022:.1f} puntos en el período"
    
    reporte += f"- **Tendencia general:** {tendencia_general}\n"
    
    reporte += "\n---\n\n## 📊 Análisis Estadístico Comparativo\n\n"
    
    # Distribución de desempeño
    reporte += f"### Distribución de Desempeño\n"
    reporte += f"- **Promedio general:** {df['valor_institucion'].mean():.1f}\n"
    reporte += f"- **Mediana:** {df['valor_institucion'].median():.1f}\n"
    reporte += f"- **Desviación estándar:** {df['valor_institucion'].std():.1f}\n"
    reporte += f"- **Coeficiente de variación:** {(df['valor_institucion'].std() / df['valor_institucion'].mean() * 100):.1f}%\n"
    
    # Análisis de volatilidad
    reporte += f"\n### Volatilidad por Institución\n"
    reporte += "| Institución | Volatilidad (%) | Interpretación |\n"
    reporte += "|---|---|---|\n"
    
    for inst in df['institucion_corta'].unique():
        datos_inst = df[df['institucion_corta'] == inst]['valor_institucion']
        if len(datos_inst) > 1:
            cambios = datos_inst.pct_change().dropna()
            volatilidad = cambios.std() * 100 if len(cambios) > 0 else 0
            
            if volatilidad < 5:
                interpretacion = "Muy estable"
            elif volatilidad < 10:
                interpretacion = "Estable"
            elif volatilidad < 20:
                interpretacion = "Moderadamente volátil"
            else:
                interpretacion = "Altamente volátil"
            
            reporte += f"| {inst} | {volatilidad:.1f}% | {interpretacion} |\n"
    
    reporte += "\n---\n\n## 📈 Visualizaciones Generadas\n\n"
    reporte += "Se han generado 4 visualizaciones completas en la carpeta `analisis_distribucion/graficas/`:\n\n"
    
    reporte += "### 1. **analisis_temporal_ies.png** - Análisis Temporal\n"
    reporte += "- Evolución del desempeño por institución (2008-2022)\n"
    reporte += "- Comparación con el promedio nacional\n"
    reporte += "- Distribución de valores (boxplots)\n"
    reporte += "- Heatmap de desempeño por año e institución\n\n"
    
    reporte += "### 2. **analisis_estadistico_ies.png** - Análisis Estadístico\n"
    reporte += "- Ranking promedio de instituciones\n"
    reporte += "- Evolución de la brecha con el promedio nacional\n"
    reporte += "- Tendencias de crecimiento/decrecimiento\n"
    reporte += "- Análisis de variabilidad\n\n"
    
    reporte += "### 3. **analisis_periodos_ies.png** - Análisis por Períodos\n"
    reporte += "- Comparación de promedios por períodos quinquenales\n"
    reporte += "- Evolución del promedio nacional\n"
    reporte += "- Instituciones sobre/bajo el promedio por año\n"
    reporte += "- Análisis de volatilidad\n\n"
    
    reporte += "### 4. **dashboard_completo_ies.png** - Dashboard Integral\n"
    reporte += "- Evolución temporal principal\n"
    reporte += "- Ranking actual\n"
    reporte += "- Distribución de promedios históricos\n"
    reporte += "- Tendencias y variabilidad\n"
    reporte += "- Estadísticas generales\n"
    reporte += "- Heatmap histórico completo\n\n"
    
    reporte += "---\n\n## 🔍 Conclusiones Principales\n\n"
    
    # Identificar líder
    lider = ranking.index[0]
    mejor_promedio = ranking.iloc[0]['mean']
    
    reporte += f"### Desempeño Institucional\n"
    reporte += f"1. **Institución Líder:** {lider} con un promedio histórico de {mejor_promedio:.1f}\n"
    reporte += f"2. **Mayor Variabilidad:** {ranking.sort_values('std', ascending=False).index[0]} (σ = {ranking['std'].max():.1f})\n"
    reporte += f"3. **Más Estable:** {ranking.sort_values('std').index[0]} (σ = {ranking['std'].min():.1f})\n"
    
    # Análisis de años críticos
    reporte += f"\n### Años Críticos\n"
    reporte += f"4. **Año de Mayor Rendimiento:** {mejor_año} (promedio general: {mejor_valor:.1f})\n"
    reporte += f"5. **Año de Menor Rendimiento:** {peor_año} (promedio general: {peor_valor:.1f})\n"
    
    # Instituciones sobre promedio nacional
    instituciones_superiores = []
    for inst in df['institucion_corta'].unique():
        datos_inst = df[df['institucion_corta'] == inst]
        sobre_promedio = len(datos_inst[datos_inst['valor_institucion'] > datos_inst['total_nacional']])
        total_años = len(datos_inst)
        porcentaje = (sobre_promedio / total_años) * 100
        if porcentaje >= 50:
            instituciones_superiores.append(f"{inst} ({porcentaje:.0f}%)")
    
    reporte += f"\n### Comparación Nacional\n"
    if instituciones_superiores:
        reporte += f"6. **Instituciones consistentemente superiores al promedio nacional:** {', '.join(instituciones_superiores)}\n"
    else:
        reporte += f"6. **Nota:** Pocas instituciones mantienen desempeño consistente sobre el promedio nacional\n"
    
    reporte += f"7. **Rango de variación:** {df['valor_institucion'].max() - df['valor_institucion'].min():.1f} puntos entre el mejor y peor desempeño\n"
    
    # Tendencia general
    primer_año_promedio = df[df['año'] == df['año'].min()]['valor_institucion'].mean()
    ultimo_año_promedio = df[df['año'] == df['año'].max()]['valor_institucion'].mean()
    cambio_total = ultimo_año_promedio - primer_año_promedio
    
    if cambio_total > 0:
        reporte += f"8. **Tendencia General:** Mejora de {cambio_total:.1f} puntos desde {df['año'].min()} hasta {df['año'].max()}\n"
    else:
        reporte += f"8. **Tendencia General:** Reducción de {abs(cambio_total):.1f} puntos desde {df['año'].min()} hasta {df['año'].max()}\n"
    
    reporte += f"\n---\n\n## 🎯 Recomendaciones\n\n"
    
    # Generar recomendaciones basadas en el análisis
    reporte += f"### Para las Instituciones\n"
    
    # Para la institución líder
    reporte += f"- **{lider}:** Mantener estándares de excelencia y compartir mejores prácticas\n"
    
    # Para instituciones con bajo desempeño
    instituciones_bajo_promedio = ranking[ranking['mean'] < df['valor_institucion'].mean()].index.tolist()
    if instituciones_bajo_promedio:
        reporte += f"- **Instituciones con oportunidades de mejora:** Implementar estrategias de mejoramiento basadas en las prácticas exitosas\n"
    
    # Recomendaciones generales
    reporte += f"\n### Generales\n"
    reporte += f"- Establecer programas de benchmarking entre instituciones\n"
    reporte += f"- Implementar sistemas de monitoreo continuo del desempeño\n"
    reporte += f"- Desarrollar estrategias específicas para los años de bajo rendimiento identificados\n"
    reporte += f"- Fortalecer los mecanismos de mejora continua institucional\n"
    
    reporte += f"\n---\n\n*Reporte generado automáticamente el {fecha_actual}*\n"
    reporte += f"*Basado en el análisis de {len(df)} registros de {df['institucion'].nunique()} instituciones de Santiago de Cali*\n"
    
    return reporte

def main():
    """Función principal"""
    print("Generando reporte de análisis IES Santiago de Cali...")
    
    reporte = generar_reporte_ies()
    
    # Guardar reporte
    with open('analisis_distribucion/REPORTE_ANALISIS_IES.md', 'w', encoding='utf-8') as f:
        f.write(reporte)
    
    print("✅ Reporte de análisis IES generado: REPORTE_ANALISIS_IES.md")

if __name__ == "__main__":
    main()
