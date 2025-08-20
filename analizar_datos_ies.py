#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Análisis de Datos Consolidados IES - Santiago de Cali
Visualizaciones del desempeño de instituciones de educación superior 2008-2022
"""

import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np
from datetime import datetime
import os

# Configuración de estilo
plt.style.use('default')
sns.set_palette("husl")
plt.rcParams['figure.figsize'] = (14, 10)
plt.rcParams['font.size'] = 10
plt.rcParams['axes.titlesize'] = 14
plt.rcParams['axes.labelsize'] = 12

def cargar_datos():
    """Cargar y preparar los datos consolidados IES"""
    df = pd.read_csv('files/datos_consolidados_ies.csv')
    
    # Limpiar nombres de instituciones
    df['institucion_corta'] = df['institucion'].str.replace('FUNDACION CENTRO COLOMBIANO DE ESTUDIOS PROFESIONALES,', 'FCCP')
    df['institucion_corta'] = df['institucion_corta'].str.replace('INSTITUCION UNIVERSITARIA ANTONIO JOSE CAMACHO', 'UNIAJC')
    df['institucion_corta'] = df['institucion_corta'].str.replace('UNIVERSIDAD DEL VALLE', 'UNIVALLE')
    df['institucion_corta'] = df['institucion_corta'].str.replace('FUNDACION TECNOLOGICA AUTONOMA DEL PACIFICO', 'FATAP')
    
    return df

def crear_analisis_temporal(df):
    """Crear análisis temporal del desempeño de las instituciones"""
    fig, ((ax1, ax2), (ax3, ax4)) = plt.subplots(2, 2, figsize=(16, 12))
    
    # 1. Evolución temporal por institución
    for institucion in df['institucion_corta'].unique():
        datos_inst = df[df['institucion_corta'] == institucion]
        ax1.plot(datos_inst['año'], datos_inst['valor_institucion'], 
                marker='o', linewidth=2, label=institucion, markersize=6)
    
    ax1.set_title('Evolución del Desempeño por Institución (2008-2022)', fontweight='bold')
    ax1.set_xlabel('Año')
    ax1.set_ylabel('Valor de Desempeño')
    ax1.legend()
    ax1.grid(True, alpha=0.3)
    ax1.set_xticks(range(2008, 2023, 2))
    
    # 2. Comparación con el promedio nacional
    ax2.plot(df['año'].unique(), df.groupby('año')['total_nacional'].first(), 
            'r-', linewidth=3, label='Promedio Nacional', marker='s', markersize=8)
    
    for institucion in df['institucion_corta'].unique():
        datos_inst = df[df['institucion_corta'] == institucion]
        ax2.plot(datos_inst['año'], datos_inst['valor_institucion'], 
                marker='o', linewidth=2, label=institucion, alpha=0.8)
    
    ax2.set_title('Comparación con el Promedio Nacional', fontweight='bold')
    ax2.set_xlabel('Año')
    ax2.set_ylabel('Valor de Desempeño')
    ax2.legend()
    ax2.grid(True, alpha=0.3)
    ax2.set_xticks(range(2008, 2023, 2))
    
    # 3. Boxplot de distribución de valores por institución
    data_boxplot = []
    labels_boxplot = []
    for institucion in df['institucion_corta'].unique():
        datos_inst = df[df['institucion_corta'] == institucion]['valor_institucion']
        data_boxplot.append(datos_inst)
        labels_boxplot.append(institucion)
    
    ax3.boxplot(data_boxplot, labels=labels_boxplot)
    ax3.set_title('Distribución de Valores por Institución', fontweight='bold')
    ax3.set_ylabel('Valor de Desempeño')
    ax3.tick_params(axis='x', rotation=45)
    
    # 4. Heatmap de desempeño por año e institución
    pivot_data = df.pivot(index='institucion_corta', columns='año', values='valor_institucion')
    im = ax4.imshow(pivot_data.values, cmap='RdYlGn', aspect='auto')
    ax4.set_xticks(range(len(pivot_data.columns)))
    ax4.set_xticklabels(pivot_data.columns, rotation=45)
    ax4.set_yticks(range(len(pivot_data.index)))
    ax4.set_yticklabels(pivot_data.index)
    ax4.set_title('Mapa de Calor: Desempeño por Año e Institución', fontweight='bold')
    
    # Agregar valores en el heatmap
    for i in range(len(pivot_data.index)):
        for j in range(len(pivot_data.columns)):
            value = pivot_data.iloc[i, j]
            if not pd.isna(value):
                color = 'white' if value < 70 else 'black'
                ax4.text(j, i, f'{value:.1f}', ha='center', va='center', 
                        color=color, fontweight='bold', fontsize=8)
    
    # Colorbar
    plt.colorbar(im, ax=ax4, label='Valor de Desempeño')
    
    plt.suptitle('Análisis Temporal - Instituciones de Educación Superior Santiago de Cali', 
                 fontsize=16, fontweight='bold')
    plt.tight_layout()
    plt.savefig('analisis_distribucion/graficas/analisis_temporal_ies.png', 
                dpi=300, bbox_inches='tight')
    plt.close()
    print("Gráfica guardada: analisis_temporal_ies.png")

def crear_analisis_estadistico(df):
    """Crear análisis estadístico detallado"""
    fig, ((ax1, ax2), (ax3, ax4)) = plt.subplots(2, 2, figsize=(16, 12))
    
    # 1. Ranking promedio de instituciones
    ranking = df.groupby('institucion_corta')['valor_institucion'].agg(['mean', 'std', 'min', 'max']).round(2)
    ranking = ranking.sort_values('mean', ascending=False)
    
    bars = ax1.bar(range(len(ranking)), ranking['mean'], 
                   yerr=ranking['std'], capsize=5, color='skyblue')
    ax1.set_xticks(range(len(ranking)))
    ax1.set_xticklabels(ranking.index, rotation=45, ha='right')
    ax1.set_title('Ranking Promedio de Instituciones (2008-2022)', fontweight='bold')
    ax1.set_ylabel('Valor Promedio de Desempeño')
    
    # Agregar valores en las barras
    for i, (bar, value) in enumerate(zip(bars, ranking['mean'])):
        ax1.text(bar.get_x() + bar.get_width()/2., bar.get_height() + 1,
                f'{value:.1f}', ha='center', va='bottom', fontweight='bold')
    
    # 2. Evolución de la brecha con el promedio nacional
    brecha_data = []
    años = sorted(df['año'].unique())
    
    for año in años:
        datos_año = df[df['año'] == año]
        promedio_nacional = datos_año['total_nacional'].iloc[0]
        
        for institucion in df['institucion_corta'].unique():
            valor_inst = datos_año[datos_año['institucion_corta'] == institucion]['valor_institucion']
            if not valor_inst.empty:
                brecha = valor_inst.iloc[0] - promedio_nacional
                brecha_data.append({'año': año, 'institucion': institucion, 'brecha': brecha})
    
    df_brecha = pd.DataFrame(brecha_data)
    
    for institucion in df['institucion_corta'].unique():
        datos_inst = df_brecha[df_brecha['institucion'] == institucion]
        ax2.plot(datos_inst['año'], datos_inst['brecha'], 
                marker='o', linewidth=2, label=institucion)
    
    ax2.axhline(y=0, color='red', linestyle='--', alpha=0.7, label='Promedio Nacional')
    ax2.set_title('Brecha con el Promedio Nacional', fontweight='bold')
    ax2.set_xlabel('Año')
    ax2.set_ylabel('Diferencia con Promedio Nacional')
    ax2.legend()
    ax2.grid(True, alpha=0.3)
    
    # 3. Tendencias (crecimiento/decrecimiento)
    tendencias = {}
    for institucion in df['institucion_corta'].unique():
        datos_inst = df[df['institucion_corta'] == institucion].sort_values('año')
        if len(datos_inst) > 1:
            # Calcular pendiente de la tendencia
            x = np.arange(len(datos_inst))
            y = datos_inst['valor_institucion'].values
            pendiente = np.polyfit(x, y, 1)[0]
            tendencias[institucion] = pendiente
    
    colores = ['green' if v > 0 else 'red' for v in tendencias.values()]
    bars = ax3.bar(range(len(tendencias)), list(tendencias.values()), color=colores)
    ax3.set_xticks(range(len(tendencias)))
    ax3.set_xticklabels(list(tendencias.keys()), rotation=45, ha='right')
    ax3.set_title('Tendencia de Crecimiento (2008-2022)', fontweight='bold')
    ax3.set_ylabel('Pendiente de Tendencia')
    ax3.axhline(y=0, color='black', linestyle='-', alpha=0.3)
    
    # Agregar valores
    for bar, value in zip(bars, tendencias.values()):
        ax3.text(bar.get_x() + bar.get_width()/2., 
                bar.get_height() + (0.05 if value > 0 else -0.15),
                f'{value:.2f}', ha='center', va='bottom' if value > 0 else 'top', 
                fontweight='bold')
    
    # 4. Comparación de variabilidad
    variabilidad = df.groupby('institucion_corta')['valor_institucion'].std().sort_values()
    
    bars = ax4.bar(range(len(variabilidad)), variabilidad.values, color='orange')
    ax4.set_xticks(range(len(variabilidad)))
    ax4.set_xticklabels(variabilidad.index, rotation=45, ha='right')
    ax4.set_title('Variabilidad del Desempeño (Desviación Estándar)', fontweight='bold')
    ax4.set_ylabel('Desviación Estándar')
    
    for bar, value in zip(bars, variabilidad.values):
        ax4.text(bar.get_x() + bar.get_width()/2., bar.get_height() + 0.2,
                f'{value:.1f}', ha='center', va='bottom', fontweight='bold')
    
    plt.suptitle('Análisis Estadístico - Instituciones de Educación Superior', 
                 fontsize=16, fontweight='bold')
    plt.tight_layout()
    plt.savefig('analisis_distribucion/graficas/analisis_estadistico_ies.png', 
                dpi=300, bbox_inches='tight')
    plt.close()
    print("Gráfica guardada: analisis_estadistico_ies.png")

def crear_analisis_periodos(df):
    """Crear análisis por períodos específicos"""
    fig, ((ax1, ax2), (ax3, ax4)) = plt.subplots(2, 2, figsize=(16, 12))
    
    # Definir períodos
    periodo_1 = df[df['año'].between(2008, 2012)]  # 2008-2012
    periodo_2 = df[df['año'].between(2013, 2017)]  # 2013-2017
    periodo_3 = df[df['año'].between(2018, 2022)]  # 2018-2022
    
    # 1. Comparación de promedios por período
    periodos_data = []
    for periodo, nombre in [(periodo_1, '2008-2012'), (periodo_2, '2013-2017'), (periodo_3, '2018-2022')]:
        for institucion in df['institucion_corta'].unique():
            datos_inst = periodo[periodo['institucion_corta'] == institucion]
            if not datos_inst.empty:
                promedio = datos_inst['valor_institucion'].mean()
                periodos_data.append({'periodo': nombre, 'institucion': institucion, 'promedio': promedio})
    
    df_periodos = pd.DataFrame(periodos_data)
    pivot_periodos = df_periodos.pivot(index='institucion', columns='periodo', values='promedio')
    
    pivot_periodos.plot(kind='bar', ax=ax1, width=0.8)
    ax1.set_title('Promedio de Desempeño por Períodos', fontweight='bold')
    ax1.set_ylabel('Valor Promedio')
    ax1.legend(title='Período')
    ax1.tick_params(axis='x', rotation=45)
    
    # 2. Evolución del promedio nacional
    promedio_nacional_años = df.groupby('año')['total_nacional'].first()
    ax2.plot(promedio_nacional_años.index, promedio_nacional_años.values, 
            'ro-', linewidth=3, markersize=8)
    ax2.set_title('Evolución del Promedio Nacional (2008-2022)', fontweight='bold')
    ax2.set_xlabel('Año')
    ax2.set_ylabel('Promedio Nacional')
    ax2.grid(True, alpha=0.3)
    ax2.set_xticks(range(2008, 2023, 2))
    
    # Agregar línea de tendencia
    x = np.arange(len(promedio_nacional_años))
    z = np.polyfit(x, promedio_nacional_años.values, 1)
    p = np.poly1d(z)
    ax2.plot(promedio_nacional_años.index, p(x), "r--", alpha=0.8, 
            label=f'Tendencia: {z[0]:.2f}/año')
    ax2.legend()
    
    # 3. Instituciones sobre/bajo el promedio nacional por año
    sobre_promedio = []
    bajo_promedio = []
    años = sorted(df['año'].unique())
    
    for año in años:
        datos_año = df[df['año'] == año]
        promedio_nacional = datos_año['total_nacional'].iloc[0]
        
        sobre = len(datos_año[datos_año['valor_institucion'] > promedio_nacional])
        bajo = len(datos_año[datos_año['valor_institucion'] <= promedio_nacional])
        
        sobre_promedio.append(sobre)
        bajo_promedio.append(bajo)
    
    width = 0.35
    x = np.arange(len(años))
    
    ax3.bar(x - width/2, sobre_promedio, width, label='Sobre el promedio', color='green', alpha=0.7)
    ax3.bar(x + width/2, bajo_promedio, width, label='Bajo el promedio', color='red', alpha=0.7)
    
    ax3.set_title('Instituciones Sobre/Bajo el Promedio Nacional por Año', fontweight='bold')
    ax3.set_xlabel('Año')
    ax3.set_ylabel('Número de Instituciones')
    ax3.set_xticks(x)
    ax3.set_xticklabels(años, rotation=45)
    ax3.legend()
    
    # 4. Análisis de volatilidad por institución
    volatilidad = {}
    for institucion in df['institucion_corta'].unique():
        datos_inst = df[df['institucion_corta'] == institucion]['valor_institucion']
        # Calcular volatilidad como la desviación estándar de los cambios porcentuales
        cambios = datos_inst.pct_change().dropna()
        volatilidad[institucion] = cambios.std() * 100 if len(cambios) > 0 else 0
    
    bars = ax4.bar(range(len(volatilidad)), list(volatilidad.values()), color='purple', alpha=0.7)
    ax4.set_xticks(range(len(volatilidad)))
    ax4.set_xticklabels(list(volatilidad.keys()), rotation=45, ha='right')
    ax4.set_title('Volatilidad del Desempeño por Institución', fontweight='bold')
    ax4.set_ylabel('Volatilidad (%)')
    
    for bar, value in zip(bars, volatilidad.values()):
        ax4.text(bar.get_x() + bar.get_width()/2., bar.get_height() + 0.5,
                f'{value:.1f}%', ha='center', va='bottom', fontweight='bold')
    
    plt.suptitle('Análisis por Períodos - Instituciones de Educación Superior', 
                 fontsize=16, fontweight='bold')
    plt.tight_layout()
    plt.savefig('analisis_distribucion/graficas/analisis_periodos_ies.png', 
                dpi=300, bbox_inches='tight')
    plt.close()
    print("Gráfica guardada: analisis_periodos_ies.png")

def crear_dashboard_completo(df):
    """Crear un dashboard completo con todas las métricas"""
    fig = plt.figure(figsize=(20, 16))
    gs = fig.add_gridspec(3, 4, hspace=0.3, wspace=0.3)
    
    # 1. Evolución temporal principal (span 2 columns)
    ax1 = fig.add_subplot(gs[0, :2])
    for institucion in df['institucion_corta'].unique():
        datos_inst = df[df['institucion_corta'] == institucion]
        ax1.plot(datos_inst['año'], datos_inst['valor_institucion'], 
                marker='o', linewidth=3, label=institucion, markersize=8)
    
    # Promedio nacional
    ax1.plot(df['año'].unique(), df.groupby('año')['total_nacional'].first(), 
            'k--', linewidth=3, label='Promedio Nacional', alpha=0.8)
    
    ax1.set_title('Evolución del Desempeño 2008-2022', fontweight='bold', fontsize=16)
    ax1.set_xlabel('Año')
    ax1.set_ylabel('Valor de Desempeño')
    ax1.legend()
    ax1.grid(True, alpha=0.3)
    
    # 2. Ranking actual (span 2 columns)
    ax2 = fig.add_subplot(gs[0, 2:])
    ranking_actual = df[df['año'] == df['año'].max()].sort_values('valor_institucion', ascending=True)
    bars = ax2.barh(range(len(ranking_actual)), ranking_actual['valor_institucion'], 
                    color=sns.color_palette("viridis", len(ranking_actual)))
    ax2.set_yticks(range(len(ranking_actual)))
    ax2.set_yticklabels(ranking_actual['institucion_corta'])
    ax2.set_title(f'Ranking {df["año"].max()}', fontweight='bold', fontsize=16)
    ax2.set_xlabel('Valor de Desempeño')
    
    for i, bar in enumerate(bars):
        width = bar.get_width()
        ax2.text(width + 1, bar.get_y() + bar.get_height()/2, 
                f'{width:.1f}', ha='left', va='center', fontweight='bold')
    
    # 3. Comparación de promedios históricos
    ax3 = fig.add_subplot(gs[1, 0])
    promedios = df.groupby('institucion_corta')['valor_institucion'].mean().sort_values(ascending=False)
    ax3.pie(promedios.values, labels=promedios.index, autopct='%1.1f%%', startangle=90)
    ax3.set_title('Distribución de\nPromedios Históricos', fontweight='bold')
    
    # 4. Tendencias
    ax4 = fig.add_subplot(gs[1, 1])
    tendencias = {}
    for institucion in df['institucion_corta'].unique():
        datos_inst = df[df['institucion_corta'] == institucion].sort_values('año')
        if len(datos_inst) > 1:
            x = np.arange(len(datos_inst))
            y = datos_inst['valor_institucion'].values
            pendiente = np.polyfit(x, y, 1)[0]
            tendencias[institucion] = pendiente
    
    colores = ['green' if v > 0 else 'red' for v in tendencias.values()]
    bars = ax4.bar(range(len(tendencias)), list(tendencias.values()), color=colores)
    ax4.set_xticks(range(len(tendencias)))
    ax4.set_xticklabels(list(tendencias.keys()), rotation=45, ha='right')
    ax4.set_title('Tendencias de\nCrecimiento', fontweight='bold')
    ax4.axhline(y=0, color='black', linestyle='-', alpha=0.3)
    
    # 5. Variabilidad
    ax5 = fig.add_subplot(gs[1, 2])
    variabilidad = df.groupby('institucion_corta')['valor_institucion'].std()
    ax5.bar(range(len(variabilidad)), variabilidad.values, color='orange')
    ax5.set_xticks(range(len(variabilidad)))
    ax5.set_xticklabels(variabilidad.index, rotation=45, ha='right')
    ax5.set_title('Variabilidad\n(Desv. Estándar)', fontweight='bold')
    
    # 6. Mejores y peores años
    ax6 = fig.add_subplot(gs[1, 3])
    mejor_año = df.groupby('año')['valor_institucion'].mean().idxmax()
    peor_año = df.groupby('año')['valor_institucion'].mean().idxmin()
    
    datos_texto = f"""
    ESTADÍSTICAS GENERALES
    
    📊 Período analizado: 2008-2022
    🏛️ Instituciones: {df['institucion'].nunique()}
    📍 Ciudad: Santiago de Cali
    
    📈 Mejor año promedio: {mejor_año}
    📉 Peor año promedio: {peor_año}
    
    🎯 Valor máximo: {df['valor_institucion'].max():.1f}
    🎯 Valor mínimo: {df['valor_institucion'].min():.1f}
    🎯 Promedio general: {df['valor_institucion'].mean():.1f}
    """
    
    ax6.text(0.05, 0.95, datos_texto, transform=ax6.transAxes, fontsize=10,
            verticalalignment='top', bbox=dict(boxstyle='round', facecolor='lightblue', alpha=0.8))
    ax6.set_xlim(0, 1)
    ax6.set_ylim(0, 1)
    ax6.axis('off')
    ax6.set_title('Estadísticas\nGenerales', fontweight='bold')
    
    # 7. Heatmap histórico (span all bottom)
    ax7 = fig.add_subplot(gs[2, :])
    pivot_data = df.pivot(index='institucion_corta', columns='año', values='valor_institucion')
    im = ax7.imshow(pivot_data.values, cmap='RdYlGn', aspect='auto')
    ax7.set_xticks(range(len(pivot_data.columns)))
    ax7.set_xticklabels(pivot_data.columns)
    ax7.set_yticks(range(len(pivot_data.index)))
    ax7.set_yticklabels(pivot_data.index)
    ax7.set_title('Heatmap Histórico: Desempeño por Año e Institución', fontweight='bold', fontsize=16)
    
    # Agregar valores en el heatmap
    for i in range(len(pivot_data.index)):
        for j in range(len(pivot_data.columns)):
            value = pivot_data.iloc[i, j]
            if not pd.isna(value):
                color = 'white' if value < 70 else 'black'
                ax7.text(j, i, f'{value:.0f}', ha='center', va='center', 
                        color=color, fontweight='bold', fontsize=9)
    
    plt.colorbar(im, ax=ax7, label='Valor de Desempeño')
    
    plt.suptitle('Dashboard Completo - Instituciones de Educación Superior Santiago de Cali', 
                 fontsize=20, fontweight='bold')
    plt.savefig('analisis_distribucion/graficas/dashboard_completo_ies.png', 
                dpi=300, bbox_inches='tight')
    plt.close()
    print("Gráfica guardada: dashboard_completo_ies.png")

def generar_estadisticas_detalladas(df):
    """Generar estadísticas detalladas del análisis"""
    print("\n" + "="*80)
    print("ANÁLISIS DETALLADO - DATOS CONSOLIDADOS IES SANTIAGO DE CALI")
    print("="*80)
    
    print(f"\n📊 INFORMACIÓN GENERAL:")
    print(f"  • Período analizado: {df['año'].min()} - {df['año'].max()}")
    print(f"  • Total de registros: {len(df)}")
    print(f"  • Instituciones analizadas: {df['institucion'].nunique()}")
    print(f"  • Ciudad: {df['ciudad'].unique()[0]}")
    
    print(f"\n🏛️ INSTITUCIONES:")
    for i, inst in enumerate(df['institucion'].unique(), 1):
        print(f"  {i}. {inst}")
    
    print(f"\n📈 ESTADÍSTICAS DE DESEMPEÑO:")
    print(f"  • Valor máximo: {df['valor_institucion'].max():.1f}")
    print(f"  • Valor mínimo: {df['valor_institucion'].min():.1f}")
    print(f"  • Promedio general: {df['valor_institucion'].mean():.1f}")
    print(f"  • Desviación estándar: {df['valor_institucion'].std():.1f}")
    
    print(f"\n🏆 RANKING PROMEDIO HISTÓRICO:")
    ranking = df.groupby('institucion_corta')['valor_institucion'].agg(['mean', 'std', 'min', 'max']).round(2)
    ranking = ranking.sort_values('mean', ascending=False)
    
    for i, (inst, datos) in enumerate(ranking.iterrows(), 1):
        print(f"  {i}. {inst}")
        print(f"     - Promedio: {datos['mean']:.1f}")
        print(f"     - Desv. Estándar: {datos['std']:.1f}")
        print(f"     - Rango: {datos['min']:.1f} - {datos['max']:.1f}")
    
    print(f"\n📅 ANÁLISIS TEMPORAL:")
    mejor_año = df.groupby('año')['valor_institucion'].mean().idxmax()
    peor_año = df.groupby('año')['valor_institucion'].mean().idxmin()
    mejor_valor = df.groupby('año')['valor_institucion'].mean().max()
    peor_valor = df.groupby('año')['valor_institucion'].mean().min()
    
    print(f"  • Mejor año promedio: {mejor_año} ({mejor_valor:.1f})")
    print(f"  • Peor año promedio: {peor_año} ({peor_valor:.1f})")
    
    print(f"\n📊 COMPARACIÓN CON PROMEDIO NACIONAL:")
    for institucion in df['institucion_corta'].unique():
        datos_inst = df[df['institucion_corta'] == institucion]
        sobre_promedio = len(datos_inst[datos_inst['valor_institucion'] > datos_inst['total_nacional']])
        total_años = len(datos_inst)
        porcentaje = (sobre_promedio / total_años) * 100
        print(f"  • {institucion}: {sobre_promedio}/{total_años} años sobre el promedio nacional ({porcentaje:.1f}%)")
    
    return ranking

def main():
    """Función principal"""
    print("Iniciando análisis de datos consolidados IES Santiago de Cali...")
    print(f"Fecha: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    # Crear directorio si no existe
    os.makedirs('analisis_distribucion/graficas', exist_ok=True)
    
    # Cargar datos
    df = cargar_datos()
    
    # Generar visualizaciones
    crear_analisis_temporal(df)
    crear_analisis_estadistico(df)
    crear_analisis_periodos(df)
    crear_dashboard_completo(df)
    
    # Generar estadísticas detalladas
    ranking = generar_estadisticas_detalladas(df)
    
    print(f"\n✅ Análisis completado exitosamente!")
    print(f"📊 Se generaron 4 gráficas en 'analisis_distribucion/graficas/'")
    print(f"📈 Analizadas {df['institucion'].nunique()} instituciones en {df['año'].nunique()} años")

if __name__ == "__main__":
    main()
