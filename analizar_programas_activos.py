#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Análisis de Programas Activos por Región
Genera visualizaciones de los programas activos consolidados por región y período
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

def cargar_y_limpiar_datos():
    """Cargar y limpiar los datos de programas activos"""
    df = pd.read_excel('resultados/Consolidado_Programas_ACTIVOS.xlsx')
    
    # Identificar filas que contienen información de región
    region_rows = df[df['CODIGO_SNIES_PROGRAMA'].astype(str).str.contains('REGIÓN', na=False)]
    print("Regiones identificadas:")
    for idx, row in region_rows.iterrows():
        region_name = row['CODIGO_SNIES_PROGRAMA']
        print(f"  - Fila {idx}: {region_name}")
    
    # Crear una estructura de datos organizada por región
    regiones_data = {}
    current_region = None
    
    for idx, row in df.iterrows():
        codigo = str(row['CODIGO_SNIES_PROGRAMA'])
        
        if 'REGIÓN' in codigo:
            # Es una fila de región
            current_region = codigo.replace('REGIÓN: ', '')
            regiones_data[current_region] = []
        elif current_region and not pd.isna(row['CODIGO_SNIES_PROGRAMA']) and codigo != 'nan':
            # Es una fila de datos de programa
            regiones_data[current_region].append(row)
    
    return regiones_data, df

def procesar_datos_por_region(regiones_data):
    """Procesar datos agregados por región"""
    resultados_region = {}
    periodos = ['2018_1', '2018_2', '2019_1', '2019_2', '2020_1', '2020_2', 
                '2021_1', '2021_2', '2022_1', '2022_2', '2023_1', '2023_2', 
                '2024_1', '2024_2']
    
    for region, programas in regiones_data.items():
        if not programas:
            continue
            
        # Convertir a DataFrame
        df_region = pd.DataFrame(programas)
        
        # Sumar valores por período
        sumas_periodo = {}
        for periodo in periodos:
            if periodo in df_region.columns:
                valores = pd.to_numeric(df_region[periodo], errors='coerce').fillna(0)
                sumas_periodo[periodo] = valores.sum()
        
        resultados_region[region] = {
            'num_programas': len(programas),
            'num_instituciones': df_region['INSTITUCION_EDUCACION_SUPERIOR'].nunique(),
            'sumas_periodo': sumas_periodo,
            'total_general': sum(sumas_periodo.values())
        }
    
    return resultados_region

def crear_visualizacion_resumen_regiones(resultados_region):
    """Crear visualización de resumen por regiones"""
    fig, ((ax1, ax2), (ax3, ax4)) = plt.subplots(2, 2, figsize=(16, 12))
    
    regiones = list(resultados_region.keys())
    
    # 1. Número de programas por región
    num_programas = [resultados_region[r]['num_programas'] for r in regiones]
    bars1 = ax1.bar(range(len(regiones)), num_programas, color='skyblue')
    ax1.set_xticks(range(len(regiones)))
    ax1.set_xticklabels(regiones, rotation=45, ha='right')
    ax1.set_title('Número de Programas Activos por Región', fontweight='bold')
    ax1.set_ylabel('Número de Programas')
    
    # Agregar valores en las barras
    for i, bar in enumerate(bars1):
        height = bar.get_height()
        ax1.text(bar.get_x() + bar.get_width()/2., height + 0.1,
                f'{int(height)}', ha='center', va='bottom', fontweight='bold')
    
    # 2. Número de instituciones por región
    num_instituciones = [resultados_region[r]['num_instituciones'] for r in regiones]
    bars2 = ax2.bar(range(len(regiones)), num_instituciones, color='lightcoral')
    ax2.set_xticks(range(len(regiones)))
    ax2.set_xticklabels(regiones, rotation=45, ha='right')
    ax2.set_title('Número de Instituciones por Región', fontweight='bold')
    ax2.set_ylabel('Número de Instituciones')
    
    for i, bar in enumerate(bars2):
        height = bar.get_height()
        ax2.text(bar.get_x() + bar.get_width()/2., height + 0.1,
                f'{int(height)}', ha='center', va='bottom', fontweight='bold')
    
    # 3. Total general de estudiantes por región
    total_estudiantes = [resultados_region[r]['total_general'] for r in regiones]
    bars3 = ax3.bar(range(len(regiones)), total_estudiantes, color='lightgreen')
    ax3.set_xticks(range(len(regiones)))
    ax3.set_xticklabels(regiones, rotation=45, ha='right')
    ax3.set_title('Total de Estudiantes por Región (2018-2024)', fontweight='bold')
    ax3.set_ylabel('Total de Estudiantes')
    
    for i, bar in enumerate(bars3):
        height = bar.get_height()
        ax3.text(bar.get_x() + bar.get_width()/2., height + 100,
                f'{int(height):,}', ha='center', va='bottom', fontweight='bold', fontsize=9)
    
    # 4. Gráfica circular de distribución de programas
    ax4.pie(num_programas, labels=regiones, autopct='%1.1f%%', startangle=90)
    ax4.set_title('Distribución Porcentual de Programas por Región', fontweight='bold')
    
    plt.suptitle('Análisis de Programas Activos por Región', fontsize=16, fontweight='bold')
    plt.tight_layout()
    plt.savefig('analisis_distribucion/graficas/programas_activos_por_region.png', 
                dpi=300, bbox_inches='tight')
    plt.close()
    print("Gráfica guardada: programas_activos_por_region.png")

def crear_evolucion_temporal(resultados_region):
    """Crear gráfica de evolución temporal por región"""
    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(16, 12))
    
    periodos = ['2018_1', '2018_2', '2019_1', '2019_2', '2020_1', '2020_2', 
                '2021_1', '2021_2', '2022_1', '2022_2', '2023_1', '2023_2', 
                '2024_1', '2024_2']
    
    # Preparar datos para la gráfica
    data_temporal = {}
    for region in resultados_region.keys():
        data_temporal[region] = [resultados_region[region]['sumas_periodo'].get(p, 0) for p in periodos]
    
    # 1. Evolución temporal por región (líneas)
    for region, valores in data_temporal.items():
        ax1.plot(periodos, valores, marker='o', linewidth=2, label=region)
    
    ax1.set_title('Evolución Temporal de Estudiantes por Región (2018-2024)', fontweight='bold')
    ax1.set_ylabel('Número de Estudiantes')
    ax1.set_xlabel('Período')
    ax1.tick_params(axis='x', rotation=45)
    ax1.legend(bbox_to_anchor=(1.05, 1), loc='upper left')
    ax1.grid(True, alpha=0.3)
    
    # 2. Heatmap de estudiantes por región y período
    df_heatmap = pd.DataFrame(data_temporal, index=periodos).T
    sns.heatmap(df_heatmap, annot=True, fmt='.0f', cmap='YlOrRd', ax=ax2, cbar_kws={'label': 'Estudiantes'})
    ax2.set_title('Mapa de Calor: Estudiantes por Región y Período', fontweight='bold')
    ax2.set_ylabel('Región')
    ax2.set_xlabel('Período')
    
    plt.tight_layout()
    plt.savefig('analisis_distribucion/graficas/evolucion_temporal_regiones.png', 
                dpi=300, bbox_inches='tight')
    plt.close()
    print("Gráfica guardada: evolucion_temporal_regiones.png")

def crear_analisis_comparativo(resultados_region):
    """Crear análisis comparativo entre regiones"""
    fig, ((ax1, ax2), (ax3, ax4)) = plt.subplots(2, 2, figsize=(16, 12))
    
    regiones = list(resultados_region.keys())
    
    # 1. Comparación de totales por región (barras horizontales)
    total_estudiantes = [resultados_region[r]['total_general'] for r in regiones]
    bars = ax1.barh(regiones, total_estudiantes, color=sns.color_palette("viridis", len(regiones)))
    ax1.set_title('Total de Estudiantes por Región (2018-2024)', fontweight='bold')
    ax1.set_xlabel('Total de Estudiantes')
    
    for i, bar in enumerate(bars):
        width = bar.get_width()
        ax1.text(width + 50, bar.get_y() + bar.get_height()/2, 
                f'{int(width):,}', ha='left', va='center', fontweight='bold')
    
    # 2. Ratio estudiantes/programa por región
    ratios = [resultados_region[r]['total_general'] / resultados_region[r]['num_programas'] 
              if resultados_region[r]['num_programas'] > 0 else 0 for r in regiones]
    ax2.bar(range(len(regiones)), ratios, color='lightcoral')
    ax2.set_xticks(range(len(regiones)))
    ax2.set_xticklabels(regiones, rotation=45, ha='right')
    ax2.set_title('Promedio de Estudiantes por Programa', fontweight='bold')
    ax2.set_ylabel('Estudiantes/Programa')
    
    # 3. Distribución de períodos más activos
    # Sumar todos los períodos de la primera mitad (2018-2021) vs segunda mitad (2022-2024)
    primera_mitad = ['2018_1', '2018_2', '2019_1', '2019_2', '2020_1', '2020_2', '2021_1', '2021_2']
    segunda_mitad = ['2022_1', '2022_2', '2023_1', '2023_2', '2024_1', '2024_2']
    
    suma_primera = []
    suma_segunda = []
    
    for region in regiones:
        suma1 = sum(resultados_region[region]['sumas_periodo'].get(p, 0) for p in primera_mitad)
        suma2 = sum(resultados_region[region]['sumas_periodo'].get(p, 0) for p in segunda_mitad)
        suma_primera.append(suma1)
        suma_segunda.append(suma2)
    
    x = np.arange(len(regiones))
    width = 0.35
    
    ax3.bar(x - width/2, suma_primera, width, label='2018-2021', color='skyblue')
    ax3.bar(x + width/2, suma_segunda, width, label='2022-2024', color='orange')
    
    ax3.set_title('Comparación por Períodos: Primera vs Segunda Mitad', fontweight='bold')
    ax3.set_ylabel('Total de Estudiantes')
    ax3.set_xticks(x)
    ax3.set_xticklabels(regiones, rotation=45, ha='right')
    ax3.legend()
    
    # 4. Ranking de regiones por total de estudiantes
    datos_ranking = [(region, resultados_region[region]['total_general']) for region in regiones]
    datos_ranking.sort(key=lambda x: x[1], reverse=True)
    
    regiones_ranking = [x[0] for x in datos_ranking]
    valores_ranking = [x[1] for x in datos_ranking]
    
    bars4 = ax4.bar(range(len(regiones_ranking)), valores_ranking, 
                    color=sns.color_palette("plasma", len(regiones_ranking)))
    ax4.set_xticks(range(len(regiones_ranking)))
    ax4.set_xticklabels(regiones_ranking, rotation=45, ha='right')
    ax4.set_title('Ranking de Regiones por Total de Estudiantes', fontweight='bold')
    ax4.set_ylabel('Total de Estudiantes')
    
    # Agregar ranking numbers
    for i, bar in enumerate(bars4):
        height = bar.get_height()
        ax4.text(bar.get_x() + bar.get_width()/2., height + 100,
                f'#{i+1}\n{int(height):,}', ha='center', va='bottom', 
                fontweight='bold', fontsize=9)
    
    plt.suptitle('Análisis Comparativo de Regiones', fontsize=16, fontweight='bold')
    plt.tight_layout()
    plt.savefig('analisis_distribucion/graficas/analisis_comparativo_regiones.png', 
                dpi=300, bbox_inches='tight')
    plt.close()
    print("Gráfica guardada: analisis_comparativo_regiones.png")

def generar_reporte_estadistico(resultados_region):
    """Generar reporte estadístico de los resultados"""
    print("\n" + "="*60)
    print("REPORTE ESTADÍSTICO - PROGRAMAS ACTIVOS POR REGIÓN")
    print("="*60)
    
    total_programas = sum(r['num_programas'] for r in resultados_region.values())
    total_instituciones = sum(r['num_instituciones'] for r in resultados_region.values())
    total_estudiantes = sum(r['total_general'] for r in resultados_region.values())
    
    print(f"\nRESUMEN GENERAL:")
    print(f"  Total de regiones: {len(resultados_region)}")
    print(f"  Total de programas: {total_programas}")
    print(f"  Total de instituciones: {total_instituciones}")
    print(f"  Total de estudiantes (2018-2024): {total_estudiantes:,}")
    
    print(f"\nDETALLE POR REGIÓN:")
    
    # Ordenar regiones por total de estudiantes
    regiones_ordenadas = sorted(resultados_region.items(), 
                               key=lambda x: x[1]['total_general'], reverse=True)
    
    for i, (region, datos) in enumerate(regiones_ordenadas, 1):
        porcentaje = (datos['total_general'] / total_estudiantes) * 100
        print(f"\n  {i}. {region}")
        print(f"     - Programas: {datos['num_programas']}")
        print(f"     - Instituciones: {datos['num_instituciones']}")
        print(f"     - Total estudiantes: {datos['total_general']:,} ({porcentaje:.1f}%)")
        print(f"     - Promedio estudiantes/programa: {datos['total_general']/datos['num_programas']:.1f}")
    
    return {
        'total_programas': total_programas,
        'total_instituciones': total_instituciones,
        'total_estudiantes': total_estudiantes,
        'regiones_data': resultados_region
    }

def main():
    """Función principal"""
    print("Iniciando análisis de programas activos por región...")
    print(f"Fecha: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    # Crear directorio si no existe
    os.makedirs('analisis_distribucion/graficas', exist_ok=True)
    
    # Cargar y procesar datos
    regiones_data, df_original = cargar_y_limpiar_datos()
    resultados_region = procesar_datos_por_region(regiones_data)
    
    if not resultados_region:
        print("No se encontraron datos válidos para procesar.")
        return
    
    # Generar visualizaciones
    crear_visualizacion_resumen_regiones(resultados_region)
    crear_evolucion_temporal(resultados_region)
    crear_analisis_comparativo(resultados_region)
    
    # Generar reporte estadístico
    estadisticas = generar_reporte_estadistico(resultados_region)
    
    print(f"\n✅ Análisis completado exitosamente!")
    print(f"📊 Se generaron 3 gráficas en 'analisis_distribucion/graficas/'")
    print(f"📈 Analizadas {len(resultados_region)} regiones con {estadisticas['total_programas']} programas")

if __name__ == "__main__":
    main()
