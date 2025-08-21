#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Análisis Actualizado de Programas Activos por Región
Versión actualizada para manejar los nuevos nombres de regiones con "(ACTIVOS)"
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

def cargar_y_limpiar_datos_actualizados():
    """Cargar y limpiar los datos actualizados de programas activos"""
    df = pd.read_excel('resultados/Consolidado_Programas_ACTIVOS.xlsx')
    
    # Identificar filas que contienen información de región (actualizado para incluir "(ACTIVOS)")
    region_rows = df[df['CODIGO_SNIES_PROGRAMA'].astype(str).str.contains('REGIÓN.*ACTIVOS', na=False)]
    print("Regiones identificadas en archivo actualizado:")
    for idx, row in region_rows.iterrows():
        region_name = row['CODIGO_SNIES_PROGRAMA']
        print(f"  - Fila {idx}: {region_name}")
    
    # Crear una estructura de datos organizada por región
    regiones_data = {}
    current_region = None
    
    for idx, row in df.iterrows():
        codigo = str(row['CODIGO_SNIES_PROGRAMA'])
        
        if 'REGIÓN' in codigo and 'ACTIVOS' in codigo:
            # Es una fila de región - limpiar el nombre
            current_region = codigo.replace('REGIÓN: ', '').replace(' (ACTIVOS)', '')
            regiones_data[current_region] = []
            print(f"Procesando región: {current_region}")
        elif current_region and not pd.isna(row['CODIGO_SNIES_PROGRAMA']) and codigo != 'nan':
            # Es una fila de datos de programa
            regiones_data[current_region].append(row)
    
    return regiones_data, df

def procesar_datos_por_region_actualizado(regiones_data):
    """Procesar datos agregados por región con nombres actualizados"""
    resultados_region = {}
    periodos = ['2018_1', '2018_2', '2019_1', '2019_2', '2020_1', '2020_2', 
                '2021_1', '2021_2', '2022_1', '2022_2', '2023_1', '2023_2', 
                '2024_1', '2024_2']
    
    for region, programas in regiones_data.items():
        if not programas:
            continue
            
        print(f"Procesando {len(programas)} programas en {region}")
        
        # Convertir a DataFrame
        df_region = pd.DataFrame(programas)
        
        # Sumar valores por período
        sumas_periodo = {}
        for periodo in periodos:
            if periodo in df_region.columns:
                valores = pd.to_numeric(df_region[periodo], errors='coerce').fillna(0)
                sumas_periodo[periodo] = valores.sum()
        
        # Contar instituciones únicas
        instituciones_unicas = df_region['INSTITUCION_EDUCACION_SUPERIOR'].nunique()
        
        resultados_region[region] = {
            'num_programas': len(programas),
            'num_instituciones': instituciones_unicas,
            'sumas_periodo': sumas_periodo,
            'total_general': sum(sumas_periodo.values()),
            'instituciones': df_region['INSTITUCION_EDUCACION_SUPERIOR'].unique().tolist()
        }
        
        print(f"  - {region}: {len(programas)} programas, {instituciones_unicas} instituciones, {sum(sumas_periodo.values()):.0f} estudiantes")
    
    return resultados_region

def crear_visualizacion_actualizada(resultados_region):
    """Crear visualización actualizada con los nuevos nombres de regiones"""
    fig, ((ax1, ax2), (ax3, ax4)) = plt.subplots(2, 2, figsize=(16, 12))
    
    regiones = list(resultados_region.keys())
    print(f"Creando gráficas para {len(regiones)} regiones: {regiones}")
    
    # 1. Número de programas por región
    num_programas = [resultados_region[r]['num_programas'] for r in regiones]
    bars1 = ax1.bar(range(len(regiones)), num_programas, color='skyblue')
    ax1.set_xticks(range(len(regiones)))
    ax1.set_xticklabels(regiones, rotation=45, ha='right')
    ax1.set_title('Programas Activos por Región (Actualizado)', fontweight='bold')
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
    ax2.set_title('Instituciones por Región (Actualizado)', fontweight='bold')
    ax2.set_ylabel('Número de Instituciones')
    
    for i, bar in enumerate(bars2):
        height = bar.get_height()
        ax2.text(bar.get_x() + bar.get_width()/2., height + 0.1,
                f'{int(height)}', ha='center', va='bottom', fontweight='bold')
    
    # 3. Total de estudiantes por región
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
    
    # 4. Gráfica circular actualizada
    ax4.pie(num_programas, labels=regiones, autopct='%1.1f%%', startangle=90)
    ax4.set_title('Distribución Porcentual de Programas (Actualizado)', fontweight='bold')
    
    plt.suptitle('Análisis Actualizado de Programas Activos por Región', fontsize=16, fontweight='bold')
    plt.tight_layout()
    plt.savefig('analisis_distribucion/graficas/programas_activos_actualizado.png', 
                dpi=300, bbox_inches='tight')
    plt.close()
    print("Gráfica actualizada guardada: programas_activos_actualizado.png")

def crear_comparacion_cambios(resultados_region):
    """Crear gráfica que muestre los cambios en las regiones"""
    fig, ((ax1, ax2), (ax3, ax4)) = plt.subplots(2, 2, figsize=(16, 12))
    
    regiones = list(resultados_region.keys())
    
    # Mapeo de nombres anteriores a actuales para mostrar cambios
    cambios_regiones = {
        'Región Bogotá': 'Sin cambio',
        'Región Centro Occidente': 'Sin cambio', 
        'Región Centro Oriente': 'Sin cambio',
        'Región Norte': 'Sin cambio',
        'Región Occidente': 'Antes: Región Sur Occidente',
        'Región Sur Oriente': 'Sin cambio'
    }
    
    # 1. Ranking por total de estudiantes
    datos_ranking = [(region, resultados_region[region]['total_general']) for region in regiones]
    datos_ranking.sort(key=lambda x: x[1], reverse=True)
    
    regiones_ranking = [x[0] for x in datos_ranking]
    valores_ranking = [x[1] for x in datos_ranking]
    
    colors = ['gold', 'silver', '#CD7F32', 'lightblue', 'lightgreen', 'lightcoral'][:len(regiones_ranking)]
    bars1 = ax1.barh(regiones_ranking, valores_ranking, color=colors)
    ax1.set_title('Ranking Actualizado: Total de Estudiantes por Región', fontweight='bold')
    ax1.set_xlabel('Total de Estudiantes')
    
    # Agregar valores y ranking
    for i, bar in enumerate(bars1):
        width = bar.get_width()
        rank = f"#{i+1}"
        ax1.text(width + 50, bar.get_y() + bar.get_height()/2, 
                f'{rank} - {int(width):,}', ha='left', va='center', fontweight='bold')
    
    # 2. Eficiencia por programa
    eficiencias = [(region, resultados_region[region]['total_general']/resultados_region[region]['num_programas']) 
                   for region in regiones]
    eficiencias.sort(key=lambda x: x[1], reverse=True)
    
    regiones_ef = [x[0] for x in eficiencias]
    valores_ef = [x[1] for x in eficiencias]
    
    ax2.bar(range(len(regiones_ef)), valores_ef, color='orange')
    ax2.set_xticks(range(len(regiones_ef)))
    ax2.set_xticklabels(regiones_ef, rotation=45, ha='right')
    ax2.set_title('Eficiencia: Estudiantes por Programa', fontweight='bold')
    ax2.set_ylabel('Estudiantes/Programa')
    
    # 3. Distribución de programas e instituciones
    num_programas = [resultados_region[r]['num_programas'] for r in regiones]
    num_instituciones = [resultados_region[r]['num_instituciones'] for r in regiones]
    
    x = np.arange(len(regiones))
    width = 0.35
    
    ax3.bar(x - width/2, num_programas, width, label='Programas', color='skyblue')
    ax3.bar(x + width/2, num_instituciones, width, label='Instituciones', color='lightcoral')
    
    ax3.set_title('Programas vs Instituciones por Región', fontweight='bold')
    ax3.set_ylabel('Cantidad')
    ax3.set_xticks(x)
    ax3.set_xticklabels(regiones, rotation=45, ha='right')
    ax3.legend()
    
    # 4. Mapa de cambios en nombres
    ax4.axis('off')
    ax4.set_title('Cambios en Nombres de Regiones', fontweight='bold', fontsize=14)
    
    texto_cambios = "ACTUALIZACIÓN DE NOMBRES:\n\n"
    for region, cambio in cambios_regiones.items():
        if cambio == 'Sin cambio':
            texto_cambios += f"✓ {region}\n"
        else:
            texto_cambios += f"🔄 {region}\n   ({cambio})\n"
    
    ax4.text(0.1, 0.9, texto_cambios, transform=ax4.transAxes, fontsize=12,
             verticalalignment='top', bbox=dict(boxstyle='round', facecolor='lightblue', alpha=0.8))
    
    plt.suptitle('Análisis Comparativo con Nombres Actualizados', fontsize=16, fontweight='bold')
    plt.tight_layout()
    plt.savefig('analisis_distribucion/graficas/comparacion_regiones_actualizadas.png', 
                dpi=300, bbox_inches='tight')
    plt.close()
    print("Gráfica de comparación guardada: comparacion_regiones_actualizadas.png")

def generar_reporte_actualizado(resultados_region):
    """Generar reporte estadístico actualizado"""
    print("\n" + "="*70)
    print("REPORTE ACTUALIZADO - PROGRAMAS ACTIVOS POR REGIÓN")
    print("="*70)
    
    total_programas = sum(r['num_programas'] for r in resultados_region.values())
    total_instituciones = sum(r['num_instituciones'] for r in resultados_region.values())
    total_estudiantes = sum(r['total_general'] for r in resultados_region.values())
    
    print(f"\nRESUMEN GENERAL ACTUALIZADO:")
    print(f"  Total de regiones: {len(resultados_region)}")
    print(f"  Total de programas: {total_programas}")
    print(f"  Total de instituciones: {total_instituciones}")
    print(f"  Total de estudiantes (2018-2024): {total_estudiantes:,}")
    
    # Detectar cambio en Región Occidente
    if 'Región Occidente' in resultados_region:
        print(f"\n⚠️  CAMBIO DETECTADO:")
        print(f"  'Región Sur Occidente' ahora se llama 'Región Occidente'")
        print(f"  Datos de Región Occidente: {resultados_region['Región Occidente']['num_programas']} programas")
    
    print(f"\nRANKING ACTUALIZADO POR REGIÓN:")
    
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
    
    return resultados_region

def main():
    """Función principal actualizada"""
    print("Iniciando análisis ACTUALIZADO de programas activos por región...")
    print(f"Fecha: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("Verificando archivo con nombres de regiones actualizados...")
    
    # Crear directorio si no existe
    os.makedirs('analisis_distribucion/graficas', exist_ok=True)
    
    # Cargar y procesar datos actualizados
    regiones_data, df_original = cargar_y_limpiar_datos_actualizados()
    resultados_region = procesar_datos_por_region_actualizado(regiones_data)
    
    if not resultados_region:
        print("No se encontraron datos válidos para procesar.")
        return
    
    # Generar visualizaciones actualizadas
    crear_visualizacion_actualizada(resultados_region)
    crear_comparacion_cambios(resultados_region)
    
    # Generar reporte estadístico
    generar_reporte_actualizado(resultados_region)
    
    print(f"\n✅ Análisis ACTUALIZADO completado exitosamente!")
    print(f"📊 Se generaron 2 gráficas actualizadas en 'analisis_distribucion/graficas/'")
    print(f"📈 Analizadas {len(resultados_region)} regiones (nombres actualizados)")
    print(f"🔄 Detectado cambio: 'Región Sur Occidente' → 'Región Occidente'")

if __name__ == "__main__":
    main()
