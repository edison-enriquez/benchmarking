#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Análisis de Distribución de Programas de Benchmarking
Genera gráficas circulares y de barras para analizar la distribución de programas
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
plt.rcParams['figure.figsize'] = (12, 8)
plt.rcParams['font.size'] = 10
plt.rcParams['axes.titlesize'] = 14
plt.rcParams['axes.labelsize'] = 12

def cargar_datos():
    """Cargar datos del archivo Excel"""
    try:
        df = pd.read_excel('../files/programas_benchmarking.xlsx')
        print(f"Datos cargados correctamente: {df.shape[0]} programas")
        return df
    except Exception as e:
        print(f"Error al cargar datos: {e}")
        return None

def crear_grafica_circular(data, titulo, archivo, colores=None):
    """Crear gráfica circular con etiquetas y porcentajes"""
    fig, ax = plt.subplots(figsize=(10, 8))
    
    # Crear gráfica circular
    wedges, texts, autotexts = ax.pie(data.values, 
                                      labels=data.index, 
                                      autopct='%1.1f%%',
                                      startangle=90,
                                      colors=colores)
    
    # Mejorar visualización
    for autotext in autotexts:
        autotext.set_color('white')
        autotext.set_fontweight('bold')
    
    ax.set_title(titulo, fontsize=16, fontweight='bold', pad=20)
    
    # Agregar leyenda con conteos
    legend_labels = [f'{label}: {count}' for label, count in zip(data.index, data.values)]
    ax.legend(wedges, legend_labels, title="Distribución", 
             loc="center left", bbox_to_anchor=(1, 0, 0.5, 1))
    
    plt.tight_layout()
    plt.savefig(f'graficas/{archivo}', dpi=300, bbox_inches='tight')
    plt.close()
    print(f"Gráfica guardada: {archivo}")

def crear_grafica_barras(data, titulo, archivo, color='skyblue'):
    """Crear gráfica de barras horizontal"""
    fig, ax = plt.subplots(figsize=(12, 8))
    
    bars = ax.barh(data.index, data.values, color=color)
    
    # Agregar valores en las barras
    for i, bar in enumerate(bars):
        width = bar.get_width()
        ax.text(width + 0.1, bar.get_y() + bar.get_height()/2, 
                f'{int(width)}', ha='left', va='center', fontweight='bold')
    
    ax.set_xlabel('Número de Programas', fontweight='bold')
    ax.set_title(titulo, fontsize=16, fontweight='bold', pad=20)
    ax.grid(axis='x', alpha=0.3)
    
    plt.tight_layout()
    plt.savefig(f'graficas/{archivo}', dpi=300, bbox_inches='tight')
    plt.close()
    print(f"Gráfica guardada: {archivo}")

def analizar_distribucion_nacional(df):
    """Analizar y graficar distribución nacional"""
    print("\n=== ANÁLISIS DISTRIBUCIÓN NACIONAL ===")
    
    # 1. Reconocimiento del Ministerio
    reconocimiento = df['RECONOCIMIENTO_DEL_MINISTERIO'].value_counts()
    crear_grafica_circular(reconocimiento, 
                          'Distribución Nacional por Reconocimiento del Ministerio',
                          'distribucion_reconocimiento_ministerio.png',
                          ['#FF6B6B', '#4ECDC4'])
    
    # 2. Modalidad
    modalidad = df['MODALIDAD'].value_counts()
    crear_grafica_circular(modalidad,
                          'Distribución Nacional por Modalidad',
                          'distribucion_modalidad.png',
                          ['#45B7D1', '#96CEB4', '#FECA57'])
    
    # 3. Sector
    sector = df['SECTOR'].value_counts()
    crear_grafica_circular(sector,
                          'Distribución Nacional por Sector',
                          'distribucion_sector.png',
                          ['#6C5CE7', '#A29BFE'])
    
    # 4. Distribución por Regiones
    regiones = df['REGION'].value_counts()
    crear_grafica_barras(regiones,
                        'Distribución Nacional por Regiones',
                        'distribucion_regiones.png',
                        '#FF7675')

def analizar_region_occidente(df):
    """Analizar específicamente la Región Occidente"""
    print("\n=== ANÁLISIS REGIÓN OCCIDENTE ===")
    
    # Filtrar datos de Región Occidente
    occidente = df[df['REGION'] == 'Región Occidente']
    
    if len(occidente) == 0:
        print("No se encontraron programas en Región Occidente")
        return
    
    print(f"Programas en Región Occidente: {len(occidente)}")
    
    # 1. Distribución por ciudades
    ciudades = occidente['MUNICIPIO_OFERTA_PROGRAMA'].value_counts()
    crear_grafica_circular(ciudades,
                          'Distribución en Región Occidente por Ciudades',
                          'distribucion_ciudades_occidente.png')
    
    # 2. Distribución por modalidad en Occidente
    modalidad_occidente = occidente['MODALIDAD'].value_counts()
    crear_grafica_circular(modalidad_occidente,
                          'Distribución de Modalidad en Región Occidente',
                          'distribucion_modalidad_occidente.png',
                          ['#45B7D1', '#96CEB4', '#FECA57'])
    
    # 3. Distribución por sector en Occidente
    sector_occidente = occidente['SECTOR'].value_counts()
    crear_grafica_circular(sector_occidente,
                          'Distribución por Sector en Región Occidente',
                          'distribucion_sector_occidente.png',
                          ['#6C5CE7', '#A29BFE'])

def generar_resumen_estadistico(df):
    """Generar resumen estadístico completo"""
    print("\n=== RESUMEN ESTADÍSTICO ===")
    
    resumen = {
        'Total de Programas': len(df),
        'Instituciones Únicas': df['NOMBRE_INSTITUCIÓN'].nunique(),
        'Regiones': df['REGION'].nunique(),
        'Municipios': df['MUNICIPIO_OFERTA_PROGRAMA'].nunique(),
        'Modalidades': df['MODALIDAD'].nunique(),
        'Sectores': df['SECTOR'].nunique()
    }
    
    # Crear gráfica de resumen
    fig, ((ax1, ax2), (ax3, ax4)) = plt.subplots(2, 2, figsize=(16, 12))
    
    # Gráfica 1: Reconocimiento
    reconocimiento = df['RECONOCIMIENTO_DEL_MINISTERIO'].value_counts()
    ax1.pie(reconocimiento.values, labels=reconocimiento.index, autopct='%1.1f%%', startangle=90)
    ax1.set_title('Reconocimiento del Ministerio', fontweight='bold')
    
    # Gráfica 2: Sector
    sector = df['SECTOR'].value_counts()
    ax2.pie(sector.values, labels=sector.index, autopct='%1.1f%%', startangle=90)
    ax2.set_title('Distribución por Sector', fontweight='bold')
    
    # Gráfica 3: Top 5 Regiones
    regiones = df['REGION'].value_counts().head(5)
    ax3.bar(range(len(regiones)), regiones.values, color='lightblue')
    ax3.set_xticks(range(len(regiones)))
    ax3.set_xticklabels(regiones.index, rotation=45, ha='right')
    ax3.set_title('Top 5 Regiones', fontweight='bold')
    ax3.set_ylabel('Número de Programas')
    
    # Gráfica 4: Modalidad
    modalidad = df['MODALIDAD'].value_counts()
    ax4.pie(modalidad.values, labels=modalidad.index, autopct='%1.1f%%', startangle=90)
    ax4.set_title('Distribución por Modalidad', fontweight='bold')
    
    plt.suptitle('Resumen Completo - Distribución de Programas', fontsize=18, fontweight='bold')
    plt.tight_layout()
    plt.savefig('graficas/resumen_completo_distribucion.png', dpi=300, bbox_inches='tight')
    plt.close()
    
    print("Resumen estadístico:")
    for key, value in resumen.items():
        print(f"  {key}: {value}")
    
    return resumen

def main():
    """Función principal"""
    print("Iniciando análisis de distribución de programas...")
    print(f"Fecha: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    # Crear directorio de gráficas si no existe
    os.makedirs('graficas', exist_ok=True)
    
    # Cargar datos
    df = cargar_datos()
    if df is None:
        return
    
    # Realizar análisis
    analizar_distribucion_nacional(df)
    analizar_region_occidente(df)
    resumen = generar_resumen_estadistico(df)
    
    print(f"\n✅ Análisis completado exitosamente!")
    print(f"📊 Se generaron las gráficas en la carpeta 'graficas/'")
    print(f"📈 Total de programas analizados: {len(df)}")

if __name__ == "__main__":
    main()
