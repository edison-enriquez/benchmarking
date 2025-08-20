#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Análisis Adicional - Distribución Nacional Detallada
Genera gráficas específicas para el análisis nacional de programas
"""

import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np
from datetime import datetime

# Configuración de estilo mejorada
plt.style.use('seaborn-v0_8')
sns.set_palette("Set2")
plt.rcParams['figure.figsize'] = (14, 10)
plt.rcParams['font.size'] = 11
plt.rcParams['axes.titlesize'] = 16
plt.rcParams['axes.labelsize'] = 13

def cargar_datos():
    """Cargar datos del archivo Excel"""
    df = pd.read_excel('../files/programas_benchmarking.xlsx')
    return df

def crear_mapa_distribucional_nacional(df):
    """Crear un dashboard completo de distribución nacional"""
    fig = plt.figure(figsize=(20, 16))
    
    # Configuración de la grilla
    gs = fig.add_gridspec(3, 3, hspace=0.3, wspace=0.3)
    
    # 1. Distribución por Regiones (Gráfica de barras horizontal)
    ax1 = fig.add_subplot(gs[0, :2])
    regiones = df['REGION'].value_counts()
    bars = ax1.barh(regiones.index, regiones.values, color=sns.color_palette("viridis", len(regiones)))
    ax1.set_title('Distribución Nacional por Regiones', fontweight='bold', fontsize=16)
    ax1.set_xlabel('Número de Programas')
    
    # Agregar valores en las barras
    for i, bar in enumerate(bars):
        width = bar.get_width()
        ax1.text(width + 0.1, bar.get_y() + bar.get_height()/2, 
                f'{int(width)}', ha='left', va='center', fontweight='bold')
    
    # 2. Sector (Donut Chart)
    ax2 = fig.add_subplot(gs[0, 2])
    sector = df['SECTOR'].value_counts()
    colors = ['#FF6B6B', '#4ECDC4']
    wedges, texts, autotexts = ax2.pie(sector.values, labels=sector.index, autopct='%1.1f%%',
                                       startangle=90, colors=colors, pctdistance=0.85)
    
    # Crear efecto donut
    centre_circle = plt.Circle((0,0), 0.70, fc='white')
    ax2.add_artist(centre_circle)
    ax2.set_title('Distribución por Sector', fontweight='bold')
    
    # 3. Modalidad
    ax3 = fig.add_subplot(gs[1, 0])
    modalidad = df['MODALIDAD'].value_counts()
    ax3.pie(modalidad.values, labels=modalidad.index, autopct='%1.1f%%', 
            startangle=90, colors=['#45B7D1', '#96CEB4', '#FECA57'])
    ax3.set_title('Modalidad de Estudio', fontweight='bold')
    
    # 4. Reconocimiento del Ministerio
    ax4 = fig.add_subplot(gs[1, 1])
    reconocimiento = df['RECONOCIMIENTO_DEL_MINISTERIO'].value_counts()
    ax4.pie(reconocimiento.values, labels=reconocimiento.index, autopct='%1.1f%%',
            startangle=90, colors=['#FF6B6B', '#4ECDC4'])
    ax4.set_title('Reconocimiento del Ministerio', fontweight='bold')
    
    # 5. Top 10 Ciudades
    ax5 = fig.add_subplot(gs[1, 2])
    ciudades = df['MUNICIPIO_OFERTA_PROGRAMA'].value_counts().head(10)
    ax5.bar(range(len(ciudades)), ciudades.values, color='lightcoral')
    ax5.set_xticks(range(len(ciudades)))
    ax5.set_xticklabels(ciudades.index, rotation=45, ha='right')
    ax5.set_title('Top 10 Ciudades', fontweight='bold')
    ax5.set_ylabel('Programas')
    
    # 6. Distribución por Periodicidad
    ax6 = fig.add_subplot(gs[2, 0])
    periodicidad = df['PERIODICIDAD'].value_counts()
    ax6.bar(periodicidad.index, periodicidad.values, color='lightgreen')
    ax6.set_title('Periodicidad de Programas', fontweight='bold')
    ax6.set_ylabel('Número de Programas')
    ax6.tick_params(axis='x', rotation=45)
    
    # 7. Distribución por Créditos (si existe la columna)
    ax7 = fig.add_subplot(gs[2, 1])
    if 'NÚMERO_CRÉDITOS' in df.columns:
        creditos = df['NÚMERO_CRÉDITOS'].dropna()
        ax7.hist(creditos, bins=10, color='skyblue', alpha=0.7, edgecolor='black')
        ax7.set_title('Distribución por Número de Créditos', fontweight='bold')
        ax7.set_xlabel('Número de Créditos')
        ax7.set_ylabel('Frecuencia')
    
    # 8. Comparación Región vs Sector
    ax8 = fig.add_subplot(gs[2, 2])
    crosstab = pd.crosstab(df['REGION'], df['SECTOR'])
    crosstab.plot(kind='bar', ax=ax8, color=['#FF6B6B', '#4ECDC4'])
    ax8.set_title('Región vs Sector', fontweight='bold')
    ax8.set_ylabel('Número de Programas')
    ax8.tick_params(axis='x', rotation=45)
    ax8.legend(title='Sector')
    
    plt.suptitle('Dashboard Nacional - Distribución de Programas de Benchmarking', 
                 fontsize=20, fontweight='bold', y=0.98)
    
    plt.savefig('graficas/dashboard_nacional_completo.png', dpi=300, bbox_inches='tight')
    plt.close()
    print("Dashboard nacional guardado: dashboard_nacional_completo.png")

def crear_analisis_institucional(df):
    """Crear análisis de distribución por instituciones"""
    fig, ((ax1, ax2), (ax3, ax4)) = plt.subplots(2, 2, figsize=(16, 12))
    
    # 1. Top 10 Instituciones con más programas
    instituciones = df['NOMBRE_INSTITUCIÓN'].value_counts().head(10)
    ax1.barh(range(len(instituciones)), instituciones.values, color='lightblue')
    ax1.set_yticks(range(len(instituciones)))
    ax1.set_yticklabels(instituciones.index, fontsize=10)
    ax1.set_title('Top 10 Instituciones con Más Programas', fontweight='bold')
    ax1.set_xlabel('Número de Programas')
    
    # Agregar valores
    for i, v in enumerate(instituciones.values):
        ax1.text(v + 0.05, i, str(v), va='center', fontweight='bold')
    
    # 2. Distribución Sector por Institución (solo top 5)
    top_inst = instituciones.head(5).index
    df_top = df[df['NOMBRE_INSTITUCIÓN'].isin(top_inst)]
    sector_inst = pd.crosstab(df_top['NOMBRE_INSTITUCIÓN'], df_top['SECTOR'])
    sector_inst.plot(kind='bar', ax=ax2, color=['#FF6B6B', '#4ECDC4'])
    ax2.set_title('Sector por Institución (Top 5)', fontweight='bold')
    ax2.set_ylabel('Número de Programas')
    ax2.tick_params(axis='x', rotation=45)
    ax2.legend(title='Sector')
    
    # 3. Distribución geográfica de instituciones
    inst_region = df.groupby('REGION')['NOMBRE_INSTITUCIÓN'].nunique().sort_values(ascending=False)
    ax3.bar(range(len(inst_region)), inst_region.values, color='lightgreen')
    ax3.set_xticks(range(len(inst_region)))
    ax3.set_xticklabels(inst_region.index, rotation=45, ha='right')
    ax3.set_title('Número de Instituciones por Región', fontweight='bold')
    ax3.set_ylabel('Número de Instituciones Únicas')
    
    # 4. Promedio de programas por institución por región
    prog_por_inst = df.groupby('REGION').agg({
        'NOMBRE_INSTITUCIÓN': 'nunique',
        'NOMBRE_DEL_PROGRAMA': 'count'
    })
    prog_por_inst['Promedio'] = prog_por_inst['NOMBRE_DEL_PROGRAMA'] / prog_por_inst['NOMBRE_INSTITUCIÓN']
    
    ax4.bar(range(len(prog_por_inst)), prog_por_inst['Promedio'], color='coral')
    ax4.set_xticks(range(len(prog_por_inst)))
    ax4.set_xticklabels(prog_por_inst.index, rotation=45, ha='right')
    ax4.set_title('Promedio de Programas por Institución por Región', fontweight='bold')
    ax4.set_ylabel('Promedio de Programas')
    
    plt.suptitle('Análisis Institucional - Distribución de Programas', 
                 fontsize=16, fontweight='bold')
    plt.tight_layout()
    plt.savefig('graficas/analisis_institucional.png', dpi=300, bbox_inches='tight')
    plt.close()
    print("Análisis institucional guardado: analisis_institucional.png")

def crear_matriz_correlacion(df):
    """Crear matriz de correlación entre variables categóricas"""
    fig, ax = plt.subplots(figsize=(12, 8))
    
    # Crear tabla de contingencia múltiple
    categoricas = ['REGION', 'SECTOR', 'MODALIDAD', 'RECONOCIMIENTO_DEL_MINISTERIO']
    
    # Crear una matriz de co-ocurrencia
    matriz_datos = []
    labels = []
    
    for cat in categoricas:
        valores = df[cat].value_counts()
        matriz_datos.append(valores.values)
        labels.extend([f"{cat}_{val}" for val in valores.index])
    
    # Crear heatmap de distribución
    datos_regiones = []
    for region in df['REGION'].unique():
        fila = []
        df_region = df[df['REGION'] == region]
        
        # Sector
        fila.extend([
            len(df_region[df_region['SECTOR'] == 'Oficial']),
            len(df_region[df_region['SECTOR'] == 'Privado'])
        ])
        
        # Modalidad
        fila.extend([
            len(df_region[df_region['MODALIDAD'] == 'Presencial']),
            len(df_region[df_region['MODALIDAD'] == 'Virtual']),
            len(df_region[df_region['MODALIDAD'] == 'A distancia'])
        ])
        
        # Reconocimiento
        fila.extend([
            len(df_region[df_region['RECONOCIMIENTO_DEL_MINISTERIO'] == 'Registro calificado']),
            len(df_region[df_region['RECONOCIMIENTO_DEL_MINISTERIO'] == 'Acreditación de alta calidad'])
        ])
        
        datos_regiones.append(fila)
    
    columnas = ['Oficial', 'Privado', 'Presencial', 'Virtual', 'A distancia', 
                'Registro calificado', 'Acreditación alta calidad']
    
    df_matriz = pd.DataFrame(datos_regiones, 
                            index=df['REGION'].unique(), 
                            columns=columnas)
    
    sns.heatmap(df_matriz, annot=True, fmt='d', cmap='YlOrRd', ax=ax)
    ax.set_title('Matriz de Distribución: Regiones vs Características', fontweight='bold', fontsize=14)
    ax.set_ylabel('Regiones')
    ax.set_xlabel('Características')
    
    plt.tight_layout()
    plt.savefig('graficas/matriz_distribucion_regiones.png', dpi=300, bbox_inches='tight')
    plt.close()
    print("Matriz de distribución guardada: matriz_distribucion_regiones.png")

def main():
    """Función principal para generar análisis adicionales"""
    print("Generando análisis nacional detallado...")
    
    # Cargar datos
    df = cargar_datos()
    
    # Generar análisis
    crear_mapa_distribucional_nacional(df)
    crear_analisis_institucional(df)
    crear_matriz_correlacion(df)
    
    print("\n✅ Análisis nacional detallado completado!")
    print("📊 Se generaron 3 gráficas adicionales en la carpeta 'graficas/'")

if __name__ == "__main__":
    main()
