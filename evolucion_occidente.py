#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Evolución Temporal de Graduados e Inscritos - Región Occidente
Genera gráficas de evolución temporal específicas para la Región Occidente
"""

import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np
from datetime import datetime
import os

# Configuración de estilo
plt.style.use('default')
sns.set_palette("Set2")
plt.rcParams['figure.figsize'] = (16, 10)
plt.rcParams['font.size'] = 11
plt.rcParams['axes.titlesize'] = 14
plt.rcParams['axes.labelsize'] = 12

def cargar_datos_occidente():
    """Cargar y filtrar datos específicos de la Región Occidente"""
    # Cargar archivos
    df_graduados = pd.read_csv('resultados/Consolidado_GRADUADOS_por_Region.csv')
    df_inscritos = pd.read_csv('resultados/Consolidado_INSCRITOS_por_Region.csv')
    
    # Ciudades de la Región Occidente basadas en el análisis previo
    ciudades_occidente = ['Santiago de Cali', 'Cali', 'Popayán', 'Pasto', 'Palmira', 
                         'Cartago', 'Buenaventura', 'Tuluá', 'Zarzal']
    
    # Filtrar datos de la Región Occidente
    mask_occidente = df_graduados['MUNICIPIO_OFERTA_PROGRAMA'].str.contains(
        '|'.join(ciudades_occidente), na=False)
    
    graduados_occidente = df_graduados[mask_occidente].copy()
    inscritos_occidente = df_inscritos[mask_occidente].copy()
    
    print(f"Programas en Región Occidente - Graduados: {len(graduados_occidente)}")
    print(f"Programas en Región Occidente - Inscritos: {len(inscritos_occidente)}")
    
    return graduados_occidente, inscritos_occidente

def procesar_datos_temporales(graduados_df, inscritos_df):
    """Procesar datos temporales y calcular agregados por período"""
    # Períodos disponibles
    periodos = ['2018_1', '2018_2', '2019_1', '2019_2', '2020_1', '2020_2', 
                '2021_1', '2021_2', '2022_1', '2022_2', '2023_10', '2023_20']
    
    # Calcular totales por período para graduados
    graduados_temporal = {}
    for periodo in periodos:
        if periodo in graduados_df.columns:
            valores = pd.to_numeric(graduados_df[periodo], errors='coerce').fillna(0)
            graduados_temporal[periodo] = valores.sum()
    
    # Calcular totales por período para inscritos
    inscritos_temporal = {}
    for periodo in periodos:
        if periodo in inscritos_df.columns:
            valores = pd.to_numeric(inscritos_df[periodo], errors='coerce').fillna(0)
            inscritos_temporal[periodo] = valores.sum()
    
    return graduados_temporal, inscritos_temporal

def crear_evolucion_temporal_occidente(graduados_temporal, inscritos_temporal):
    """Crear gráfica de evolución temporal para la Región Occidente"""
    fig, ((ax1, ax2), (ax3, ax4)) = plt.subplots(2, 2, figsize=(18, 12))
    
    # Preparar datos
    periodos = list(graduados_temporal.keys())
    graduados_valores = list(graduados_temporal.values())
    inscritos_valores = list(inscritos_temporal.values())
    
    # Convertir períodos a etiquetas más legibles
    etiquetas_periodos = []
    for p in periodos:
        if '_10' in p:
            etiquetas_periodos.append(p.replace('_10', '-I'))
        elif '_20' in p:
            etiquetas_periodos.append(p.replace('_20', '-II'))
        else:
            año = p[:4]
            semestre = 'I' if p.endswith('_1') else 'II'
            etiquetas_periodos.append(f"{año}-{semestre}")
    
    # 1. Evolución temporal combinada
    ax1.plot(etiquetas_periodos, graduados_valores, marker='o', linewidth=3, 
             label='Graduados', color='#2E8B57', markersize=8)
    ax1.plot(etiquetas_periodos, inscritos_valores, marker='s', linewidth=3, 
             label='Inscritos', color='#4169E1', markersize=8)
    
    ax1.set_title('Evolución Temporal - Graduados e Inscritos\nRegión Occidente (2018-2023)', 
                  fontweight='bold', fontsize=16)
    ax1.set_ylabel('Número de Estudiantes', fontweight='bold')
    ax1.set_xlabel('Período', fontweight='bold')
    ax1.legend(fontsize=12)
    ax1.grid(True, alpha=0.3)
    ax1.tick_params(axis='x', rotation=45)
    
    # Agregar valores en los puntos
    for i, (grad, insc) in enumerate(zip(graduados_valores, inscritos_valores)):
        if grad > 0:
            ax1.annotate(f'{int(grad)}', (i, grad), textcoords="offset points", 
                        xytext=(0,10), ha='center', fontsize=9, color='#2E8B57', fontweight='bold')
        if insc > 0:
            ax1.annotate(f'{int(insc)}', (i, insc), textcoords="offset points", 
                        xytext=(0,-15), ha='center', fontsize=9, color='#4169E1', fontweight='bold')
    
    # 2. Graduados por separado
    bars_grad = ax2.bar(etiquetas_periodos, graduados_valores, color='#2E8B57', alpha=0.8)
    ax2.set_title('Evolución de Graduados - Región Occidente', fontweight='bold')
    ax2.set_ylabel('Número de Graduados', fontweight='bold')
    ax2.tick_params(axis='x', rotation=45)
    ax2.grid(axis='y', alpha=0.3)
    
    # Agregar valores en las barras
    for bar in bars_grad:
        height = bar.get_height()
        if height > 0:
            ax2.text(bar.get_x() + bar.get_width()/2., height + 0.5,
                    f'{int(height)}', ha='center', va='bottom', fontweight='bold', fontsize=9)
    
    # 3. Inscritos por separado
    bars_insc = ax3.bar(etiquetas_periodos, inscritos_valores, color='#4169E1', alpha=0.8)
    ax3.set_title('Evolución de Inscritos - Región Occidente', fontweight='bold')
    ax3.set_ylabel('Número de Inscritos', fontweight='bold')
    ax3.tick_params(axis='x', rotation=45)
    ax3.grid(axis='y', alpha=0.3)
    
    # Agregar valores en las barras
    for bar in bars_insc:
        height = bar.get_height()
        if height > 0:
            ax3.text(bar.get_x() + bar.get_width()/2., height + 1,
                    f'{int(height)}', ha='center', va='bottom', fontweight='bold', fontsize=9)
    
    # 4. Ratio Graduados/Inscritos
    ratios = []
    periodos_ratio = []
    for i, (grad, insc) in enumerate(zip(graduados_valores, inscritos_valores)):
        if insc > 0:  # Evitar división por cero
            ratios.append(grad / insc)
            periodos_ratio.append(etiquetas_periodos[i])
    
    if ratios:
        bars_ratio = ax4.bar(periodos_ratio, ratios, color='#FF6347', alpha=0.8)
        ax4.set_title('Ratio Graduados/Inscritos - Región Occidente', fontweight='bold')
        ax4.set_ylabel('Ratio (Graduados/Inscritos)', fontweight='bold')
        ax4.tick_params(axis='x', rotation=45)
        ax4.grid(axis='y', alpha=0.3)
        ax4.axhline(y=1, color='red', linestyle='--', alpha=0.7, label='Ratio = 1')
        ax4.legend()
        
        # Agregar valores en las barras
        for bar in bars_ratio:
            height = bar.get_height()
            ax4.text(bar.get_x() + bar.get_width()/2., height + 0.01,
                    f'{height:.2f}', ha='center', va='bottom', fontweight='bold', fontsize=9)
    
    plt.suptitle('Análisis Temporal Completo - Región Occidente', fontsize=18, fontweight='bold')
    plt.tight_layout()
    plt.savefig('analisis_distribucion/graficas/evolucion_graduados_inscritos_occidente.png', 
                dpi=300, bbox_inches='tight')
    plt.close()
    print("Gráfica guardada: evolucion_graduados_inscritos_occidente.png")

def crear_analisis_por_ciudad(graduados_df, inscritos_df):
    """Crear análisis detallado por ciudad en la Región Occidente"""
    fig, ((ax1, ax2), (ax3, ax4)) = plt.subplots(2, 2, figsize=(18, 12))
    
    # Períodos para análisis
    periodos = ['2018_1', '2018_2', '2019_1', '2019_2', '2020_1', '2020_2', 
                '2021_1', '2021_2', '2022_1', '2022_2', '2023_10', '2023_20']
    
    # 1. Total de graduados por ciudad
    graduados_por_ciudad = {}
    for ciudad in graduados_df['MUNICIPIO_OFERTA_PROGRAMA'].unique():
        if pd.notna(ciudad):
            ciudad_data = graduados_df[graduados_df['MUNICIPIO_OFERTA_PROGRAMA'] == ciudad]
            total = 0
            for periodo in periodos:
                if periodo in ciudad_data.columns:
                    valores = pd.to_numeric(ciudad_data[periodo], errors='coerce').fillna(0)
                    total += valores.sum()
            graduados_por_ciudad[ciudad] = total
    
    # Filtrar ciudades con datos > 0
    graduados_por_ciudad = {k: v for k, v in graduados_por_ciudad.items() if v > 0}
    
    if graduados_por_ciudad:
        ciudades = list(graduados_por_ciudad.keys())
        valores_grad = list(graduados_por_ciudad.values())
        
        bars1 = ax1.barh(ciudades, valores_grad, color='#2E8B57', alpha=0.8)
        ax1.set_title('Total de Graduados por Ciudad (2018-2023)', fontweight='bold')
        ax1.set_xlabel('Total de Graduados', fontweight='bold')
        
        # Agregar valores
        for bar in bars1:
            width = bar.get_width()
            ax1.text(width + 0.5, bar.get_y() + bar.get_height()/2,
                    f'{int(width)}', ha='left', va='center', fontweight='bold')
    
    # 2. Total de inscritos por ciudad
    inscritos_por_ciudad = {}
    for ciudad in inscritos_df['MUNICIPIO_OFERTA_PROGRAMA'].unique():
        if pd.notna(ciudad):
            ciudad_data = inscritos_df[inscritos_df['MUNICIPIO_OFERTA_PROGRAMA'] == ciudad]
            total = 0
            for periodo in periodos:
                if periodo in ciudad_data.columns:
                    valores = pd.to_numeric(ciudad_data[periodo], errors='coerce').fillna(0)
                    total += valores.sum()
            inscritos_por_ciudad[ciudad] = total
    
    # Filtrar ciudades con datos > 0
    inscritos_por_ciudad = {k: v for k, v in inscritos_por_ciudad.items() if v > 0}
    
    if inscritos_por_ciudad:
        ciudades = list(inscritos_por_ciudad.keys())
        valores_insc = list(inscritos_por_ciudad.values())
        
        bars2 = ax2.barh(ciudades, valores_insc, color='#4169E1', alpha=0.8)
        ax2.set_title('Total de Inscritos por Ciudad (2018-2023)', fontweight='bold')
        ax2.set_xlabel('Total de Inscritos', fontweight='bold')
        
        # Agregar valores
        for bar in bars2:
            width = bar.get_width()
            ax2.text(width + 1, bar.get_y() + bar.get_height()/2,
                    f'{int(width)}', ha='left', va='center', fontweight='bold')
    
    # 3. Comparación graduados vs inscritos por ciudad
    ciudades_comunes = set(graduados_por_ciudad.keys()) & set(inscritos_por_ciudad.keys())
    if ciudades_comunes:
        ciudades_comp = list(ciudades_comunes)
        grad_comp = [graduados_por_ciudad[c] for c in ciudades_comp]
        insc_comp = [inscritos_por_ciudad[c] for c in ciudades_comp]
        
        x = np.arange(len(ciudades_comp))
        width = 0.35
        
        ax3.bar(x - width/2, grad_comp, width, label='Graduados', color='#2E8B57', alpha=0.8)
        ax3.bar(x + width/2, insc_comp, width, label='Inscritos', color='#4169E1', alpha=0.8)
        
        ax3.set_title('Comparación Graduados vs Inscritos por Ciudad', fontweight='bold')
        ax3.set_ylabel('Número de Estudiantes', fontweight='bold')
        ax3.set_xticks(x)
        ax3.set_xticklabels(ciudades_comp, rotation=45, ha='right')
        ax3.legend()
        ax3.grid(axis='y', alpha=0.3)
    
    # 4. Distribución porcentual de graduados
    if graduados_por_ciudad:
        ax4.pie(graduados_por_ciudad.values(), labels=graduados_por_ciudad.keys(), 
                autopct='%1.1f%%', startangle=90)
        ax4.set_title('Distribución de Graduados por Ciudad', fontweight='bold')
    
    plt.suptitle('Análisis por Ciudad - Región Occidente', fontsize=16, fontweight='bold')
    plt.tight_layout()
    plt.savefig('analisis_distribucion/graficas/analisis_ciudades_occidente.png', 
                dpi=300, bbox_inches='tight')
    plt.close()
    print("Gráfica guardada: analisis_ciudades_occidente.png")

def generar_estadisticas_occidente(graduados_temporal, inscritos_temporal):
    """Generar estadísticas detalladas de la Región Occidente"""
    print("\n" + "="*70)
    print("ESTADÍSTICAS DETALLADAS - REGIÓN OCCIDENTE")
    print("="*70)
    
    total_graduados = sum(graduados_temporal.values())
    total_inscritos = sum(inscritos_temporal.values())
    
    print(f"\nRESUMEN GENERAL:")
    print(f"  Total de graduados (2018-2023): {int(total_graduados):,}")
    print(f"  Total de inscritos (2018-2023): {int(total_inscritos):,}")
    print(f"  Ratio general graduados/inscritos: {total_graduados/total_inscritos:.3f}")
    
    print(f"\nEVOLUCIÓN POR PERÍODO:")
    print(f"{'Período':<12} {'Graduados':<12} {'Inscritos':<12} {'Ratio':<8}")
    print("-" * 50)
    
    for periodo in graduados_temporal.keys():
        grad = graduados_temporal[periodo]
        insc = inscritos_temporal[periodo]
        ratio = grad/insc if insc > 0 else 0
        
        # Formato del período
        if '_10' in periodo:
            periodo_label = periodo.replace('_10', '-I')
        elif '_20' in periodo:
            periodo_label = periodo.replace('_20', '-II')
        else:
            año = periodo[:4]
            semestre = 'I' if periodo.endswith('_1') else 'II'
            periodo_label = f"{año}-{semestre}"
        
        print(f"{periodo_label:<12} {int(grad):<12} {int(insc):<12} {ratio:.3f}")
    
    # Análisis de tendencias
    periodos_list = list(graduados_temporal.keys())
    if len(periodos_list) >= 4:
        primera_mitad_grad = sum(list(graduados_temporal.values())[:len(periodos_list)//2])
        segunda_mitad_grad = sum(list(graduados_temporal.values())[len(periodos_list)//2:])
        
        primera_mitad_insc = sum(list(inscritos_temporal.values())[:len(periodos_list)//2])
        segunda_mitad_insc = sum(list(inscritos_temporal.values())[len(periodos_list)//2:])
        
        cambio_grad = ((segunda_mitad_grad - primera_mitad_grad) / primera_mitad_grad * 100) if primera_mitad_grad > 0 else 0
        cambio_insc = ((segunda_mitad_insc - primera_mitad_insc) / primera_mitad_insc * 100) if primera_mitad_insc > 0 else 0
        
        print(f"\nANÁLISIS DE TENDENCIAS:")
        print(f"  Graduados - Primera mitad: {int(primera_mitad_grad):,}")
        print(f"  Graduados - Segunda mitad: {int(segunda_mitad_grad):,}")
        print(f"  Cambio porcentual graduados: {cambio_grad:+.1f}%")
        print(f"  Inscritos - Primera mitad: {int(primera_mitad_insc):,}")
        print(f"  Inscritos - Segunda mitad: {int(segunda_mitad_insc):,}")
        print(f"  Cambio porcentual inscritos: {cambio_insc:+.1f}%")

def main():
    """Función principal"""
    print("Iniciando análisis de evolución temporal - Región Occidente...")
    print(f"Fecha: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    # Crear directorio si no existe
    os.makedirs('analisis_distribucion/graficas', exist_ok=True)
    
    # Cargar datos
    graduados_df, inscritos_df = cargar_datos_occidente()
    
    if len(graduados_df) == 0 or len(inscritos_df) == 0:
        print("No se encontraron datos para la Región Occidente.")
        return
    
    # Procesar datos temporales
    graduados_temporal, inscritos_temporal = procesar_datos_temporales(graduados_df, inscritos_df)
    
    # Generar visualizaciones
    crear_evolucion_temporal_occidente(graduados_temporal, inscritos_temporal)
    crear_analisis_por_ciudad(graduados_df, inscritos_df)
    
    # Generar estadísticas
    generar_estadisticas_occidente(graduados_temporal, inscritos_temporal)
    
    print(f"\n✅ Análisis completado exitosamente!")
    print(f"📊 Se generaron 2 gráficas específicas para la Región Occidente")
    print(f"📈 Analizados {len(graduados_df)} programas con datos de graduados e inscritos")

if __name__ == "__main__":
    main()
