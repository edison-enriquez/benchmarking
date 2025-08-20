#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Mapa de Colombia - Análisis Regional de Instituciones Educativas
Genera mapas interactivos y estáticos mostrando regiones y distribución institucional
"""

import pandas as pd
import matplotlib.pyplot as plt
import folium
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import numpy as np
from datetime import datetime
import json
import requests
from geopy.geocoders import Nominatim
import time

# Configuración
plt.style.use('default')
plt.rcParams['figure.figsize'] = (16, 12)
plt.rcParams['font.size'] = 10

def obtener_coordenadas_ciudades():
    """Obtener coordenadas de las principales ciudades colombianas"""
    coordenadas_colombia = {
        'Bogotá, D.C.': (4.7110, -74.0721),
        'Medellín': (6.2442, -75.5812),
        'Santiago de Cali': (3.4516, -76.5320),
        'Barranquilla': (10.9685, -74.7813),
        'Cartagena de Indias': (10.3997, -75.5144),
        'Bucaramanga': (7.1193, -73.1227),
        'Manizales': (5.0703, -75.5138),
        'Armenia': (4.5339, -75.6811),
        'Ibagué': (4.4389, -75.2322),
        'Neiva': (2.9273, -75.2819),
        'Popayán': (2.4448, -76.6147),
        'Pasto': (1.2136, -77.2811),
        'Villavicencio': (4.1420, -73.6266),
        'Palmira': (3.5394, -76.3036),
        'Buenaventura': (3.8801, -77.0313),
        'Tuluá': (4.0845, -76.1955),
        'Cartago': (4.7467, -75.9111),
        'Zarzal': (4.3989, -76.0747),
        'Espinal': (4.1489, -74.8836),
        'Dosquebradas': (4.8386, -75.6736),
        'Sogamoso': (5.7081, -72.9342),
        'Girón': (7.0669, -73.1692),
        'Facatativá': (4.8144, -74.3547),
        'San José de Cúcuta': (7.8939, -72.5078),
        'Riohacha': (11.5444, -72.9072)
    }
    return coordenadas_colombia

def definir_regiones_colombia():
    """Definir las regiones de Colombia según el análisis"""
    regiones_info = {
        'Región Bogotá': {
            'color': '#E74C3C',
            'departamentos': ['Bogotá, D.C.', 'Cundinamarca'],
            'ciudades': ['Bogotá, D.C.', 'Facatativá'],
            'descripcion': 'Región Capital'
        },
        'Región Centro Occidente': {
            'color': '#3498DB',
            'departamentos': ['Antioquia', 'Caldas', 'Risaralda', 'Quindío'],
            'ciudades': ['Medellín', 'Manizales', 'Armenia', 'Dosquebradas'],
            'descripcion': 'Eje Cafetero y Antioquia'
        },
        'Región Occidente': {
            'color': '#2ECC71',
            'departamentos': ['Valle del Cauca', 'Cauca', 'Nariño'],
            'ciudades': ['Santiago de Cali', 'Palmira', 'Buenaventura', 'Tuluá', 'Cartago', 'Zarzal', 'Popayán', 'Pasto'],
            'descripcion': 'Región Pacífico'
        },
        'Región Centro Oriente': {
            'color': '#F39C12',
            'departamentos': ['Santander', 'Boyacá', 'Norte de Santander'],
            'ciudades': ['Bucaramanga', 'Sogamoso', 'Girón', 'San José de Cúcuta'],
            'descripcion': 'Región Oriental'
        },
        'Región Norte': {
            'color': '#9B59B6',
            'departamentos': ['Atlántico', 'Bolívar', 'La Guajira'],
            'ciudades': ['Barranquilla', 'Cartagena de Indias', 'Riohacha'],
            'descripcion': 'Región Caribe'
        },
        'Región Sur Oriente': {
            'color': '#E67E22',
            'departamentos': ['Meta', 'Tolima', 'Huila'],
            'ciudades': ['Villavicencio', 'Ibagué', 'Espinal', 'Neiva'],
            'descripcion': 'Región Llanos y Sur'
        }
    }
    return regiones_info

def cargar_y_procesar_datos():
    """Cargar y procesar datos de ambos archivos"""
    # Datos de benchmarking
    df_bench = pd.read_excel('files/programas_benchmarking.xlsx')
    
    # Datos de programas activos
    df_activos = pd.read_excel('resultados/Consolidado_Programas_ACTIVOS.xlsx')
    
    # Procesar datos de programas activos por región
    regiones_activos = {}
    current_region = None
    
    for idx, row in df_activos.iterrows():
        codigo = str(row['CODIGO_SNIES_PROGRAMA'])
        
        if 'REGIÓN' in codigo:
            current_region = codigo.replace('REGIÓN: ', '').replace(' (ACTIVOS)', '')
            regiones_activos[current_region] = []
        elif current_region and not pd.isna(row['CODIGO_SNIES_PROGRAMA']) and codigo != 'nan':
            regiones_activos[current_region].append(row)
    
    # Calcular estadísticas por región
    stats_regiones = {}
    periodos = ['2018_1', '2018_2', '2019_1', '2019_2', '2020_1', '2020_2', 
                '2021_1', '2021_2', '2022_1', '2022_2', '2023_1', '2023_2', 
                '2024_1', '2024_2']
    
    for region, programas in regiones_activos.items():
        if not programas:
            continue
            
        df_region = pd.DataFrame(programas)
        
        # Contar instituciones y programas
        num_instituciones = df_region['INSTITUCION_EDUCACION_SUPERIOR'].nunique()
        num_programas = len(programas)
        
        # Sumar estudiantes por período
        total_estudiantes = 0
        for periodo in periodos:
            if periodo in df_region.columns:
                valores = pd.to_numeric(df_region[periodo], errors='coerce').fillna(0)
                total_estudiantes += valores.sum()
        
        stats_regiones[region] = {
            'instituciones': num_instituciones,
            'programas': num_programas,
            'estudiantes': total_estudiantes,
            'ciudades': df_region['MUNICIPIO_OFERTA_PROGRAMA'].unique().tolist()
        }
    
    return df_bench, stats_regiones

def crear_mapa_folium_colombia(stats_regiones):
    """Crear mapa interactivo con Folium"""
    # Crear mapa centrado en Colombia
    mapa_colombia = folium.Map(
        location=[4.5709, -74.2973],  # Centro de Colombia
        zoom_start=6,
        tiles='OpenStreetMap'
    )
    
    # Obtener coordenadas y regiones
    coordenadas = obtener_coordenadas_ciudades()
    regiones_info = definir_regiones_colombia()
    
    # Agregar marcadores por región
    for region, stats in stats_regiones.items():
        if region in regiones_info:
            color = regiones_info[region]['color']
            
            # Agregar marcadores para cada ciudad de la región
            for ciudad in stats['ciudades']:
                if ciudad in coordenadas:
                    lat, lon = coordenadas[ciudad]
                    
                    # Crear popup con información
                    popup_text = f"""
                    <b>{ciudad}</b><br>
                    Región: {region}<br>
                    Instituciones en región: {stats['instituciones']}<br>
                    Programas en región: {stats['programas']}<br>
                    Estudiantes totales: {int(stats['estudiantes']):,}
                    """
                    
                    folium.CircleMarker(
                        location=[lat, lon],
                        radius=8 + (stats['instituciones'] * 2),  # Tamaño proporcional
                        popup=folium.Popup(popup_text, max_width=300),
                        color='white',
                        fillColor=color,
                        fillOpacity=0.7,
                        weight=2
                    ).add_to(mapa_colombia)
    
    # Agregar leyenda
    legend_html = '''
    <div style="position: fixed; 
                bottom: 50px; left: 50px; width: 300px; height: 200px; 
                background-color: white; z-index:9999; font-size:14px;
                border:2px solid grey; padding: 10px">
    <p><b>Regiones de Colombia</b></p>
    '''
    
    for region, info in regiones_info.items():
        if region in stats_regiones:
            stats = stats_regiones[region]
            legend_html += f'''
            <p><i class="fa fa-circle" style="color:{info['color']}"></i> 
            {region}: {stats['instituciones']} instituciones, {stats['programas']} programas</p>
            '''
    
    legend_html += '</div>'
    mapa_colombia.get_root().html.add_child(folium.Element(legend_html))
    
    # Guardar mapa
    mapa_colombia.save('analisis_distribucion/graficas/mapa_colombia_interactivo.html')
    print("Mapa interactivo guardado: mapa_colombia_interactivo.html")
    
    return mapa_colombia

def crear_mapa_matplotlib(stats_regiones):
    """Crear mapa estático con matplotlib"""
    fig, ((ax1, ax2), (ax3, ax4)) = plt.subplots(2, 2, figsize=(20, 16))
    
    coordenadas = obtener_coordenadas_ciudades()
    regiones_info = definir_regiones_colombia()
    
    # 1. Mapa de ubicación de instituciones
    for region, stats in stats_regiones.items():
        if region in regiones_info:
            color = regiones_info[region]['color']
            
            # Obtener coordenadas de ciudades de la región
            lats, lons, sizes = [], [], []
            for ciudad in stats['ciudades']:
                if ciudad in coordenadas:
                    lat, lon = coordenadas[ciudad]
                    lats.append(lat)
                    lons.append(lon)
                    sizes.append(100 + (stats['instituciones'] * 50))
            
            if lats:
                ax1.scatter(lons, lats, c=color, s=sizes, alpha=0.7, 
                           label=f"{region} ({stats['instituciones']} inst.)", edgecolor='white', linewidth=2)
    
    ax1.set_xlim(-82, -66)
    ax1.set_ylim(-5, 13)
    ax1.set_xlabel('Longitud', fontweight='bold')
    ax1.set_ylabel('Latitud', fontweight='bold')
    ax1.set_title('Distribución Geográfica de Instituciones por Región', fontweight='bold', fontsize=14)
    ax1.legend(bbox_to_anchor=(1.05, 1), loc='upper left')
    ax1.grid(True, alpha=0.3)
    
    # 2. Gráfica de barras por región
    regiones = list(stats_regiones.keys())
    instituciones = [stats_regiones[r]['instituciones'] for r in regiones]
    colores = [regiones_info.get(r, {}).get('color', '#95A5A6') for r in regiones]
    
    bars = ax2.bar(range(len(regiones)), instituciones, color=colores)
    ax2.set_xticks(range(len(regiones)))
    ax2.set_xticklabels(regiones, rotation=45, ha='right')
    ax2.set_ylabel('Número de Instituciones')
    ax2.set_title('Instituciones por Región', fontweight='bold', fontsize=14)
    
    # Agregar valores en las barras
    for i, bar in enumerate(bars):
        height = bar.get_height()
        ax2.text(bar.get_x() + bar.get_width()/2., height + 0.1,
                f'{int(height)}', ha='center', va='bottom', fontweight='bold')
    
    # 3. Estudiantes por región
    estudiantes = [stats_regiones[r]['estudiantes'] for r in regiones]
    bars3 = ax3.bar(range(len(regiones)), estudiantes, color=colores)
    ax3.set_xticks(range(len(regiones)))
    ax3.set_xticklabels(regiones, rotation=45, ha='right')
    ax3.set_ylabel('Total de Estudiantes (2018-2024)')
    ax3.set_title('Estudiantes por Región', fontweight='bold', fontsize=14)
    
    for i, bar in enumerate(bars3):
        height = bar.get_height()
        ax3.text(bar.get_x() + bar.get_width()/2., height + 100,
                f'{int(height):,}', ha='center', va='bottom', fontweight='bold', fontsize=9)
    
    # 4. Relación instituciones vs estudiantes
    ax4.scatter(instituciones, estudiantes, c=colores, s=200, alpha=0.7, edgecolor='white', linewidth=2)
    
    for i, region in enumerate(regiones):
        ax4.annotate(region, (instituciones[i], estudiantes[i]), 
                    xytext=(5, 5), textcoords='offset points', fontsize=9, fontweight='bold')
    
    ax4.set_xlabel('Número de Instituciones')
    ax4.set_ylabel('Total de Estudiantes')
    ax4.set_title('Relación Instituciones vs Estudiantes por Región', fontweight='bold', fontsize=14)
    ax4.grid(True, alpha=0.3)
    
    plt.suptitle('Mapa de Colombia - Análisis Regional de Educación Superior', 
                 fontsize=18, fontweight='bold', y=0.98)
    plt.tight_layout()
    plt.savefig('analisis_distribucion/graficas/mapa_colombia_completo.png', 
                dpi=300, bbox_inches='tight')
    plt.close()
    print("Mapa estático guardado: mapa_colombia_completo.png")

def crear_mapa_plotly(stats_regiones):
    """Crear mapa con Plotly"""
    coordenadas = obtener_coordenadas_ciudades()
    regiones_info = definir_regiones_colombia()
    
    # Preparar datos para Plotly
    ciudades_data = []
    for region, stats in stats_regiones.items():
        if region in regiones_info:
            for ciudad in stats['ciudades']:
                if ciudad in coordenadas:
                    lat, lon = coordenadas[ciudad]
                    ciudades_data.append({
                        'ciudad': ciudad,
                        'region': region,
                        'lat': lat,
                        'lon': lon,
                        'instituciones': stats['instituciones'],
                        'programas': stats['programas'],
                        'estudiantes': stats['estudiantes'],
                        'color': regiones_info[region]['color']
                    })
    
    df_ciudades = pd.DataFrame(ciudades_data)
    
    # Crear mapa con Plotly
    fig = go.Figure()
    
    for region in df_ciudades['region'].unique():
        df_region = df_ciudades[df_ciudades['region'] == region]
        
        fig.add_trace(go.Scattermapbox(
            lat=df_region['lat'],
            lon=df_region['lon'],
            mode='markers',
            marker=dict(
                size=df_region['instituciones'] * 3 + 10,
                color=df_region['color'].iloc[0],
                opacity=0.8
            ),
            text=df_region.apply(lambda x: 
                f"{x['ciudad']}<br>"
                f"Región: {x['region']}<br>"
                f"Instituciones: {x['instituciones']}<br>"
                f"Programas: {x['programas']}<br>"
                f"Estudiantes: {x['estudiantes']:,.0f}", axis=1),
            name=f"{region} ({df_region['instituciones'].iloc[0]} inst.)",
            hovertemplate='%{text}<extra></extra>'
        ))
    
    fig.update_layout(
        title=dict(
            text="Mapa Interactivo de Colombia - Distribución Regional de Instituciones",
            x=0.5,
            font=dict(size=16, family="Arial Black")
        ),
        mapbox=dict(
            style="open-street-map",
            center=dict(lat=4.5709, lon=-74.2973),
            zoom=5.5
        ),
        showlegend=True,
        height=700,
        margin=dict(l=0, r=0, t=50, b=0)
    )
    
    # Guardar como HTML y PNG
    fig.write_html('analisis_distribucion/graficas/mapa_colombia_plotly.html')
    fig.write_image('analisis_distribucion/graficas/mapa_colombia_plotly.png', width=1200, height=800)
    print("Mapa Plotly guardado: mapa_colombia_plotly.html y .png")

def generar_resumen_geografico(stats_regiones):
    """Generar resumen estadístico geográfico"""
    total_instituciones = sum(r['instituciones'] for r in stats_regiones.values())
    total_programas = sum(r['programas'] for r in stats_regiones.values())
    total_estudiantes = sum(r['estudiantes'] for r in stats_regiones.values())
    
    print("\n" + "="*60)
    print("RESUMEN GEOGRÁFICO - DISTRIBUCIÓN INSTITUCIONAL")
    print("="*60)
    
    print(f"\nTOTALES NACIONALES:")
    print(f"  • Regiones analizadas: {len(stats_regiones)}")
    print(f"  • Total instituciones: {total_instituciones}")
    print(f"  • Total programas: {total_programas}")
    print(f"  • Total estudiantes: {total_estudiantes:,.0f}")
    
    print(f"\nDISTRIBUCIÓN POR REGIÓN:")
    
    # Ordenar por número de estudiantes
    regiones_ordenadas = sorted(stats_regiones.items(), 
                               key=lambda x: x[1]['estudiantes'], reverse=True)
    
    for i, (region, stats) in enumerate(regiones_ordenadas, 1):
        porcentaje_est = (stats['estudiantes'] / total_estudiantes) * 100
        porcentaje_inst = (stats['instituciones'] / total_instituciones) * 100
        
        print(f"\n  {i}. {region}")
        print(f"     - Instituciones: {stats['instituciones']} ({porcentaje_inst:.1f}%)")
        print(f"     - Programas: {stats['programas']}")
        print(f"     - Estudiantes: {stats['estudiantes']:,.0f} ({porcentaje_est:.1f}%)")
        print(f"     - Ciudades: {', '.join(stats['ciudades'])}")
    
    return {
        'total_instituciones': total_instituciones,
        'total_programas': total_programas,
        'total_estudiantes': total_estudiantes,
        'regiones_stats': stats_regiones
    }

def main():
    """Función principal"""
    print("Generando mapa de Colombia con distribución regional...")
    print(f"Fecha: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    # Cargar datos
    df_bench, stats_regiones = cargar_y_procesar_datos()
    
    if not stats_regiones:
        print("No se encontraron datos para procesar.")
        return
    
    # Generar mapas
    print("\nGenerando visualizaciones geográficas...")
    
    try:
        crear_mapa_folium_colombia(stats_regiones)
    except Exception as e:
        print(f"Error creando mapa Folium: {e}")
    
    try:
        crear_mapa_matplotlib(stats_regiones)
    except Exception as e:
        print(f"Error creando mapa Matplotlib: {e}")
    
    try:
        crear_mapa_plotly(stats_regiones)
    except Exception as e:
        print(f"Error creando mapa Plotly: {e}")
    
    # Generar resumen
    resumen = generar_resumen_geografico(stats_regiones)
    
    print(f"\n✅ Mapas de Colombia generados exitosamente!")
    print(f"🗺️ Se crearon 3 tipos de mapas en 'analisis_distribucion/graficas/'")
    print(f"📊 Analizadas {len(stats_regiones)} regiones con {resumen['total_instituciones']} instituciones")

if __name__ == "__main__":
    main()
