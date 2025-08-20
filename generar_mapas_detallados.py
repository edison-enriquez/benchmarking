#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Mapa Detallado de Colombia - Versión Matplotlib
Genera mapas estáticos detallados usando solo matplotlib y datos de coordenadas
"""

import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
from datetime import datetime
import matplotlib.patches as patches

# Configuración de estilo
plt.style.use('default')
plt.rcParams['figure.figsize'] = (18, 14)
plt.rcParams['font.size'] = 10

def obtener_coordenadas_completas():
    """Coordenadas detalladas de ciudades colombianas por región"""
    coordenadas_por_region = {
        'Región Bogotá': {
            'color': '#E74C3C',
            'ciudades': {
                'Bogotá, D.C.': (4.7110, -74.0721),
                'Facatativá': (4.8144, -74.3547)
            }
        },
        'Región Centro Occidente': {
            'color': '#3498DB',
            'ciudades': {
                'Medellín': (6.2442, -75.5812),
                'Manizales': (5.0703, -75.5138),
                'Armenia': (4.5339, -75.6811),
                'Dosquebradas': (4.8386, -75.6736),
                'Ibagué': (4.4389, -75.2322),
                'Espinal': (4.1489, -74.8836),
                'Neiva': (2.9273, -75.2819)
            }
        },
        'Región Occidente': {
            'color': '#2ECC71',
            'ciudades': {
                'Santiago de Cali': (3.4516, -76.5320),
                'Palmira': (3.5394, -76.3036),
                'Buenaventura': (3.8801, -77.0313),
                'Tuluá': (4.0845, -76.1955),
                'Cartago': (4.7467, -75.9111),
                'Zarzal': (4.3989, -76.0747),
                'Popayán': (2.4448, -76.6147),
                'Pasto': (1.2136, -77.2811)
            }
        },
        'Región Centro Oriente': {
            'color': '#F39C12',
            'ciudades': {
                'Bucaramanga': (7.1193, -73.1227),
                'Sogamoso': (5.7081, -72.9342),
                'Girón': (7.0669, -73.1692),
                'San José de Cúcuta': (7.8939, -72.5078)
            }
        },
        'Región Norte': {
            'color': '#9B59B6',
            'ciudades': {
                'Barranquilla': (10.9685, -74.7813),
                'Cartagena de Indias': (10.3997, -75.5144),
                'Riohacha': (11.5444, -72.9072)
            }
        },
        'Región Sur Oriente': {
            'color': '#E67E22',
            'ciudades': {
                'Villavicencio': (4.1420, -73.6266)
            }
        }
    }
    return coordenadas_por_region

def cargar_estadisticas_regionales():
    """Cargar estadísticas de cada región"""
    return {
        'Región Centro Occidente': {
            'instituciones': 7,
            'programas': 11,
            'estudiantes': 10208
        },
        'Región Bogotá': {
            'instituciones': 10,
            'programas': 11,
            'estudiantes': 9246
        },
        'Región Norte': {
            'instituciones': 3,
            'programas': 5,
            'estudiantes': 4977
        },
        'Región Occidente': {
            'instituciones': 5,
            'programas': 12,
            'estudiantes': 3578
        },
        'Región Centro Oriente': {
            'instituciones': 3,
            'programas': 5,
            'estudiantes': 3032
        },
        'Región Sur Oriente': {
            'instituciones': 1,
            'programas': 2,
            'estudiantes': 1191
        }
    }

def crear_mapa_principal_colombia():
    """Crear el mapa principal de Colombia con todas las regiones"""
    fig, ax = plt.subplots(figsize=(16, 20))
    
    coordenadas_regiones = obtener_coordenadas_completas()
    stats = cargar_estadisticas_regionales()
    
    # Configurar límites del mapa (Colombia)
    ax.set_xlim(-79, -66)
    ax.set_ylim(-1, 13)
    
    # Dibujar contorno aproximado de Colombia
    colombia_x = [-79, -78, -77, -75, -73, -71, -69, -67, -66, -66, -67, -68, -70, -72, -74, -76, -78, -79, -79]
    colombia_y = [12, 11, 10, 11, 12, 11, 10, 8, 6, 4, 2, 0, -1, 1, 3, 5, 8, 10, 12]
    ax.plot(colombia_x, colombia_y, 'k-', linewidth=2, alpha=0.3)
    
    # Crear diccionario para leyenda
    handles_leyenda = []
    
    # Plotear ciudades por región
    for region, info in coordenadas_regiones.items():
        color = info['color']
        ciudades = info['ciudades']
        
        if region in stats:
            region_stats = stats[region]
            
            # Crear listas de coordenadas
            lats, lons = [], []
            nombres_ciudades = []
            
            for ciudad, (lat, lon) in ciudades.items():
                lats.append(lat)
                lons.append(lon)
                nombres_ciudades.append(ciudad)
            
            # Plotear puntos
            scatter = ax.scatter(lons, lats, 
                               c=color, 
                               s=200 + (region_stats['instituciones'] * 30),  # Tamaño proporcional
                               alpha=0.8, 
                               edgecolor='white', 
                               linewidth=2,
                               label=f"{region}")
            
            # Agregar nombres de ciudades
            for i, ciudad in enumerate(nombres_ciudades):
                ax.annotate(ciudad, 
                           (lons[i], lats[i]), 
                           xytext=(5, 5), 
                           textcoords='offset points',
                           fontsize=8, 
                           fontweight='bold',
                           bbox=dict(boxstyle='round,pad=0.3', facecolor='white', alpha=0.7))
            
            # Agregar a leyenda
            handles_leyenda.append(scatter)
    
    # Configurar el mapa
    ax.set_xlabel('Longitud', fontweight='bold', fontsize=12)
    ax.set_ylabel('Latitud', fontweight='bold', fontsize=12)
    ax.set_title('Mapa de Colombia - Distribución Regional de Instituciones de Educación Superior\n' +
                 'Análisis de Programas Activos por Región (2018-2024)', 
                 fontweight='bold', fontsize=16, pad=20)
    
    # Agregar grilla
    ax.grid(True, alpha=0.3, linestyle='--')
    
    # Leyenda personalizada
    legend_elements = []
    for region, info in coordenadas_regiones.items():
        if region in stats:
            region_stats = stats[region]
            legend_elements.append(
                plt.scatter([], [], c=info['color'], s=150, alpha=0.8, edgecolor='white', linewidth=2,
                           label=f"{region}\n{region_stats['instituciones']} inst., {region_stats['programas']} prog.\n{region_stats['estudiantes']:,} estudiantes")
            )
    
    ax.legend(handles=legend_elements, loc='upper left', bbox_to_anchor=(1.02, 1), 
             fontsize=10, title="Regiones", title_fontsize=12)
    
    plt.tight_layout()
    plt.savefig('analisis_distribucion/graficas/mapa_colombia_detallado.png', 
                dpi=300, bbox_inches='tight')
    plt.close()
    print("Mapa detallado guardado: mapa_colombia_detallado.png")

def crear_mapas_por_region():
    """Crear mapas individuales por región"""
    coordenadas_regiones = obtener_coordenadas_completas()
    stats = cargar_estadisticas_regionales()
    
    fig, axes = plt.subplots(2, 3, figsize=(18, 12))
    axes = axes.flatten()
    
    for idx, (region, info) in enumerate(coordenadas_regiones.items()):
        ax = axes[idx]
        color = info['color']
        ciudades = info['ciudades']
        
        if region in stats:
            region_stats = stats[region]
            
            # Obtener coordenadas
            lats, lons = [], []
            nombres_ciudades = []
            
            for ciudad, (lat, lon) in ciudades.items():
                lats.append(lat)
                lons.append(lon)
                nombres_ciudades.append(ciudad.replace(', D.C.', '').replace('Santiago de ', ''))
            
            # Plotear región
            ax.scatter(lons, lats, 
                      c=color, 
                      s=300, 
                      alpha=0.8, 
                      edgecolor='white', 
                      linewidth=2)
            
            # Agregar nombres de ciudades
            for i, ciudad in enumerate(nombres_ciudades):
                ax.annotate(ciudad, 
                           (lons[i], lats[i]), 
                           xytext=(0, 10), 
                           textcoords='offset points',
                           fontsize=9, 
                           fontweight='bold',
                           ha='center',
                           bbox=dict(boxstyle='round,pad=0.3', facecolor='white', alpha=0.8))
            
            # Configurar límites para cada región
            if lons and lats:
                margin = 0.5
                ax.set_xlim(min(lons) - margin, max(lons) + margin)
                ax.set_ylim(min(lats) - margin, max(lats) + margin)
            
            # Título con estadísticas
            ax.set_title(f"{region}\n{region_stats['instituciones']} Instituciones | " +
                        f"{region_stats['programas']} Programas\n{region_stats['estudiantes']:,} Estudiantes",
                        fontweight='bold', fontsize=11, color=color)
            
            ax.grid(True, alpha=0.3)
            ax.set_aspect('equal', adjustable='box')
    
    plt.suptitle('Detalle por Regiones - Instituciones de Educación Superior', 
                 fontsize=16, fontweight='bold', y=0.98)
    plt.tight_layout()
    plt.savefig('analisis_distribucion/graficas/mapa_regiones_detalle.png', 
                dpi=300, bbox_inches='tight')
    plt.close()
    print("Mapa por regiones guardado: mapa_regiones_detalle.png")

def crear_mapa_concentracion():
    """Crear mapa de concentración institucional"""
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(20, 10))
    
    coordenadas_regiones = obtener_coordenadas_completas()
    stats = cargar_estadisticas_regionales()
    
    # Mapa 1: Concentración por instituciones
    for region, info in coordenadas_regiones.items():
        if region in stats:
            color = info['color']
            ciudades = info['ciudades']
            region_stats = stats[region]
            
            lats, lons = [], []
            for ciudad, (lat, lon) in ciudades.items():
                lats.append(lat)
                lons.append(lon)
            
            # Tamaño proporcional al número de instituciones
            size = 100 + (region_stats['instituciones'] * 100)
            
            ax1.scatter(lons, lats, 
                       c=color, 
                       s=size, 
                       alpha=0.7, 
                       edgecolor='black', 
                       linewidth=1)
            
            # Etiqueta con número de instituciones
            if lons and lats:
                center_lon, center_lat = np.mean(lons), np.mean(lats)
                ax1.annotate(f"{region_stats['instituciones']}", 
                            (center_lon, center_lat), 
                            fontsize=12, 
                            fontweight='bold',
                            ha='center', va='center',
                            color='white')
    
    ax1.set_xlim(-79, -66)
    ax1.set_ylim(-1, 13)
    ax1.set_title('Concentración por Número de Instituciones', fontweight='bold', fontsize=14)
    ax1.set_xlabel('Longitud')
    ax1.set_ylabel('Latitud')
    ax1.grid(True, alpha=0.3)
    
    # Mapa 2: Concentración por estudiantes
    for region, info in coordenadas_regiones.items():
        if region in stats:
            color = info['color']
            ciudades = info['ciudades']
            region_stats = stats[region]
            
            lats, lons = [], []
            for ciudad, (lat, lon) in ciudades.items():
                lats.append(lat)
                lons.append(lon)
            
            # Tamaño proporcional al número de estudiantes
            size = 50 + (region_stats['estudiantes'] / 50)
            
            ax2.scatter(lons, lats, 
                       c=color, 
                       s=size, 
                       alpha=0.7, 
                       edgecolor='black', 
                       linewidth=1)
            
            # Etiqueta con número de estudiantes
            if lons and lats:
                center_lon, center_lat = np.mean(lons), np.mean(lats)
                ax2.annotate(f"{region_stats['estudiantes']:,}", 
                            (center_lon, center_lat), 
                            fontsize=10, 
                            fontweight='bold',
                            ha='center', va='center',
                            color='white')
    
    ax2.set_xlim(-79, -66)
    ax2.set_ylim(-1, 13)
    ax2.set_title('Concentración por Número de Estudiantes', fontweight='bold', fontsize=14)
    ax2.set_xlabel('Longitud')
    ax2.set_ylabel('Latitud')
    ax2.grid(True, alpha=0.3)
    
    plt.suptitle('Mapas de Concentración - Instituciones y Estudiantes por Región', 
                 fontsize=16, fontweight='bold')
    plt.tight_layout()
    plt.savefig('analisis_distribucion/graficas/mapa_concentracion.png', 
                dpi=300, bbox_inches='tight')
    plt.close()
    print("Mapa de concentración guardado: mapa_concentracion.png")

def main():
    """Función principal"""
    print("Generando mapas detallados de Colombia...")
    print(f"Fecha: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    # Crear mapas
    crear_mapa_principal_colombia()
    crear_mapas_por_region()
    crear_mapa_concentracion()
    
    print(f"\n✅ Mapas detallados generados exitosamente!")
    print(f"🗺️ Se crearon 3 mapas adicionales en 'analisis_distribucion/graficas/':")
    print(f"   • mapa_colombia_detallado.png")
    print(f"   • mapa_regiones_detalle.png") 
    print(f"   • mapa_concentracion.png")

if __name__ == "__main__":
    main()
