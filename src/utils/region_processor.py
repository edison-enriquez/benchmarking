import pandas as pd
import os
import re
import unicodedata
import datetime
import time
import gc # Garbage Collector

# --- Configuración ---
BASE_INPUT_PATH = './files/'
CSV_CONVERTIDOS_FOLDER = os.path.join(BASE_INPUT_PATH, 'csv_convertidos')
PROGRAMAS_BENCHMARKING_FILE = os.path.join(BASE_INPUT_PATH, 'programas_benchmarking.xlsx') # Ruta al archivo maestro

BASE_OUTPUT_PATH = './resultados/'
POR_REGION_OUTPUT_FOLDER = os.path.join(BASE_OUTPUT_PATH, 'por_region')

# Indicadores que se procesarán (deben coincidir con los nombres de archivo generados por el script anterior)
INDICADORES = ['INSCRITOS', 'ADMITIDOS', 'MATRICULADOS', 'PRIMER_CURSO', 'GRADUADOS']

# Columnas clave que se tomarán del archivo de programas para la base de cada CSV regional
COLUMNAS_BASE_PROGRAMAS = [
    'CODIGO_SNIES_PROGRAMA',
    'INSTITUCION_EDUCACION_SUPERIOR',
    'NOMBRE_DEL_PROGRAMA', # Añadido para más contexto
    'REGION',
    'MUNICIPIO_OFERTA_PROGRAMA'
]

# --- Funciones Auxiliares ---
def normalize_text(text):
    """Normaliza el texto: a mayúsculas, sin acentos, reemplaza espacios con guiones bajos."""
    if not isinstance(text, str):
        text = str(text)
    nfkd_form = unicodedata.normalize('NFKD', text)
    text_without_accents = "".join([c for c in nfkd_form if not unicodedata.combining(c)])
    text_cleaned = re.sub(r'\s+|-', '_', text_without_accents.upper())
    text_cleaned = re.sub(r'[^A-Z0-9_]', '', text_cleaned)
    return text_cleaned.strip('_')

def clean_snies_code(series):
    """Convierte a string, quita espacios y elimina el sufijo .0 si existe."""
    return series.astype(str).str.strip().str.replace(r'\.0$', '', regex=True)

def setup_logging(log_path):
    """Configura el archivo de log."""
    timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    log_file_name = os.path.join(log_path, f'log_procesamiento_por_region_{timestamp}.txt')
    with open(log_file_name, 'w', encoding='utf-8') as f:
        f.write(f"--- INICIO DEL LOG: {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')} ---\n\n")
    return log_file_name

def log_message(log_file, message_type, message):
    """Escribe un mensaje en el archivo de log."""
    print(f"[{message_type}] {message}") # Imprimir también en consola para visibilidad inmediata
    with open(log_file, 'a', encoding='utf-8') as f:
        f.write(f"[{datetime.datetime.now().strftime('%H:%M:%S')}] [{message_type}] {message}\n")

def load_and_prepare_programas(file_path, log_file):
    """Carga y prepara el DataFrame de programas."""
    try:
        log_message(log_file, "INFO", f"Cargando archivo de programas: {file_path}")
        df_programas = pd.read_excel(file_path, sheet_name='Programas', engine='openpyxl')
        
        df_programas.columns = [normalize_text(col) for col in df_programas.columns]
        
        mapeo_columnas_programas = {
            'CODIGO_SNIES_DEL_PROGRAMA': 'CODIGO_SNIES_PROGRAMA',
            'NOMBRE_INSTITUCION': 'INSTITUCION_EDUCACION_SUPERIOR',
            'MUNICIPIO_OFERTA_PROGRAMA': 'MUNICIPIO_OFERTA_PROGRAMA'
        }
        for old_name, new_name in mapeo_columnas_programas.items():
            if old_name in df_programas.columns and new_name not in df_programas.columns:
                 df_programas.rename(columns={old_name: new_name}, inplace=True)
            elif old_name in df_programas.columns and new_name in df_programas.columns and old_name != new_name:
                 log_message(log_file, "DEBUG", f"En df_programas, tanto '{old_name}' como '{new_name}' existen. Se usará '{new_name}'.")

        if 'CODIGO_SNIES_PROGRAMA' not in df_programas.columns:
            log_message(log_file, "ERROR", "La columna 'CODIGO_SNIES_PROGRAMA' es crucial y no se encontró en el archivo de programas después de la normalización.")
            return None
        
        # Limpiar códigos SNIES (string, sin espacios, sin .0)
        df_programas['CODIGO_SNIES_PROGRAMA'] = clean_snies_code(df_programas['CODIGO_SNIES_PROGRAMA'])
        
        for col_base in COLUMNAS_BASE_PROGRAMAS:
            if col_base not in df_programas.columns:
                log_message(log_file, "ERROR", f"Columna base '{col_base}' no encontrada en el archivo de programas. Columnas disponibles: {df_programas.columns.tolist()}")
                return None
        
        log_message(log_file, "INFO", f"Archivo de programas cargado y preparado. Columnas: {df_programas.columns.tolist()}")
        return df_programas
    except Exception as e:
        log_message(log_file, "ERROR", f"No se pudo cargar o preparar el archivo de programas '{file_path}': {e}")
        return None

def process_indicator_file_for_region(indicator_csv_path, df_programas_region, region_name, log_file):
    """
    Procesa un archivo CSV de indicador para una región específica.
    Devuelve un DataFrame (posiblemente vacío) con los datos pivotados.
    """
    data_for_pivot = []
    try:
        log_message(log_file, "DEBUG", f"Intentando leer CSV: {indicator_csv_path}")
        df_indicator_year = pd.read_csv(indicator_csv_path, dtype={'CODIGO_SNIES_PROGRAMA': str}, low_memory=False) # Leer como string inicialmente si es posible
        df_indicator_year.columns = [normalize_text(col) for col in df_indicator_year.columns]
        log_message(log_file, "DEBUG", f"Columnas normalizadas de {os.path.basename(indicator_csv_path)}: {df_indicator_year.columns.tolist()}")

        if 'CODIGO_SNIES_PROGRAMA' not in df_indicator_year.columns or 'ANO' not in df_indicator_year.columns:
            log_message(log_file, "WARNING", f"Archivo {indicator_csv_path} no tiene 'CODIGO_SNIES_PROGRAMA' o 'ANO'. Omitiendo. Columnas encontradas: {df_indicator_year.columns.tolist()}")
            return pd.DataFrame()

        # Limpiar códigos SNIES (string, sin espacios, sin .0) ANTES de filtrar
        df_indicator_year['CODIGO_SNIES_PROGRAMA'] = clean_snies_code(df_indicator_year['CODIGO_SNIES_PROGRAMA'])
        
        # Los códigos en df_programas_region ya están limpios desde load_and_prepare_programas
        codigos_snies_region = df_programas_region['CODIGO_SNIES_PROGRAMA'].unique()

        # --- LOGGING PARA DIAGNÓSTICO (Mantenido) ---
        log_message(log_file, "DIAGNOSTICO", f"Región '{region_name}'. Buscando SNIES (muestra): {codigos_snies_region[:5]} (Total: {len(codigos_snies_region)})")
        snies_in_file = df_indicator_year['CODIGO_SNIES_PROGRAMA'].unique()
        log_message(log_file, "DIAGNOSTICO", f"Archivo '{os.path.basename(indicator_csv_path)}'. SNIES encontrados (limpios, muestra): {snies_in_file[:5]} (Total: {len(snies_in_file)})")
        intersection = set(codigos_snies_region) & set(snies_in_file)
        log_message(log_file, "DIAGNOSTICO", f"Intersección SNIES encontrada: {len(intersection)} códigos.")
        if len(intersection) == 0 and len(codigos_snies_region) > 0 and len(snies_in_file) > 0:
             log_message(log_file, "WARNING", f"¡No hay coincidencia entre los SNIES de la región '{region_name}' y los SNIES del archivo '{os.path.basename(indicator_csv_path)}' después de limpiar!")
        # --- FIN LOGGING DIAGNÓSTICO ---

        df_filtered_by_region = df_indicator_year[df_indicator_year['CODIGO_SNIES_PROGRAMA'].isin(codigos_snies_region)]

        if df_filtered_by_region.empty:
            log_message(log_file, "DEBUG", f"No se encontraron datos para la región '{region_name}' en {indicator_csv_path} después de filtrar por SNIES.")
            return pd.DataFrame()

        # --- Lógica para procesar periodos y valores ---
        potential_value_columns = [col for col in df_filtered_by_region.columns if col not in ['CODIGO_SNIES_PROGRAMA', 'ANO', 'INSTITUCION_EDUCACION_SUPERIOR', 'NOMBRE_DEL_PROGRAMA', 'REGION', 'MUNICIPIO_OFERTA_PROGRAMA']]
        
        period_columns = [col for col in df_filtered_by_region.columns if re.match(r'^\d{4}_\d{1,2}$', col)]
        
        if not period_columns:
            col_semestre = next((col for col in df_filtered_by_region.columns if 'SEMESTRE' in col.upper() or 'PERIODO' in col.upper() and col.upper() != 'PERIODO_ACADEMICO'), None)
            col_valor_indicador = potential_value_columns[-1] if potential_value_columns else None

            if col_semestre and col_valor_indicador and 'ANO' in df_filtered_by_region.columns:
                 # Corregido: Mover el log message DENTRO del bucle o quitar la dependencia de 'row'
                log_message(log_file, "INFO", f"Construyendo periodos desde ANO, {col_semestre}, y valor en {col_valor_indicador} para {indicator_csv_path}")
                for _, row in df_filtered_by_region.iterrows():
                    try: 
                        periodo_val_str = str(row[col_semestre])
                        periodo_val_norm = normalize_text(periodo_val_str)
                        
                        if periodo_val_norm.isdigit():
                             periodo_final = f"{int(row['ANO'])}_{periodo_val_norm}" 
                        else: 
                            match_num = re.search(r'\d+', periodo_val_norm)
                            if match_num:
                                periodo_final = f"{int(row['ANO'])}_{match_num.group(0)}"
                            else: 
                                log_message(log_file, "DEBUG", f"No se pudo extraer número del periodo '{periodo_val_str}' (normalizado: '{periodo_val_norm}'). Usando primeros 3 caracteres.")
                                periodo_final = f"{int(row['ANO'])}_{periodo_val_norm[:3]}"
                        
                        codigo_snies = row['CODIGO_SNIES_PROGRAMA'] # Ya está limpio
                        valor = row[col_valor_indicador]
                        data_for_pivot.append({'CODIGO_SNIES_PROGRAMA': codigo_snies, 'PERIODO': periodo_final, 'VALOR': valor})
                    except Exception as e_period:
                        log_message(log_file, "WARNING", f"Error al normalizar periodo para fila en {indicator_csv_path}. Fila: {row.to_dict()}. Error: {e_period}") # Loguear la fila
                        continue 
            else:
                log_message(log_file, "WARNING", f"No se pudieron identificar columnas de periodo claras o de semestre/valor en {indicator_csv_path}. Columnas disponibles: {df_filtered_by_region.columns.tolist()}. Semestre detectado: {col_semestre}. Valor detectado: {col_valor_indicador}.")
                return pd.DataFrame()
        else: 
            log_message(log_file, "INFO", f"Usando columnas de periodo preexistentes: {period_columns} para {indicator_csv_path}")
            for _, row in df_filtered_by_region.iterrows():
                codigo_snies = row['CODIGO_SNIES_PROGRAMA'] # Ya está limpio
                for periodo_col in period_columns:
                    valor = 0
                    try:
                        valor = pd.to_numeric(row[periodo_col], errors='coerce')
                        if pd.isna(valor):
                            valor = 0 
                    except Exception:
                        valor = 0 
                        
                    data_for_pivot.append({'CODIGO_SNIES_PROGRAMA': codigo_snies, 'PERIODO': periodo_col, 'VALOR': valor})

        if not data_for_pivot:
            log_message(log_file, "DEBUG", f"No se generaron datos para pivotar desde {indicator_csv_path} para la región {region_name}.")
            return pd.DataFrame()
            
        df_pivot_ready = pd.DataFrame(data_for_pivot)
        
        df_pivot_ready['VALOR'] = pd.to_numeric(df_pivot_ready['VALOR'], errors='coerce').fillna(0)
        
        pivot_table = pd.pivot_table(df_pivot_ready,
                                     index='CODIGO_SNIES_PROGRAMA',
                                     columns='PERIODO',
                                     values='VALOR',
                                     aggfunc='sum').reset_index() 
        log_message(log_file, "DEBUG", f"Tabla pivote generada para {indicator_csv_path}, región {region_name}. Columnas: {pivot_table.columns.tolist()}")
        return pivot_table

    except pd.errors.EmptyDataError:
        log_message(log_file, "WARNING", f"Archivo CSV vacío o mal formado: {indicator_csv_path}")
        return pd.DataFrame()
    except Exception as e:
        log_message(log_file, "ERROR", f"Error general procesando {indicator_csv_path} para región '{region_name}': {e}")
        import traceback
        log_file_handle = open(log_file,'a')
        traceback.print_exc(file=log_file_handle)
        log_file_handle.close()
        return pd.DataFrame()

# --- Script Principal ---
def main():
    os.makedirs(BASE_OUTPUT_PATH, exist_ok=True)
    os.makedirs(POR_REGION_OUTPUT_FOLDER, exist_ok=True)
    
    log_file = setup_logging(BASE_OUTPUT_PATH)
    log_message(log_file, "INFO", "--- Iniciando Script de Generación de CSVs por Región e Indicador (v3) ---")

    df_programas_base = load_and_prepare_programas(PROGRAMAS_BENCHMARKING_FILE, log_file)
    if df_programas_base is None:
        log_message(log_file, "CRITICAL", "No se pudo cargar el archivo de programas. Abortando.")
        return

    if 'REGION' not in df_programas_base.columns:
        log_message(log_file, "CRITICAL", "Columna 'REGION' no encontrada en el archivo de programas. Abortando.")
        return
        
    regiones = df_programas_base['REGION'].dropna().unique().tolist()
    log_message(log_file, "INFO", f"Regiones detectadas: {regiones}")

    if not os.path.exists(CSV_CONVERTIDOS_FOLDER):
        log_message(log_file, "CRITICAL", f"La carpeta de CSV convertidos '{CSV_CONVERTIDOS_FOLDER}' no existe. Ejecute primero el script de conversión XLSX a CSV.")
        return
        
    available_years = sorted([d for d in os.listdir(CSV_CONVERTIDOS_FOLDER) if os.path.isdir(os.path.join(CSV_CONVERTIDOS_FOLDER, d)) and d.isdigit()])
    if not available_years:
        log_message(log_file, "CRITICAL", f"No se encontraron carpetas de años en '{CSV_CONVERTIDOS_FOLDER}'. Abortando.")
        return
    log_message(log_file, "INFO", f"Años con datos CSV detectados en '{CSV_CONVERTIDOS_FOLDER}': {available_years}")
    if '2023' not in available_years:
        log_message(log_file, "WARNING", "El año 2023 NO está entre los años detectados para procesar.")


    for indicador in INDICADORES:
        log_message(log_file, "INFO", f"--- Procesando Indicador: {indicador} ---")
        for region in regiones:
            region_normalized_for_filename = normalize_text(region)
            log_message(log_file, "INFO", f"  Procesando Región: {region} (Archivo: {region_normalized_for_filename})")

            df_programas_region_actual = df_programas_base[df_programas_base['REGION'] == region][COLUMNAS_BASE_PROGRAMAS].copy()
            
            if df_programas_region_actual.empty:
                log_message(log_file, "DEBUG", f"    No hay programas definidos para la región '{region}' en el archivo de benchmarking. Omitiendo esta región para el indicador {indicador}.")
                continue

            df_region_indicador_consolidado = df_programas_region_actual.copy()
            # Limpiar SNIES aquí también por si acaso
            df_region_indicador_consolidado['CODIGO_SNIES_PROGRAMA'] = clean_snies_code(df_region_indicador_consolidado['CODIGO_SNIES_PROGRAMA'])


            for year in available_years:
                log_message(log_file, "DEBUG", f"    Buscando datos para el año: {year}")
                indicator_csv_file = os.path.join(CSV_CONVERTIDOS_FOLDER, year, f"{indicador}.csv")
                
                if not os.path.exists(indicator_csv_file):
                    log_message(log_file, "DEBUG", f"    Archivo no encontrado: {indicator_csv_file}. Omitiendo para este año/indicador/región.")
                    continue
                
                log_message(log_file, "INFO", f"    Procesando archivo de año {year}: {indicator_csv_file}")
                
                df_pivot_year_data = process_indicator_file_for_region(indicator_csv_file, df_programas_region_actual, region, log_file)

                if not df_pivot_year_data.empty:
                    # Limpiar SNIES antes del merge
                    df_pivot_year_data['CODIGO_SNIES_PROGRAMA'] = clean_snies_code(df_pivot_year_data['CODIGO_SNIES_PROGRAMA'])

                    data_cols_from_pivot = df_pivot_year_data.columns.difference(['CODIGO_SNIES_PROGRAMA']).tolist()
                    existing_data_cols = df_region_indicador_consolidado.columns.difference(COLUMNAS_BASE_PROGRAMAS).tolist()
                    overlapping_cols = set(data_cols_from_pivot) & set(existing_data_cols)
                    if overlapping_cols:
                        log_message(log_file, "WARNING", f"    Columnas de periodo duplicadas encontradas al intentar unir datos de {year} para {region}-{indicador}: {overlapping_cols}. Verifique la lógica de periodos o los archivos fuente.")

                    df_region_indicador_consolidado = pd.merge(
                        df_region_indicador_consolidado,
                        df_pivot_year_data, 
                        on='CODIGO_SNIES_PROGRAMA',
                        how='left' 
                    )
                    log_message(log_file, "DEBUG", f"    Datos del año {year} ({indicador}) para región {region} unidos. Columnas actuales: {df_region_indicador_consolidado.columns.tolist()}")
                else:
                     log_message(log_file, "DEBUG", f"    No se generaron datos pivotados desde {indicator_csv_file} para la región {region}.")

                
                del df_pivot_year_data
                gc.collect()
                time.sleep(0.1)

            period_data_columns = df_region_indicador_consolidado.columns.difference(COLUMNAS_BASE_PROGRAMAS).tolist()
            if period_data_columns:
                 # Intentar convertir a Int64 que maneja nulos, luego llenar nulos y convertir a int normal
                 for col in period_data_columns:
                     try:
                         df_region_indicador_consolidado[col] = pd.to_numeric(df_region_indicador_consolidado[col], errors='coerce').fillna(0).astype(int)
                     except Exception as e_conv:
                         log_message(log_file, "WARNING", f"No se pudo convertir la columna de periodo '{col}' a entero para {region}-{indicador}. Error: {e_conv}. Se dejará como está (puede contener floats o NaNs rellenados).")
                         df_region_indicador_consolidado[col] = df_region_indicador_consolidado[col].fillna(0) # Asegurar que NaNs se rellenen

                 log_message(log_file, "DEBUG", f"    Rellenados NaNs con 0 e intentado convertir a entero para columnas de periodo: {period_data_columns}")
            else:
                 log_message(log_file, "WARNING", f"    No se encontraron columnas de datos de periodo para rellenar NaNs para {indicador} - {region}.")


            if df_region_indicador_consolidado.empty:
                 log_message(log_file, "WARNING", f"    El DataFrame consolidado para {indicador} - {region} está vacío antes de guardar. No se guardará archivo.")
                 continue 

            output_file_path = os.path.join(POR_REGION_OUTPUT_FOLDER, f"{region_normalized_for_filename}_{indicador}.csv")
            try:
                final_cols = df_region_indicador_consolidado.columns.tolist()
                if not all(bc in final_cols for bc in COLUMNAS_BASE_PROGRAMAS):
                     log_message(log_file, "ERROR", f"    Faltan columnas base en el DataFrame final para {output_file_path}. Columnas presentes: {final_cols}. No se guardará.")
                     continue

                df_region_indicador_consolidado.to_csv(output_file_path, index=False, encoding='utf-8-sig')
                log_message(log_file, "SUCCESS", f"    Archivo guardado: {output_file_path}")
            except Exception as e_save:
                log_message(log_file, "ERROR", f"    No se pudo guardar el archivo {output_file_path}: {e_save}")
            
            del df_region_indicador_consolidado, df_programas_region_actual 
            gc.collect()

    log_message(log_file, "INFO", "--- Script de Generación de CSVs por Región e Indicador Finalizado ---")

if __name__ == '__main__':
    main()
