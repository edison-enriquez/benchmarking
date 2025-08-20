import pandas as pd
import os
import re
import unicodedata
import datetime
import time
import gc

# --- Configuración ---
BASE_INPUT_PATH = './files/'
CSV_CONVERTIDOS_FOLDER = os.path.join(BASE_INPUT_PATH, 'csv_convertidos')
PROGRAMAS_BENCHMARKING_FILE = os.path.join(BASE_INPUT_PATH, 'programas_benchmarking.xlsx')

BASE_OUTPUT_PATH = './resultados/'
OUTPUT_EXCEL_ACTIVOS = 'Consolidado_Programas_ACTIVOS.xlsx'
OUTPUT_EXCEL_INACTIVOS = 'Consolidado_Programas_INACTIVOS.xlsx'

# Indicadores que se procesarán como hojas en el Excel
INDICADORES = ['INSCRITOS', 'ADMITIDOS', 'MATRICULADOS', 'PRIMER_CURSO', 'GRADUADOS']

# Años para determinar si un programa está activo o inactivo (últimos 2 años)
ANOS_EVALUACION = ['2023', '2024']

# Columnas base del archivo de programas
COLUMNAS_BASE_PROGRAMAS = [
    'CODIGO_SNIES_PROGRAMA',
    'INSTITUCION_EDUCACION_SUPERIOR',
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
    try:
        str_series = series.astype(str)
    except Exception as e:
        str_series = series.apply(lambda x: str(x) if pd.notna(x) else '')
    return str_series.str.strip().str.replace(r'\.0$', '', regex=True)

def setup_logging(log_path):
    """Configura el archivo de log."""
    timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    log_file_name = os.path.join(log_path, f'log_clasificacion_activos_inactivos_{timestamp}.txt')
    with open(log_file_name, 'w', encoding='utf-8') as f:
        f.write(f"--- INICIO DEL LOG CLASIFICACIÓN: {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')} ---\n\n")
    return log_file_name

def log_message(log_file, message_type, message):
    """Escribe un mensaje en el archivo de log."""
    print(f"[{message_type}] {message}")
    with open(log_file, 'a', encoding='utf-8') as f:
        f.write(f"[{datetime.datetime.now().strftime('%H:%M:%S')}] [{message_type}] {message}\n")

def load_and_prepare_programas(file_path, log_file):
    """Carga y prepara el DataFrame de programas."""
    try:
        log_message(log_file, "INFO", f"Cargando archivo de programas: {file_path}")
        df_programas = pd.read_excel(file_path, sheet_name='Programas', engine='openpyxl')
        df_programas.columns = [normalize_text(col) for col in df_programas.columns]
        
        mapeo = {
            'CODIGO_SNIES_DEL_PROGRAMA': 'CODIGO_SNIES_PROGRAMA', 
            'NOMBRE_INSTITUCION': 'INSTITUCION_EDUCACION_SUPERIOR',
            'NOMBRE_INSTITUCIÓN': 'INSTITUCION_EDUCACION_SUPERIOR'
        }
        for old, new in mapeo.items():
            if old in df_programas.columns and new not in df_programas.columns: 
                df_programas.rename(columns={old: new}, inplace=True)
        
        if 'CODIGO_SNIES_PROGRAMA' not in df_programas.columns or 'REGION' not in df_programas.columns:
            log_message(log_file, "ERROR", "Columnas 'CODIGO_SNIES_PROGRAMA' y 'REGION' no encontradas en programas.")
            return None
            
        df_programas['CODIGO_SNIES_PROGRAMA'] = clean_snies_code(df_programas['CODIGO_SNIES_PROGRAMA'])
        
        cols_to_keep = COLUMNAS_BASE_PROGRAMAS + ['REGION']
        if not all(col in df_programas.columns for col in cols_to_keep):
             log_message(log_file, "ERROR", f"Faltan columnas base requeridas en programas: {[c for c in cols_to_keep if c not in df_programas.columns]}")
             return None
             
        log_message(log_file, "INFO", "Archivo de programas cargado.")
        return df_programas[cols_to_keep]
    except Exception as e:
        log_message(log_file, "ERROR", f"Cargando archivo de programas '{file_path}': {e}")
        return None

def get_periodo_columns(df):
    """Identifica columnas que parecen periodos YYYY_S."""
    cols = [col for col in df.columns if re.match(r'^\d{4}_\d{1,2}$', col)]
    sort_key = lambda x: (int(x.split('_')[0]), int(x.split('_')[1]))
    return sorted(cols, key=sort_key)

def process_indicator_data(indicator_csv_path, log_file):
    """Lee un archivo CSV de indicador y devuelve datos agrupados."""
    try:
        log_message(log_file, "DEBUG", f"Leyendo indicador: {indicator_csv_path}")
        try:
            df_indicator = pd.read_csv(indicator_csv_path, dtype={'CODIGO_SNIES_PROGRAMA': str}, low_memory=False)
        except ValueError:
            try:
                df_indicator = pd.read_csv(indicator_csv_path, dtype={'CODIGO_SNIES_DEL_PROGRAMA': str}, low_memory=False)
            except ValueError:
                df_indicator = pd.read_csv(indicator_csv_path, low_memory=False)
        df_indicator.columns = [normalize_text(col) for col in df_indicator.columns]

        if 'CODIGO_SNIES_PROGRAMA' not in df_indicator.columns:
            if 'CODIGO_SNIES_DEL_PROGRAMA' in df_indicator.columns:
                df_indicator.rename(columns={'CODIGO_SNIES_DEL_PROGRAMA': 'CODIGO_SNIES_PROGRAMA'}, inplace=True)

        if 'CODIGO_SNIES_PROGRAMA' not in df_indicator.columns or 'ANO' not in df_indicator.columns:
            log_message(log_file, "WARNING", f"Archivo {indicator_csv_path} omite 'CODIGO_SNIES_PROGRAMA' o 'ANO'.")
            return pd.DataFrame()

        df_indicator['CODIGO_SNIES_PROGRAMA'] = clean_snies_code(df_indicator['CODIGO_SNIES_PROGRAMA'])

        period_columns = get_periodo_columns(df_indicator)
        data_list = []

        if period_columns:
            log_message(log_file, "DEBUG", f"Usando columnas de periodo preexistentes: {period_columns}")
            value_cols = period_columns
            id_cols = ['CODIGO_SNIES_PROGRAMA']
            id_cols = [col for col in id_cols if col in df_indicator.columns]
            if not id_cols:
                 log_message(log_file, "ERROR", f"No se encontró la columna 'CODIGO_SNIES_PROGRAMA' en {indicator_csv_path} para despivotar.")
                 return pd.DataFrame()
                 
            df_unpivoted = pd.melt(df_indicator, id_vars=id_cols, value_vars=value_cols, var_name='PERIODO', value_name='VALOR')
            data_list = df_unpivoted[['CODIGO_SNIES_PROGRAMA', 'PERIODO', 'VALOR']].to_dict('records')
        else:
            col_semestre = next((col for col in df_indicator.columns if 'SEMESTRE' in col.upper() or 'PERIODO' in col.upper() and col.upper() != 'PERIODO_ACADEMICO'), None)
            col_valor_indicador = None
            indicador_filename = os.path.basename(indicator_csv_path).split('.')[0]
            if indicador_filename in df_indicator.columns:
                col_valor_indicador = indicador_filename
            else:
                potential_value_columns = []
                non_data_cols = ['CODIGO_SNIES_PROGRAMA', 'ANO', 'INSTITUCION_EDUCACION_SUPERIOR', 'NOMBRE_DEL_PROGRAMA', 'REGION', 'MUNICIPIO_OFERTA_PROGRAMA', col_semestre]
                non_data_cols.extend(df_indicator.columns[:12])
                
                for col in reversed(df_indicator.columns):
                    if col not in non_data_cols:
                        try:
                            pd.to_numeric(df_indicator[col], errors='raise')
                            potential_value_columns.append(col)
                            col_valor_indicador = col
                            break 
                        except (ValueError, TypeError):
                            continue

            if col_semestre and col_valor_indicador and 'ANO' in df_indicator.columns:
                log_message(log_file, "DEBUG", f"Construyendo periodos desde ANO, {col_semestre}, valor en {col_valor_indicador}")
                for _, row in df_indicator.iterrows():
                    try:
                        if pd.isna(row['ANO']):
                            log_message(log_file, "WARNING", f"Valor NaN encontrado en columna ANO. Omitiendo fila: {row.to_dict()}")
                            continue

                        ano_int = int(row['ANO'])

                        p_val = normalize_text(str(row[col_semestre]))
                        try:
                            semestre_num = int(float(str(row[col_semestre])))
                            semestre_part = str(semestre_num)
                        except (ValueError, TypeError):
                            p_num = re.search(r'\d+', p_val)
                            semestre_part = p_num.group(0) if p_num else p_val[:1]
                        if semestre_part not in ['1', '2']:
                             log_message(log_file, "DEBUG", f"Valor de semestre no estándar '{row[col_semestre]}' (normalizado: '{p_val}'). Usando: '{semestre_part}'")
                        
                        periodo = f"{ano_int}_{semestre_part}"
                        
                        data_list.append({'CODIGO_SNIES_PROGRAMA': row['CODIGO_SNIES_PROGRAMA'], 'PERIODO': periodo, 'VALOR': row[col_valor_indicador]})
                    except Exception as e_p:
                        log_message(log_file, "WARNING", f"Error procesando fila periodo: {e_p}. Fila: {row.to_dict()}")
                        continue
            else:
                 log_message(log_file, "WARNING", f"No se pudo determinar estructura periodo/valor en {indicator_csv_path}. Sem: {col_semestre}, Val: {col_valor_indicador}")
                 return pd.DataFrame()

        if not data_list: return pd.DataFrame()
        df_processed = pd.DataFrame(data_list)
        df_processed['VALOR'] = pd.to_numeric(df_processed['VALOR'], errors='coerce').fillna(0)
        return df_processed.groupby(['CODIGO_SNIES_PROGRAMA', 'PERIODO'])['VALOR'].sum().reset_index()
    except Exception as e:
        log_message(log_file, "ERROR", f"Procesando {indicator_csv_path}: {e}")
        return pd.DataFrame()

def clasificar_programas_activos_inactivos(df_primer_curso, log_file):
    """
    Clasifica programas como ACTIVOS o INACTIVOS basado en datos de PRIMER_CURSO.
    Un programa es INACTIVO si tiene 0 estudiantes en PRIMER_CURSO durante los últimos 2 años consecutivos.
    """
    log_message(log_file, "INFO", "--- Iniciando clasificación de programas ACTIVOS/INACTIVOS ---")
    
    # Crear tabla pivote con PRIMER_CURSO por programa y periodo
    if df_primer_curso.empty:
        log_message(log_file, "ERROR", "No hay datos de PRIMER_CURSO para clasificar programas.")
        return pd.DataFrame(), pd.DataFrame()
    
    try:
        df_pivot = df_primer_curso.pivot(index='CODIGO_SNIES_PROGRAMA', columns='PERIODO', values='VALOR').reset_index()
        df_pivot.columns.name = None
        df_pivot = df_pivot.fillna(0)
        
        # Identificar columnas de los años de evaluación
        periodo_cols = get_periodo_columns(df_pivot)
        periodos_evaluacion = [col for col in periodo_cols if any(ano in col for ano in ANOS_EVALUACION)]
        
        log_message(log_file, "INFO", f"Periodos de evaluación encontrados: {periodos_evaluacion}")
        
        if not periodos_evaluacion:
            log_message(log_file, "ERROR", f"No se encontraron datos para los años de evaluación {ANOS_EVALUACION}")
            return pd.DataFrame(), pd.DataFrame()
        
        # Clasificar programas
        activos = []
        inactivos = []
        
        for _, row in df_pivot.iterrows():
            codigo_snies = row['CODIGO_SNIES_PROGRAMA']
            
            # Verificar si tiene estudiantes en PRIMER_CURSO en algún periodo de evaluación
            tiene_estudiantes = False
            for periodo in periodos_evaluacion:
                if periodo in row and row[periodo] > 0:
                    tiene_estudiantes = True
                    break
            
            if tiene_estudiantes:
                activos.append(codigo_snies)
            else:
                inactivos.append(codigo_snies)
        
        df_activos = df_pivot[df_pivot['CODIGO_SNIES_PROGRAMA'].isin(activos)]
        df_inactivos = df_pivot[df_pivot['CODIGO_SNIES_PROGRAMA'].isin(inactivos)]
        
        log_message(log_file, "INFO", f"Programas ACTIVOS encontrados: {len(activos)}")
        log_message(log_file, "INFO", f"Programas INACTIVOS encontrados: {len(inactivos)}")
        
        return activos, inactivos
        
    except Exception as e:
        log_message(log_file, "ERROR", f"Error en clasificación de programas: {e}")
        return pd.DataFrame(), pd.DataFrame()

def generar_excel_clasificado(df_programas_base, lista_codigos, clasificacion, indicadores_data, output_filename, log_file):
    """Genera archivo Excel para programas ACTIVOS o INACTIVOS."""
    try:
        import xlsxwriter
    except ImportError:
        log_message(log_file, "CRITICAL", "La librería 'xlsxwriter' es necesaria. Instálala con: pip install xlsxwriter")
        return

    excel_output_path = os.path.join(BASE_OUTPUT_PATH, output_filename)
    
    try:
        writer = pd.ExcelWriter(excel_output_path, engine='xlsxwriter')
    except Exception as e_writer:
         log_message(log_file, "CRITICAL", f"No se pudo crear el ExcelWriter: {e_writer}")
         return

    log_message(log_file, "INFO", f"Creando archivo Excel {clasificacion}: {excel_output_path}")

    # Filtrar programas por la lista de códigos
    df_programas_filtrados = df_programas_base[df_programas_base['CODIGO_SNIES_PROGRAMA'].isin(lista_codigos)]
    
    if df_programas_filtrados.empty:
        log_message(log_file, "WARNING", f"No se encontraron programas {clasificacion} en el archivo base.")
        writer.close()
        return

    regiones = sorted(df_programas_filtrados['REGION'].dropna().unique().tolist())

    # Procesar cada indicador para generar una hoja
    for indicador in INDICADORES:
        log_message(log_file, "INFO", f"--- Procesando Indicador {clasificacion}: {indicador} ---")
        
        if indicador not in indicadores_data or indicadores_data[indicador].empty:
            log_message(log_file, "WARNING", f"No hay datos para '{indicador}' {clasificacion}. Omitiendo hoja.")
            continue
            
        df_indicador_data = indicadores_data[indicador]
        
        # Filtrar datos del indicador solo para programas de la clasificación
        df_indicador_filtrado = df_indicador_data[df_indicador_data['CODIGO_SNIES_PROGRAMA'].isin(lista_codigos)]
        
        if df_indicador_filtrado.empty:
            log_message(log_file, "WARNING", f"No hay datos de '{indicador}' para programas {clasificacion}.")
            continue

        try:
            df_pivot_total = df_indicador_filtrado.pivot(index='CODIGO_SNIES_PROGRAMA', columns='PERIODO', values='VALOR').reset_index()
            df_pivot_total.columns.name = None
        except Exception as e_pivot:
             log_message(log_file, "ERROR", f"Error al pivotar {indicador} {clasificacion}: {e_pivot}. Omitiendo hoja.")
             continue

        df_pivot_total['CODIGO_SNIES_PROGRAMA'] = clean_snies_code(df_pivot_total['CODIGO_SNIES_PROGRAMA'])

        df_consolidado = pd.merge(df_programas_filtrados, df_pivot_total, on='CODIGO_SNIES_PROGRAMA', how='left')
        
        period_cols = get_periodo_columns(df_consolidado)
        cols_final_order = COLUMNAS_BASE_PROGRAMAS + period_cols
        cols_final_order = [col for col in cols_final_order if col in df_consolidado.columns]
        
        df_consolidado[period_cols] = df_consolidado[period_cols].fillna(0)
        for col_p in period_cols:
             try: df_consolidado[col_p] = df_consolidado[col_p].astype(int)
             except: df_consolidado[col_p] = df_consolidado[col_p].astype(float)

        # Escribir en la hoja de Excel con formato
        sheet_name = indicador[:31] 
        log_message(log_file, "INFO", f"Escribiendo hoja {clasificacion}: '{sheet_name}'")
        
        workbook = writer.book
        worksheet = workbook.add_worksheet(sheet_name)
        writer.sheets[sheet_name] = worksheet 

        # Definir formatos
        header_format = workbook.add_format({'bold': True, 'bg_color': '#D9E1F2', 'border': 1, 'align': 'center', 'valign': 'vcenter', 'text_wrap': True})
        region_header_format = workbook.add_format({'bold': True, 'font_size': 11, 'bg_color': '#F2F2F2'})
        total_row_format = workbook.add_format({'bold': True, 'bg_color': '#E7E6E6', 'border': 1, 'num_format': '#,##0'})
        data_format = workbook.add_format({'border': 1})
        number_format = workbook.add_format({'border': 1, 'num_format': '#,##0'})

        # Escribir encabezado principal
        for col_num, value in enumerate(cols_final_order):
            worksheet.write(0, col_num, value, header_format)
        
        current_row_excel = 1 

        # Iterar por regiones para escribir bloques
        for region in regiones:
            df_region_data = df_consolidado[df_consolidado['REGION'] == region][cols_final_order] 
            
            if df_region_data.empty: continue

            # Escribir encabezado de región
            region_text = f'REGIÓN: {region} ({clasificacion})'
            worksheet.write(current_row_excel, 0, region_text, region_header_format)
            for i in range(1, len(cols_final_order)):
                 worksheet.write(current_row_excel, i, None, region_header_format)
            current_row_excel += 1
            
            # Escribir filas de datos
            for _, data_row_values in df_region_data.iterrows():
                for col_num, value in enumerate(data_row_values):
                     is_number_col = col_num >= len(COLUMNAS_BASE_PROGRAMAS)
                     cell_format = number_format if is_number_col else data_format
                     try:
                         worksheet.write(current_row_excel, col_num, value, cell_format)
                     except TypeError:
                         worksheet.write_string(current_row_excel, col_num, str(value), data_format)
                current_row_excel += 1

            # Calcular y escribir fila de totales
            if not df_region_data.empty and period_cols:
                sums = df_region_data[period_cols].sum()
                worksheet.write(current_row_excel, 1, f'TOTAL REGIÓN {region}', total_row_format) 
                for col_num, col_name in enumerate(cols_final_order):
                    if col_name in sums:
                        worksheet.write(current_row_excel, col_num, sums[col_name], total_row_format)
                    elif col_num != 1:
                        worksheet.write(current_row_excel, col_num, None, total_row_format)

                current_row_excel += 2
            else:
                 current_row_excel += 1 

        # Ajustar ancho de columnas
        for i, col in enumerate(cols_final_order):
             try:
                 max_len_data = df_consolidado[col].astype(str).map(len).max() if not df_consolidado[col].empty else 0
             except Exception:
                 max_len_data = 10

             max_len_header = len(col)
             width = min(max(max_len_data, max_len_header) + 2, 50)
             worksheet.set_column(i, i, width)

        del df_consolidado
        gc.collect()

    # Guardar el archivo Excel
    try:
        writer.close() 
        log_message(log_file, "SUCCESS", f"Archivo Excel {clasificacion} '{excel_output_path}' guardado exitosamente.")
    except Exception as e_save:
        log_message(log_file, "ERROR", f"No se pudo guardar el archivo Excel {clasificacion} '{excel_output_path}': {e_save}")

# --- Script Principal ---
def main():
    os.makedirs(BASE_OUTPUT_PATH, exist_ok=True)
    log_file = setup_logging(BASE_OUTPUT_PATH)
    log_message(log_file, "INFO", "--- Iniciando Script de Clasificación ACTIVOS/INACTIVOS (v1) ---")

    df_programas_base = load_and_prepare_programas(PROGRAMAS_BENCHMARKING_FILE, log_file)
    if df_programas_base is None: return

    if not os.path.exists(CSV_CONVERTIDOS_FOLDER):
        log_message(log_file, "CRITICAL", f"La carpeta '{CSV_CONVERTIDOS_FOLDER}' no existe.")
        return
        
    available_years = sorted([d for d in os.listdir(CSV_CONVERTIDOS_FOLDER) if os.path.isdir(os.path.join(CSV_CONVERTIDOS_FOLDER, d)) and d.isdigit()])
    if not available_years:
        log_message(log_file, "CRITICAL", f"No se encontraron carpetas de años en '{CSV_CONVERTIDOS_FOLDER}'.")
        return
    log_message(log_file, "INFO", f"Años con datos CSV detectados: {available_years}")

    # Cargar datos de todos los indicadores
    indicadores_data = {}
    for indicador in INDICADORES:
        all_years_data = []
        for year in available_years:
            indicator_csv_file = os.path.join(CSV_CONVERTIDOS_FOLDER, year, f"{indicador}.csv")
            if os.path.exists(indicator_csv_file):
                 df_year_data = process_indicator_data(indicator_csv_file, log_file)
                 if not df_year_data.empty: all_years_data.append(df_year_data)
            else: log_message(log_file, "DEBUG", f"Archivo no encontrado: {indicator_csv_file}")

        if all_years_data:
            df_indicador_total = pd.concat(all_years_data, ignore_index=True)
            indicadores_data[indicador] = df_indicador_total.groupby(['CODIGO_SNIES_PROGRAMA', 'PERIODO'])['VALOR'].sum().reset_index()
        else:
            indicadores_data[indicador] = pd.DataFrame()

    # Clasificar programas basado en PRIMER_CURSO
    if 'PRIMER_CURSO' not in indicadores_data or indicadores_data['PRIMER_CURSO'].empty:
        log_message(log_file, "CRITICAL", "No se encontraron datos de PRIMER_CURSO. No se puede realizar la clasificación.")
        return

    codigos_activos, codigos_inactivos = clasificar_programas_activos_inactivos(indicadores_data['PRIMER_CURSO'], log_file)

    if not codigos_activos and not codigos_inactivos:
        log_message(log_file, "CRITICAL", "No se pudieron clasificar programas. Abortando.")
        return

    # Generar archivos Excel para ACTIVOS e INACTIVOS
    if codigos_activos:
        generar_excel_clasificado(df_programas_base, codigos_activos, "ACTIVOS", indicadores_data, OUTPUT_EXCEL_ACTIVOS, log_file)

    if codigos_inactivos:
        generar_excel_clasificado(df_programas_base, codigos_inactivos, "INACTIVOS", indicadores_data, OUTPUT_EXCEL_INACTIVOS, log_file)

    log_message(log_file, "INFO", "--- Script de Clasificación ACTIVOS/INACTIVOS Finalizado ---")

if __name__ == '__main__':
    main()