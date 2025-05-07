import pandas as pd
import os
import re
from rapidfuzz import process, fuzz
import unicodedata # Para una mejor normalización de texto (eliminar acentos)

# --- Configuración ---
# Ruta base donde se encuentran las carpetas de años con archivos XLSX
# Asegúrate de que esta ruta sea correcta para tu estructura de archivos.
# Los archivos originales deben estar en subcarpetas como ./files/2018/, ./files/2019/, etc.
BASE_PATH = './files/'

# Nombre de la carpeta donde se guardarán los CSV convertidos
OUTPUT_CSV_FOLDER_NAME = 'csv_convertidos'

# Mapeo de nombres de archivo CSV estándar a posibles variantes de columnas clave
# Esto ayuda a nombrar el archivo CSV de salida y a identificar la columna principal de datos.
# Las claves son los nombres estándar que queremos para los archivos CSV.
# Los valores son listas de posibles nombres de columnas (o partes de ellos) que podrían indicar ese tipo de datos.
# Se convertirán a mayúsculas y se reemplazarán espacios por guiones bajos antes de la comparación.
KEY_COLUMN_VARIANTS_FOR_FILENAME = {
    'INSCRITOS': ['INSCRITOS', 'INSCRIPCIONES', 'INSCRITO'],
    'ADMITIDOS': ['ADMITIDOS', 'ADMISIONES', 'ADMITIDO'],
    'MATRICULADOS': ['MATRICULADOS', 'MATRICULA', 'MATRICULADO'],
    'PRIMER_CURSO': ['PRIMER_CURSO', 'MATRICULADOS_PRIMER_CURSO', 'NUEVOS_ESTUDIANTES', 'PRIMIPAROS'],
    'GRADUADOS': ['GRADUADOS', 'EGRESADOS', 'GRADUADO']
}

# Columnas obligatorias que deben existir (o ser mapeadas) en los archivos CSV finales.
# Se intentará normalizar las columnas existentes para que coincidan con estos nombres.
MANDATORY_COLUMNS_STD = {
    'CODIGO_SNIES_PROGRAMA': ['CODIGO_SNIES_DEL_PROGRAMA', 'CODIGO_SNIES', 'SNIES_PROGRAMA', 'COD_SNIES_PROGR'],
    'ANO': ['AÑO', 'ANO', 'ANIO', 'YEAR'], # Se añadirá si no existe, basado en la carpeta
    # Puedes añadir más columnas obligatorias y sus variantes aquí si es necesario
    # 'NOMBRE_INSTITUCION': ['INSTITUCION_EDUCACION_SUPERIOR', 'NOMBRE_INSTITUCION', 'UNIVERSIDAD'],
    # 'NOMBRE_PROGRAMA': ['NOMBRE_DEL_PROGRAMA', 'PROGRAMA_ACADEMICO', 'DENOMINACION_PROGRAMA'],
}

# Umbral de similitud para la coincidencia de nombres de columna con rapidfuzz
SIMILARITY_THRESHOLD = 80 # Porcentaje (0-100)

# --- Funciones Auxiliares ---

def normalize_text(text):
    """Normaliza el texto: a mayúsculas, sin acentos, reemplaza espacios con guiones bajos."""
    if not isinstance(text, str):
        text = str(text)
    # Eliminar acentos
    nfkd_form = unicodedata.normalize('NFKD', text)
    text_without_accents = "".join([c for c in nfkd_form if not unicodedata.combining(c)])
    # A mayúsculas, reemplazar espacios y guiones, eliminar caracteres especiales excepto _
    text_cleaned = re.sub(r'\s+|-', '_', text_without_accents.upper())
    text_cleaned = re.sub(r'[^A-Z0-9_]', '', text_cleaned)
    return text_cleaned.strip('_')

def find_best_column_match(column_name_to_match, possible_target_names, scorer=fuzz.WRatio, threshold=SIMILARITY_THRESHOLD):
    """
    Encuentra la mejor coincidencia para un nombre de columna dado una lista de nombres posibles.
    Usa rapidfuzz para la comparación.
    """
    # Normalizar el nombre de columna a buscar
    normalized_column_to_match = normalize_text(column_name_to_match)

    # Normalizar la lista de nombres posibles para la comparación
    normalized_target_names = [normalize_text(name) for name in possible_target_names]

    best_match = process.extractOne(normalized_column_to_match, normalized_target_names, scorer=scorer, score_cutoff=threshold)

    if best_match:
        # Devolver el nombre original de la lista 'possible_target_names' que corresponde a la mejor coincidencia normalizada
        original_target_index = normalized_target_names.index(best_match[0])
        return possible_target_names[original_target_index]
    return None

def identify_file_type_and_key_column(df_columns):
    """
    Identifica el tipo de archivo (ej. INSCRITOS, MATRICULADOS) basado en sus columnas
    y devuelve el nombre estándar del tipo y la columna clave identificada.
    """
    normalized_df_columns = [normalize_text(col) for col in df_columns]

    for standard_name, variants in KEY_COLUMN_VARIANTS_FOR_FILENAME.items():
        normalized_variants = [normalize_text(var) for var in variants]
        for i, col_norm in enumerate(normalized_df_columns):
            # Intentar una coincidencia parcial o completa
            for var_norm in normalized_variants:
                if var_norm in col_norm: # Coincidencia de subcadena
                    # Devolver el nombre estándar y la columna original que coincidió
                    return standard_name, df_columns[i]
            # Si no hay subcadena, intentar con fuzzy matching
            best_fuzzy_match_for_variant = find_best_column_match(col_norm, normalized_variants, threshold=85) # Umbral más alto para variantes
            if best_fuzzy_match_for_variant:
                 return standard_name, df_columns[i]


    # Caso especial: si la última columna contiene una palabra clave común para datos numéricos
    # Esto es una heurística y puede necesitar ajustes.
    if df_columns:
        last_col_normalized = normalize_text(df_columns[-1])
        # Palabras clave comunes para columnas de datos numéricos de indicadores
        numeric_keywords = ['TOTAL', 'VALOR', 'CANTIDAD', 'NUMERO']
        for kw in numeric_keywords:
            if kw in last_col_normalized:
                 # Intentar deducir el tipo por el nombre del archivo si es posible,
                 # o devolver un genérico y la última columna.
                 # Esta parte es más compleja sin el nombre del archivo.
                 # Por ahora, si la última columna parece numérica, la marcamos como potencial columna de datos.
                 print(f"    ⚠️ No se pudo determinar un tipo de archivo estándar. Usando la última columna '{df_columns[-1]}' como posible columna de datos.")
                 return "DATOS_GENERALES", df_columns[-1] # Nombre genérico

    return None, None


def main():
    """Función principal para procesar los archivos."""
    print("🚀 Iniciando script de conversión de XLSX a CSV y normalización...")

    # Crear la carpeta de salida principal para CSVs si no existe
    output_base_path = os.path.join(BASE_PATH, OUTPUT_CSV_FOLDER_NAME)
    os.makedirs(output_base_path, exist_ok=True)
    print(f"📂 Carpeta de salida para CSVs: {output_base_path}")

    # Obtener la lista de carpetas de años (asumiendo que son numéricas)
    try:
        year_folders = sorted([d for d in os.listdir(BASE_PATH) if os.path.isdir(os.path.join(BASE_PATH, d)) and d.isdigit()])
    except FileNotFoundError:
        print(f"❌ ERROR: La ruta base '{BASE_PATH}' no fue encontrada. Verifica la configuración.")
        return

    if not year_folders:
        print(f"⚠️ No se encontraron carpetas de años en '{BASE_PATH}'. Asegúrate de que la estructura es correcta (ej: {BASE_PATH}2018/, {BASE_PATH}2019/).")
        return

    print(f"🗓️ Años detectados para procesar: {year_folders}")

    for year_str in year_folders:
        year_input_path = os.path.join(BASE_PATH, year_str)
        year_output_path = os.path.join(output_base_path, year_str)
        os.makedirs(year_output_path, exist_ok=True)

        print(f"\n🔄 Procesando año: {year_str}")
        print(f"  📂 Carpeta de entrada: {year_input_path}")
        print(f"  📂 Carpeta de salida para CSVs de {year_str}: {year_output_path}")

        excel_files = [f for f in os.listdir(year_input_path) if f.endswith('.xlsx') and not f.startswith('~')]

        if not excel_files:
            print(f"  ⚠️ No se encontraron archivos .xlsx en '{year_input_path}' para el año {year_str}.")
            continue

        for excel_file in excel_files:
            file_path = os.path.join(year_input_path, excel_file)
            print(f"\n  📄 Procesando archivo: {excel_file}")

            try:
                # Leer el archivo Excel
                # Intentar leer todas las hojas si hay varias, o la primera por defecto.
                # Por simplicidad, aquí leemos solo la primera hoja.
                # Si tus archivos Excel tienen datos importantes en múltiples hojas,
                # este bucle necesitaría ser adaptado para iterar sobre las hojas.
                xls = pd.ExcelFile(file_path)
                sheet_name = xls.sheet_names[0] # Tomar la primera hoja
                print(f"    📑 Leyendo hoja: '{sheet_name}'")
                df = pd.read_excel(xls, sheet_name=sheet_name)

                if df.empty:
                    print(f"    ⚠️ El archivo (o la primera hoja) '{excel_file}' está vacío. Omitiendo.")
                    continue

                original_columns = df.columns.tolist()
                current_columns = original_columns[:] # Copia para modificar

                # --- 1. Normalización básica de nombres de columnas ---
                renamed_cols_map_basic = {col: normalize_text(col) for col in current_columns}
                df.rename(columns=renamed_cols_map_basic, inplace=True)
                current_columns = df.columns.tolist()
                print(f"    📊 Columnas normalizadas (básico): {current_columns}")

                # --- 2. Mapeo a columnas obligatorias estándar ---
                renamed_cols_map_mandatory = {}
                temp_current_columns = df.columns.tolist() # Usar una copia para iterar mientras se modifica

                for actual_col_norm in temp_current_columns: # Iterar sobre las columnas ya normalizadas básicas
                    for std_col_name, variants in MANDATORY_COLUMNS_STD.items():
                        # `std_col_name` ya está normalizado por definición
                        best_match_for_std = find_best_column_match(actual_col_norm, variants + [std_col_name])
                        if best_match_for_std: # Si hay una buena coincidencia con alguna variante o el nombre estándar
                            if actual_col_norm != std_col_name: # Solo renombrar si es diferente y no ya mapeado
                                if actual_col_norm not in renamed_cols_map_mandatory or renamed_cols_map_mandatory[actual_col_norm] != std_col_name :
                                     # Evitar renombrar una columna que ya fue mapeada a otro estándar o a sí misma si es un mejor match
                                    if std_col_name not in df.columns or actual_col_norm == std_col_name: # Si el destino no existe o es un self-match
                                        renamed_cols_map_mandatory[actual_col_norm] = std_col_name
                                        print(f"      🔄 Mapeando columna '{actual_col_norm}' a estándar '{std_col_name}' (basado en variante '{best_match_for_std}')")
                                    elif df[actual_col_norm].equals(df[std_col_name]): # Si las columnas son idénticas y el std_name ya existe
                                        print(f"      ℹ️ Columna '{actual_col_norm}' es idéntica a la ya existente '{std_col_name}'. Se podría eliminar '{actual_col_norm}'.")
                                    # else:
                                        # print(f"      ⚠️ Conflicto: '{actual_col_norm}' coincide con '{std_col_name}', pero '{std_col_name}' ya existe y no es idéntica.")


                df.rename(columns=renamed_cols_map_mandatory, inplace=True)
                current_columns = df.columns.tolist()
                print(f"    📊 Columnas después del mapeo a estándar: {current_columns}")


                # --- 3. Añadir columna 'ANO' si no existe ---
                # La columna 'ANO' es una de las MANDATORY_COLUMNS_STD
                year_col_std_name = 'ANO' # Usar el nombre estándar definido en MANDATORY_COLUMNS_STD
                if year_col_std_name not in df.columns:
                    df[year_col_std_name] = int(year_str)
                    print(f"    ➕ Añadida columna '{year_col_std_name}' con valor: {year_str}")
                else:
                    # Verificar si la columna AÑO existente tiene valores consistentes con year_str
                    # Esto es opcional pero bueno para la validación de datos
                    try:
                        df[year_col_std_name] = pd.to_numeric(df[year_col_std_name], errors='coerce')
                        # Si hay NaNs después de la conversión, podrían ser valores no numéricos.
                        # Si todos los valores son iguales al año de la carpeta, está bien.
                        # Si son diferentes, podría ser un problema de datos.
                        if not df[df[year_col_std_name] != int(year_str)][year_col_std_name].empty:
                             print(f"    ⚠️ Advertencia: La columna '{year_col_std_name}' existe pero contiene valores diferentes a '{year_str}'. Se mantendrán los valores existentes.")
                    except Exception as e_yr:
                        print(f"    ⚠️ No se pudo verificar la columna '{year_col_std_name}': {e_yr}. Se mantendrá como está.")


                # --- 4. Identificar tipo de archivo para nombre de salida ---
                file_type_std, identified_key_col = identify_file_type_and_key_column(df.columns.tolist())

                if file_type_std:
                    output_filename_base = file_type_std
                    print(f"    🏷️ Tipo de archivo identificado como: '{file_type_std}' (columna clave: '{identified_key_col}')")
                else:
                    # Si no se puede identificar un tipo, usar el nombre original del archivo (sin extensión)
                    # y añadir un prefijo o sufijo para indicar que es procesado.
                    output_filename_base = os.path.splitext(excel_file)[0] + "_procesado"
                    print(f"    ⚠️ No se pudo identificar un tipo de archivo estándar. Nombre de salida base: '{output_filename_base}'")


                # --- 5. Verificación final de columnas obligatorias ---
                missing_mandatory = []
                for std_name_m, _ in MANDATORY_COLUMNS_STD.items():
                    if std_name_m not in df.columns:
                        missing_mandatory.append(std_name_m)
                
                if 'CODIGO_SNIES_PROGRAMA' not in df.columns: # Comprobación específica para la columna más crítica
                    if not any('SNIES' in col for col in df.columns): # Si ni siquiera hay una columna con SNIES
                         print(f"    ❌ ERROR CRÍTICO: No se encontró ni se pudo mapear la columna 'CODIGO_SNIES_PROGRAMA' o similar en '{excel_file}'. Omitiendo guardado.")
                         continue # Saltar al siguiente archivo


                if missing_mandatory:
                    print(f"    ⚠️ Advertencia: Faltan las siguientes columnas obligatorias estandarizadas después del procesamiento: {missing_mandatory}. El archivo se guardará igualmente.")


                # --- 6. Guardar como CSV ---
                output_csv_filename = f"{output_filename_base}.csv"
                output_csv_path = os.path.join(year_output_path, output_csv_filename)

                df.to_csv(output_csv_path, index=False, encoding='utf-8-sig')
                print(f"    ✅ Archivo CSV guardado como: {output_csv_path}")

            except Exception as e:
                print(f"    ❌ ERROR procesando el archivo '{excel_file}': {e}")
                import traceback
                traceback.print_exc() # Imprime el traceback completo para más detalles del error

    print("\n🎉 Proceso de conversión y normalización completado.")

if __name__ == '__main__':
    main()
