# limpiar_csv.py
# Este script contiene una función genérica para limpiar y preprocesar un DataFrame.
# Ahora incluye la lógica para inferir automáticamente las reglas de limpieza
# (manejo de nulos y conversión de tipos) basándose en el contenido del DataFrame
# y los campos requeridos para el modelo RDF.

import pandas as pd # Importa la biblioteca Pandas para la manipulación de DataFrames.
import io # Importa el módulo io (no se usa directamente en esta versión, pero es una importación común).

# Define la función genérica para limpiar el DataFrame.
# Ahora acepta el DataFrame y la lista de columnas requeridas para el modelo RDF.
def limpiar_dataframe_generico(df):
    """
    Toma un DataFrame de Pandas y aplica reglas de limpieza inferidas automáticamente.
    La inferencia se basa en los tipos de datos de las columnas y la presencia de nulos.

    Args:
        df (pd.DataFrame): DataFrame a limpiar. Se asume que las columnas ya han sido renombradas
                           a los nombres estandarizados si es necesario para el mapeo RDF posterior.

    Returns:
        pd.DataFrame: El DataFrame con los datos limpios.
    """
    df_limpio = df.copy() # Trabaja en una copia para no modificar el DataFrame original.

    # --- Lógica de Inferencia Automática para la Limpieza ---
    # Estas listas se construirán dinámicamente
    columns_to_drop_na = []
    columns_to_fill_na_str = []
    columns_to_fill_na_zero_int = []
    columns_to_convert_to_int = []
    columns_to_convert_to_str = []
    columns_to_convert_to_float = [] # Añadido para manejar floats explícitamente
    columns_to_convert_to_datetime = [] # Añadido para manejar fechas

    # Primera pasada: Identificar columnas críticas para dropna (ej. aquellas que parecen IDs únicos)
    # Esto es una inferencia heurística y podría necesitar ajustes.
    # Por ahora, si una columna tiene muchos valores únicos y parece un ID, la consideramos crítica.
    for col in df_limpio.columns:
        if df_limpio[col].isnull().any(): # Solo si tiene nulos
            # Heurística simple: si tiene un alto porcentaje de valores únicos y es de tipo objeto/string
            if (df_limpio[col].nunique() / len(df_limpio) > 0.8) and (pd.api.types.is_object_dtype(df_limpio[col]) or pd.api.types.is_string_dtype(df_limpio[col])):
                columns_to_drop_na.append(col)
            # También si es una columna numérica que debería ser completa (ej. años, conteos)
            elif pd.api.types.is_numeric_dtype(df_limpio[col]) and col.lower() in ['year', 'id', 'doi', 'citation_count']:
                columns_to_drop_na.append(col)

    # Aplicar la eliminación de filas con nulos en columnas críticas inferidas
    existing_critical_columns = [col for col in columns_to_drop_na if col in df_limpio.columns]
    if existing_critical_columns:
        df_limpio.dropna(subset=existing_critical_columns, inplace=True)
        df_limpio.reset_index(drop=True, inplace=True)

    # Segunda pasada: Inferir reglas para rellenar nulos y convertir tipos para las columnas restantes
    for col in df_limpio.columns:
        # Si la columna ya fue procesada por dropna, no la volvemos a procesar aquí para fillna/convert
        if col in columns_to_drop_na:
            continue

        # Inferir si es numérica y debería rellenarse con 0
        numeric_fill_zero_candidates = ['volume', 'issue', 'page_start', 'page_end', 'article_number', 'citation_count', 'year']
        if col.lower() in numeric_fill_zero_candidates:
            if pd.api.types.is_numeric_dtype(df_limpio[col]) or df_limpio[col].isnull().any():
                columns_to_fill_na_zero_int.append(col)
        
        # Inferir si es de tipo fecha
        elif pd.api.types.is_object_dtype(df_limpio[col]) or pd.api.types.is_string_dtype(df_limpio[col]):
            # Intentar convertir a datetime para ver si es una columna de fecha
            try:
                # Usar infer_datetime_format=True para mejor detección de formatos
                temp_series = pd.to_datetime(df_limpio[col].dropna(), errors='coerce', infer_datetime_format=True)
                # Si la mayoría de los valores se convirtieron a fecha, considerarla una columna de fecha
                if temp_series.count() / len(df_limpio[col].dropna()) > 0.7: # Más del 70% son fechas válidas
                    columns_to_convert_to_datetime.append(col)
                else:
                    # Si no es fecha, tratar como string
                    if df_limpio[col].isnull().any():
                        columns_to_fill_na_str.append(col)
                    else:
                        columns_to_convert_to_str.append(col)
            except Exception: # Si falla la conversión a datetime, es una columna de string
                if df_limpio[col].isnull().any():
                    columns_to_fill_na_str.append(col)
                else:
                    columns_to_convert_to_str.append(col)

        elif pd.api.types.is_float_dtype(df_limpio[col]):
            # Si es flotante, y todos los valores no nulos son enteros (ej. 1.0, 2.0), convertir a int
            if df_limpio[col].dropna().apply(lambda x: x.is_integer()).all():
                columns_to_convert_to_int.append(col)
            else:
                # Si no son todos enteros, mantener como float
                columns_to_convert_to_float.append(col)
        
        elif pd.api.types.is_integer_dtype(df_limpio[col]):
            columns_to_convert_to_int.append(col)
        
        elif pd.api.types.is_bool_dtype(df_limpio[col]):
            # Las columnas booleanas se pueden dejar como están o convertir a string si es necesario para RDF
            columns_to_convert_to_str.append(col) # Por simplicidad, tratarlas como string en RDF

    # Eliminar duplicados y asegurar que las reglas no se solapen
    columns_to_drop_na = list(set(columns_to_drop_na))
    columns_to_fill_na_str = list(set(columns_to_fill_na_str))
    columns_to_fill_na_zero_int = list(set(columns_to_fill_na_zero_int))
    columns_to_convert_to_int = list(set(columns_to_convert_to_int))
    columns_to_convert_to_str = list(set(columns_to_convert_to_str))
    columns_to_convert_to_float = list(set(columns_to_convert_to_float))
    columns_to_convert_to_datetime = list(set(columns_to_convert_to_datetime))

    # Asegurar la exclusividad de las reglas de tipo y relleno
    # Una columna solo debe estar en una de estas listas de procesamiento final
    processed_cols = set(columns_to_drop_na)

    def filter_and_add(col_list, target_list):
        for col in col_list:
            if col not in processed_cols:
                target_list.append(col)
                processed_cols.add(col)
        return target_list

    final_fill_na_zero_int = filter_and_add(columns_to_fill_na_zero_int, [])
    final_convert_to_datetime = filter_and_add(columns_to_convert_to_datetime, [])
    final_convert_to_int = filter_and_add(columns_to_convert_to_int, [])
    final_convert_to_float = filter_and_add(columns_to_convert_to_float, [])
    final_fill_na_str = filter_and_add(columns_to_fill_na_str, [])
    final_convert_to_str = filter_and_add(columns_to_convert_to_str, []) # Para columnas que no cayeron en otra categoría

    # --- Aplicar las Reglas de Limpieza Inferidas ---
    for col in final_fill_na_str:
        if col in df_limpio.columns:
            df_limpio[col] = df_limpio[col].fillna('')
    
    for col in final_fill_na_zero_int:
        if col in df_limpio.columns:
            df_limpio[col] = pd.to_numeric(df_limpio[col], errors='coerce').fillna(0).astype(int)

    for col in final_convert_to_int:
        if col in df_limpio.columns:
            df_limpio[col] = pd.to_numeric(df_limpio[col], errors='coerce').fillna(0).astype(int) # También rellena con 0 si hay nulos después de conversión
    
    for col in final_convert_to_float:
        if col in df_limpio.columns:
            df_limpio[col] = pd.to_numeric(df_limpio[col], errors='coerce') # Mantener NaN si no se puede convertir
    
    for col in final_convert_to_datetime:
        if col in df_limpio.columns:
            df_limpio[col] = pd.to_datetime(df_limpio[col], errors='coerce', infer_datetime_format=True)
            # Podrías rellenar NaT (Not a Time) si es necesario, ej. df_limpio[col].fillna(pd.Timestamp('1900-01-01'))
            # Por ahora, los NaT se manejarán como nulos en la conversión RDF.

    for col in final_convert_to_str:
        if col in df_limpio.columns:
            df_limpio[col] = df_limpio[col].astype(str)

    return df_limpio