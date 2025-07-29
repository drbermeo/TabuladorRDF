import pandas as pd # Importa la biblioteca Pandas para la manipulación eficiente de estructuras de datos como DataFrames.
import io # Importa el módulo 'io' que permite trabajar con flujos de datos en memoria (aunque no se usa directamente en la función 'limpiar_dataframe_generico' tal como está aquí, es una importación común para manejo de archivos).

# Define la función principal para limpiar y preprocesar un DataFrame de forma genérica.
# Esta función está diseñada para ser flexible y adaptable a diferentes conjuntos de datos.
def limpiar_dataframe_generico(df):
    """
    Esta función toma un DataFrame de Pandas como entrada y aplica un conjunto de reglas
    de limpieza y preprocesamiento de datos. Estas reglas se infieren automáticamente
    basándose en el tipo de datos de cada columna y la presencia de valores nulos (NaN).

    El objetivo principal es estandarizar los datos, manejar valores faltantes o incorrectos,
    y convertir las columnas a los tipos de datos adecuados para su posterior uso,
    especialmente para la conversión a un grafo de conocimiento RDF donde la consistencia
    de los datos es crucial.

    Args:
        df (pd.DataFrame): El DataFrame de Pandas que necesita ser limpiado.
                           Se asume que, si es necesario para el mapeo RDF, las columnas
                           ya han sido renombradas a los nombres estandarizados antes
                           de pasar el DataFrame a esta función.

    Returns:
        pd.DataFrame: Un nuevo DataFrame de Pandas con los datos limpios y preprocesados.
                      La función trabaja sobre una copia para evitar modificar el DataFrame
                      original pasado como argumento.
    """
    df_limpio = df.copy() # Crea una copia del DataFrame de entrada para realizar las operaciones
                          # de limpieza sin alterar el DataFrame original.

    # --- Lógica de Inferencia Automática para la Limpieza ---
    # Se inicializan listas vacías para categorizar las columnas según el tipo de limpieza
    # que se les aplicará. Estas listas se llenarán dinámicamente durante el proceso de inferencia.
    columns_to_drop_na = []            # Columnas donde las filas con valores nulos se eliminarán.
    columns_to_fill_na_str = []        # Columnas de texto donde los nulos se rellenarán con una cadena vacía.
    columns_to_fill_na_zero_int = []   # Columnas numéricas donde los nulos se rellenarán con 0 y se convertirán a entero.
    columns_to_convert_to_int = []     # Columnas que deben ser convertidas a tipo entero.
    columns_to_convert_to_str = []     # Columnas que deben ser convertidas a tipo cadena (string).
    columns_to_convert_to_float = []   # Columnas que deben ser convertidas a tipo flotante (decimal).
    columns_to_convert_to_datetime = [] # Columnas que deben ser convertidas a tipo fecha y hora.

    # Primera pasada de inferencia: Identificar columnas "críticas" para eliminar filas con nulos.
    # Esta heurística busca columnas que probablemente actúen como identificadores únicos (IDs)
    # y que no deberían tener valores faltantes.
    for col in df_limpio.columns:
        if df_limpio[col].isnull().any(): # Comprueba si la columna contiene algún valor nulo.
            # Heurística 1: Si la columna es de tipo objeto/string y tiene un alto porcentaje de
            # valores únicos (sugiriendo que es un ID), se considera crítica para dropna.
            if (df_limpio[col].nunique() / len(df_limpio) > 0.8) and (pd.api.types.is_object_dtype(df_limpio[col]) or pd.api.types.is_string_dtype(df_limpio[col])):
                columns_to_drop_na.append(col)
            # Heurística 2: Si es una columna numérica y su nombre sugiere que es un identificador
            # o un conteo importante (ej. año, ID, DOI, conteo de citas), también se considera crítica.
            elif pd.api.types.is_numeric_dtype(df_limpio[col]) and col.lower() in ['year', 'id', 'doi', 'citation_count']:
                columns_to_drop_na.append(col)

    # Aplicar la eliminación de filas basándose en las columnas críticas inferidas.
    # Se crea una lista de columnas críticas que realmente existen en el DataFrame.
    existing_critical_columns = [col for col in columns_to_drop_na if col in df_limpio.columns]
    if existing_critical_columns: # Si hay columnas críticas identificadas...
        df_limpio.dropna(subset=existing_critical_columns, inplace=True) # Elimina las filas que tienen nulos en estas columnas.
        df_limpio.reset_index(drop=True, inplace=True) # Restablece el índice del DataFrame después de eliminar filas.

    # Segunda pasada de inferencia: Determinar reglas de relleno de nulos y conversión de tipos
    # para las columnas restantes (aquellas que no fueron procesadas por dropna).
    for col in df_limpio.columns:
        # Se salta la columna si ya fue marcada para eliminación de filas por nulos (prioridad alta).
        if col in columns_to_drop_na:
            continue

        # Inferir si la columna es numérica y si los nulos deben rellenarse con 0.
        # Se usa una lista de nombres de columnas comunes que suelen ser numéricas y rellenadas con 0.
        numeric_fill_zero_candidates = ['volume', 'issue', 'page_start', 'page_end', 'article_number', 'citation_count', 'year']
        if col.lower() in numeric_fill_zero_candidates:
            # Si es numérica o tiene nulos (indicando que podría ser numérica pero con problemas),
            # se marca para rellenar con 0 y convertir a entero.
            if pd.api.types.is_numeric_dtype(df_limpio[col]) or df_limpio[col].isnull().any():
                columns_to_fill_na_zero_int.append(col)
        
        # Inferir si la columna es de tipo fecha (DateTime).
        # Se aplica a columnas de tipo objeto o string que puedan contener fechas.
        elif pd.api.types.is_object_dtype(df_limpio[col]) or pd.api.types.is_string_dtype(df_limpio[col]):
            try:
                # Intenta convertir los valores no nulos de la columna a tipo datetime.
                # 'errors='coerce'' convierte los valores que no pueden ser fechas a NaT (Not a Time).
                # 'infer_datetime_format=True' ayuda a detectar automáticamente el formato de fecha.
                temp_series = pd.to_datetime(df_limpio[col].dropna(), errors='coerce', infer_datetime_format=True)
                # Si una alta proporción (más del 70%) de los valores se convierten a fechas válidas,
                # se considera una columna de fecha.
                if temp_series.count() / len(df_limpio[col].dropna()) > 0.7:
                    columns_to_convert_to_datetime.append(col)
                else:
                    # Si no es predominantemente una columna de fecha, se trata como string.
                    if df_limpio[col].isnull().any(): # Si tiene nulos, se rellenarán con string vacío.
                        columns_to_fill_na_str.append(col)
                    else: # Si no tiene nulos, solo se asegura que sea string.
                        columns_to_convert_to_str.append(col)
            except Exception: # Si falla la conversión a datetime por cualquier razón, se asume que es una columna de string.
                if df_limpio[col].isnull().any():
                    columns_to_fill_na_str.append(col)
                else:
                    columns_to_convert_to_str.append(col)

        # Inferir si la columna es de tipo flotante (decimal).
        elif pd.api.types.is_float_dtype(df_limpio[col]):
            # Si es flotante, pero todos sus valores no nulos son enteros (ej. 1.0, 2.0),
            # se sugiere convertirla a tipo entero.
            if df_limpio[col].dropna().apply(lambda x: x.is_integer()).all():
                columns_to_convert_to_int.append(col)
            else:
                # Si no son todos enteros, se mantiene como flotante.
                columns_to_convert_to_float.append(col)
        
        # Inferir si la columna ya es de tipo entero.
        elif pd.api.types.is_integer_dtype(df_limpio[col]):
            columns_to_convert_to_int.append(col)
        
        # Inferir si la columna es de tipo booleano.
        elif pd.api.types.is_bool_dtype(df_limpio[col]):
            # Para fines de representación RDF, las booleanas a menudo se manejan como strings.
            columns_to_convert_to_str.append(col)

    # Post-procesamiento de las listas de columnas:
    # Eliminar duplicados de las listas (usando set y volviendo a convertir a list).
    columns_to_drop_na = list(set(columns_to_drop_na))
    columns_to_fill_na_str = list(set(columns_to_fill_na_str))
    columns_to_fill_na_zero_int = list(set(columns_to_fill_na_zero_int))
    columns_to_convert_to_int = list(set(columns_to_convert_to_int))
    columns_to_convert_to_str = list(set(columns_to_convert_to_str))
    columns_to_convert_to_float = list(set(columns_to_convert_to_float))
    columns_to_convert_to_datetime = list(set(columns_to_convert_to_datetime))

    # Asegurar que las reglas de limpieza sean mutuamente excluyentes (una columna no debe estar
    # en múltiples listas de procesamiento para evitar conflictos).
    processed_cols = set(columns_to_drop_na) # Las columnas que ya se procesaron con dropna tienen prioridad.

    # Función auxiliar para filtrar y añadir columnas a las listas finales,
    # evitando duplicados y solapamientos.
    def filter_and_add(col_list, target_list):
        for col in col_list:
            if col not in processed_cols: # Si la columna no ha sido procesada aún...
                target_list.append(col)  # Añádela a la lista de destino.
                processed_cols.add(col)  # Márcala como procesada.
        return target_list

    # Se construyen las listas finales de columnas a procesar en un orden específico,
    # dando prioridad a las conversiones más específicas o a las que implican relleno de nulos.
    final_fill_na_zero_int = filter_and_add(columns_to_fill_na_zero_int, [])
    final_convert_to_datetime = filter_and_add(columns_to_convert_to_datetime, [])
    final_convert_to_int = filter_and_add(columns_to_convert_to_int, [])
    final_convert_to_float = filter_and_add(columns_to_convert_to_float, [])
    final_fill_na_str = filter_and_add(columns_to_fill_na_str, [])
    # Las columnas restantes que no cayeron en otra categoría se convierten a string por defecto.
    final_convert_to_str = filter_and_add(columns_to_convert_to_str, [])

    # --- Aplicar las Reglas de Limpieza Inferidas ---
    # Se itera sobre las listas finales y se aplican las transformaciones al DataFrame.

    # Rellenar nulos en columnas de string con una cadena vacía.
    for col in final_fill_na_str:
        if col in df_limpio.columns: # Se asegura que la columna exista.
            df_limpio[col] = df_limpio[col].fillna('')
    
    # Rellenar nulos con 0 y convertir a entero para columnas numéricas específicas.
    for col in final_fill_na_zero_int:
        if col in df_limpio.columns:
            # Convierte a numérico, los errores a NaN, rellena NaN con 0, y luego convierte a entero.
            df_limpio[col] = pd.to_numeric(df_limpio[col], errors='coerce').fillna(0).astype(int)

    # Convertir a entero. También maneja nulos rellenando con 0 si aparecen después de la conversión.
    for col in final_convert_to_int:
        if col in df_limpio.columns:
            df_limpio[col] = pd.to_numeric(df_limpio[col], errors='coerce').fillna(0).astype(int)
    
    # Convertir a flotante. Los valores que no se puedan convertir se mantienen como NaN.
    for col in final_convert_to_float:
        if col in df_limpio.columns:
            df_limpio[col] = pd.to_numeric(df_limpio[col], errors='coerce')
    
    # Convertir a tipo fecha y hora. Los valores que no se puedan convertir se convierten a NaT.
    for col in final_convert_to_datetime:
        if col in df_limpio.columns:
            df_limpio[col] = pd.to_datetime(df_limpio[col], errors='coerce', infer_datetime_format=True)
            # Nota: Los NaT (valores nulos de fecha) se manejarán como nulos en la conversión RDF posterior.
            # Se podría añadir un relleno de NaT aquí si se necesita un valor de fecha por defecto.

    # Convertir a string para todas las demás columnas no procesadas.
    for col in final_convert_to_str:
        if col in df_limpio.columns:
            df_limpio[col] = df_limpio[col].astype(str)

    return df_limpio # Retorna el DataFrame ya limpio y preprocesado.