import streamlit as st # Importa la biblioteca Streamlit para construir la interfaz de usuario web.
import pandas as pd # Importa la biblioteca Pandas para la manipulación y análisis de datos en formato DataFrame.
import io # Importa el módulo 'io' para trabajar con flujos de datos en memoria, útil para manejar archivos subidos.

# Importa funciones y namespaces personalizados desde otros módulos Python.
# 'limpiar_dataframe_generico' se importa desde 'limpiar_csv.py' para el preprocesamiento de datos.
from limpiar_csv import limpiar_dataframe_generico
# Se importan la función principal 'convertir_dataframe_a_rdf' y varios namespaces RDF
# (DRBER, XSD, FOAF, SCHEMA, DCT, BIBO, RDFS, OWL, URIRef) desde 'convertir_a_rdf.py'.
# Estos namespaces son cruciales para definir el vocabulario del grafo de conocimiento.
from convertir_a_rdf import convertir_dataframe_a_rdf, DRBER, XSD, FOAF, SCHEMA, DCT, BIBO, RDFS, OWL, URIRef

# --- Configuración Inicial de Streamlit ---
# Configura las propiedades básicas de la página web que se mostrará al usuario.
st.set_page_config(layout="wide", page_title="Conversor CSV a Grafo RDF - Interactivo") # Establece el diseño de la página (ancho completo) y el título de la pestaña del navegador.

st.title(" Conversor Interactivo de CSV a Grafo de Conocimiento RDF") # Muestra un título principal en la aplicación.
st.markdown(""" Esta aplicación te guía a través de un proceso de dos pasos para transformar tus datos CSV
en un Grafo de Conocimiento RDF, permitiendo una personalización completa y dinámica.
""")

# --- Estructura de Pestañas ---
# Define dos pestañas principales para organizar el flujo de trabajo de la aplicación.
tab1, tab2 = st.tabs([" Limpiar y Preparar CSV", " Generar Grafo RDF"])

# --- Pestaña 1: Limpiar y Preparar CSV ---
# Contenido y lógica para la primera pestaña.
with tab1:
    st.header("Paso 1: Limpiar y Preparar tu Archivo CSV") # Encabezado para esta sección.
    st.info("Sube tu archivo CSV y configura cómo debe ser leído y preprocesado.") # Mensaje informativo para el usuario.

    progress_text = "Operación en progreso. Por favor espera." # Texto para la barra de progreso.
    my_bar = st.progress(0, text=progress_text) # Inicializa una barra de progreso que se actualizará.
    my_bar.empty() # Oculta la barra de progreso inicialmente.

    csv_columns = st.session_state.get('csv_columns', []) # Recupera las columnas del CSV del estado de la sesión, o una lista vacía si no existen.

    # Widget para subir el archivo CSV.
    uploaded_file_tab1 = st.file_uploader("Sube tu archivo CSV aquí", type=["csv"], key="uploader_tab1")

    if uploaded_file_tab1 is not None: # Si se ha subido un archivo...
        st.success("Archivo cargado exitosamente. Ahora, configura la lectura.") # Mensaje de éxito.

        # --- Configuración del Delimitador Principal del CSV ---
        st.subheader("1.1 Configuración de Lectura del CSV") # Subencabezado.
        st.markdown("Por favor, especifica el delimitador principal de tu archivo CSV (ej. `,`, `;`, `\\t` para tabulador).") # Instrucciones.
        # Campo de entrada para el delimitador principal del CSV, con un valor por defecto y ayuda.
        csv_delimiter = st.text_input("Delimitador principal del CSV:", value=st.session_state.get('csv_delimiter_main', ","), help="El carácter que separa las columnas en tu archivo CSV.")

        try:
            # Intenta leer el archivo CSV con el delimitador especificado.
            df_crudo_tab1 = pd.read_csv(uploaded_file_tab1, sep=csv_delimiter, on_bad_lines='skip')
            st.success(f"CSV leído correctamente con '{csv_delimiter}' como delimitador.") # Mensaje de éxito si la lectura es correcta.
            
            st.subheader("1.2 Previsualización del CSV Cargado") # Subencabezado para la previsualización.
            st.dataframe(df_crudo_tab1.head(10)) # Muestra las primeras 10 filas del DataFrame crudo.

            # Almacena el DataFrame crudo y sus columnas en el estado de la sesión para persistencia.
            st.session_state['df_crudo'] = df_crudo_tab1
            st.session_state['csv_columns'] = df_crudo_tab1.columns.tolist()
            csv_columns = st.session_state['csv_columns'] # Actualiza la variable local.
            st.session_state['csv_delimiter_main'] = csv_delimiter # Guarda el delimitador principal.
            st.session_state['drber_namespace_uri'] = "http://drber.example.org/ns#" # Define y guarda el namespace por defecto.

            # --- Renombrar Columnas Dinámicamente ---
            st.subheader("1.3 Renombrar Columnas (Opcional)") # Subencabezado.
            st.info("Puedes renombrar las columnas de tu CSV aquí. Los nuevos nombres se usarán en el mapeo RDF.") # Instrucciones.
            
            # Inicializa o actualiza un DataFrame en el estado de la sesión para el renombramiento de columnas.
            # Esto permite al usuario ver y modificar los nombres de las columnas.
            if 'column_rename_df' not in st.session_state or st.session_state['column_rename_df'].shape[0] != len(csv_columns):
                st.session_state['column_rename_df'] = pd.DataFrame({
                    "Nombre Original": csv_columns, # Muestra el nombre original de la columna.
                    "Nuevo Nombre (Opcional)": csv_columns # Permite al usuario introducir un nuevo nombre.
                })
            
            # Muestra un editor de datos interactivo para el renombramiento de columnas.
            edited_df_renaming = st.data_editor(
                st.session_state['column_rename_df'],
                column_config={
                    "Nombre Original": st.column_config.TextColumn("Nombre Original", disabled=True), # Columna original deshabilitada para edición.
                    "Nuevo Nombre (Opcional)": st.column_config.TextColumn("Nuevo Nombre (Opcional)", help="Deja vacío para usar el nombre original") # Columna editable para el nuevo nombre.
                },
                num_rows="dynamic", # Permite añadir/eliminar filas (aunque aquí no se usa para columnas).
                hide_index=True, # Oculta el índice del DataFrame.
                key="column_renamer_editor" # Clave única para el widget.
            )
            st.session_state['column_rename_df'] = edited_df_renaming # Actualiza el DataFrame de renombramiento en el estado de la sesión.

            rename_map = {} # Diccionario para almacenar el mapeo de nombres originales a nuevos nombres.
            for _, row in edited_df_renaming.iterrows(): # Itera sobre las filas del DataFrame editado.
                original = row["Nombre Original"] # Nombre original.
                new = row["Nuevo Nombre (Opcional)"] # Nuevo nombre introducido por el usuario.
                if new and new != original: # Si hay un nuevo nombre y es diferente al original...
                    rename_map[original] = new # ...añade el mapeo al diccionario.
                else: # Si el nuevo nombre está vacío o es igual al original...
                    rename_map[original] = original # ...mantiene el nombre original.
            
            st.session_state['rename_map'] = rename_map # Guarda el mapa de renombramiento en el estado de la sesión.

            # --- Manejo de Columnas Multivaluadas ---
            st.subheader("1.4 Configuración de Columnas Multivaluadas") # Subencabezado.
            st.info("Si alguna columna contiene múltiples valores separados por un delimitador (ej. 'Valor1;Valor2'), "
                    "selecciónala y especifica su delimitador interno.") # Instrucciones.
            
            # Inicializa la lista de configuraciones de delimitadores multivaluados en el estado de la sesión.
            if 'multivalued_delimiters' not in st.session_state:
                st.session_state.multivalued_delimiters = []

            # Función para añadir un nuevo mapeo de columna multivaluada.
            def add_multivalued_mapping():
                st.session_state.multivalued_delimiters.append({"column": "-- Seleccionar --", "delimiter": ";"})
            
            # Botón para añadir un nuevo mapeo.
            st.button("➕ Añadir Columna Multivaluada", on_click=add_multivalued_mapping, key="add_multivalued_btn")

            multivalued_delimiters_to_process = [] # Lista para almacenar las configuraciones finales de columnas multivaluadas.
            # Obtiene la lista de columnas actuales después del posible renombramiento.
            current_cols_for_cleaning = df_crudo_tab1.rename(columns=rename_map).columns.tolist()

            # Itera sobre las configuraciones de delimitadores multivaluados existentes en el estado de la sesión.
            for i, mapping in enumerate(st.session_state.multivalued_delimiters):
                st.markdown(f"**Columna Multivaluada #{i+1}**") # Muestra el número de configuración.
                cols_mv = st.columns([0.4, 0.4, 0.2]) # Divide la fila en columnas para los widgets.
                with cols_mv[0]: # Columna para seleccionar la columna CSV.
                    selected_col_name_mv = st.selectbox(
                        f"Columna CSV:",
                        options=["-- Seleccionar --"] + current_cols_for_cleaning, # Opciones incluyen un valor por defecto y las columnas actuales.
                        # Establece el índice por defecto si la columna ya está mapeada.
                        index=(current_cols_for_cleaning.index(mapping["column"]) + 1 if mapping["column"] in current_cols_for_cleaning else 0),
                        key=f"mv_col_{i}" # Clave única.
                    )
                with cols_mv[1]: # Columna para introducir el delimitador.
                    delimiter_mv = st.text_input(
                        f"Delimitador interno:",
                        value=mapping["delimiter"], # Valor por defecto.
                        help="Ej: ';' para punto y coma, ',' para coma, '|' para barra vertical", # Ayuda.
                        key=f"mv_delimiter_{i}" # Clave única.
                    )
                with cols_mv[2]: # Columna para el botón de eliminar.
                    if st.button("🗑️", key=f"delete_mv_{i}"): # Botón de eliminar.
                        st.session_state.multivalued_delimiters.pop(i) # Elimina la configuración del estado de la sesión.
                        st.experimental_rerun() # Fuerza un re-ejecución de la aplicación para actualizar la interfaz.

                # Si se seleccionó una columna y se introdujo un delimitador, se añade a la lista final de procesamiento.
                if selected_col_name_mv != "-- Seleccionar --" and delimiter_mv.strip():
                    multivalued_delimiters_to_process.append({"column": selected_col_name_mv, "delimiter": delimiter_mv.strip()})
            
            # Guarda la configuración final de delimitadores multivaluados en el estado de la sesión.
            st.session_state['multivalued_delimiters_final'] = multivalued_delimiters_to_process

            st.markdown("---") # Separador visual.
            # Botón para aplicar la limpieza y pasar a la siguiente fase.
            if st.button("✅ Aplicar Limpieza y Preparar para RDF", key="apply_cleaning_btn"):
                my_bar.progress(70, text="Aplicando renombrado y preparando datos...") # Actualiza la barra de progreso.
                
                df_renamed = df_crudo_tab1.rename(columns=rename_map) # Renombra las columnas del DataFrame crudo.
                
                # Llama a la función de limpieza genérica importada, que infiere y aplica las reglas.
                df_limpio_final = limpiar_dataframe_generico(df_renamed.copy())
                
                # Guarda el DataFrame limpio y la configuración de delimitadores en el estado de la sesión.
                st.session_state['df_limpio_para_rdf'] = df_limpio_final
                st.session_state['multivalued_delimiters_for_rdf'] = multivalued_delimiters_to_process
                st.session_state['cleaning_done'] = True # Marca que la limpieza ha sido completada.
                my_bar.progress(100, text="¡CSV Limpio y listo para mapeo RDF!") # Actualiza la barra de progreso a 100%.
                st.success("CSV preparado. Ahora ve a la pestaña ' Generar Grafo RDF' para continuar.") # Mensaje de éxito.
                st.rerun() # Fuerza un re-ejecución para actualizar la interfaz y habilitar la segunda pestaña.

        except Exception as e: # Captura cualquier excepción que ocurra durante la lectura o preparación del CSV.
            my_bar.empty() # Oculta la barra de progreso.
            st.error(f"¡Ocurrió un error inesperado durante la lectura o preparación del CSV! Por favor, verifica el archivo y el delimitador principal. Detalles del error: {e}") # Muestra un mensaje de error.
            st.exception(e) # Muestra la traza completa del error para depuración.

    else: # Si no se ha subido ningún archivo CSV.
        st.info("Sube un archivo CSV en esta pestaña para comenzar la limpieza.") # Mensaje informativo.
        # Limpia el estado de la sesión de variables relacionadas con el CSV y el mapeo RDF,
        # asegurando un inicio limpio si el usuario vuelve a esta pestaña.
        for key in ['df_crudo', 'csv_columns', 'cleaning_done', 'column_rename_df',
                    'multivalued_delimiters', 'multivalued_delimiters_final', 'rename_map',
                    'column_rdf_mappings', 'main_entity_type_uri', 'main_entity_id_col']:
            if key in st.session_state:
                del st.session_state[key]


# --- Pestaña 2: Generar Grafo RDF ---
# Contenido y lógica para la segunda pestaña.
with tab2:
    progress_text_tab2 = "Generación de Grafo RDF en progreso. Por favor espera." # Texto para la barra de progreso.
    my_bar_tab2 = st.progress(0, text=progress_text_tab2) # Inicializa la barra de progreso para esta pestaña.
    my_bar_tab2.empty() # Oculta la barra de progreso inicialmente.

    st.header("Paso 2: Generar tu Grafo de Conocimiento RDF") # Encabezado para esta sección.

    # Solo permite acceder a esta pestaña si la limpieza del CSV ha sido completada.
    if 'cleaning_done' in st.session_state and st.session_state['cleaning_done']:
        # Recupera el DataFrame limpio y la configuración de delimitadores del estado de la sesión.
        df_limpio_para_rdf = st.session_state['df_limpio_para_rdf']
        csv_columns_renamed = df_limpio_para_rdf.columns.tolist() # Obtiene los nombres de las columnas ya renombradas.
        multivalued_delimiters_for_rdf = st.session_state['multivalued_delimiters_final']

        st.info("Tu CSV ha sido limpiado y preparado. Ahora, define el modelo de tu grafo de conocimiento RDF.") # Mensaje informativo.

        # --- Definición de la Entidad Principal ---
        st.subheader("2.1 Define la Entidad Principal de tu Grafo") # Subencabezado.
        st.markdown("Cada fila de tu CSV se convertirá en una instancia de esta entidad.") # Instrucciones.

        # Campo de entrada para la URI de la clase de la entidad principal.
        main_entity_type_uri = st.text_input(
            "URI de la Clase de la Entidad Principal:",
            value=st.session_state.get('main_entity_type_uri', f"{DRBER}Record"), # Valor por defecto.
            help="Ej: http://example.org/ns#Producto, schema:Article, foaf:Person" # Ayuda.
        )
        st.session_state['main_entity_type_uri'] = main_entity_type_uri # Guarda la URI en el estado de la sesión.

        # Selector para elegir la columna que actuará como ID único de la entidad principal.
        main_entity_id_col = st.selectbox(
            "Columna CSV para el ID Único de la Entidad Principal (recomendado):",
            options=["-- Seleccionar --"] + csv_columns_renamed, # Opciones incluyen un valor por defecto y las columnas renombradas.
            # Establece el índice por defecto si la columna ya está seleccionada.
            index=(csv_columns_renamed.index(st.session_state.get('main_entity_id_col', "-- Seleccionar --")) + 1 if st.session_state.get('main_entity_id_col') in csv_columns_renamed else 0),
            help="Selecciona una columna que contenga un identificador único para cada fila (ej. DOI, ID de Producto). Si no se selecciona, se usará el índice de la fila."
        )
        st.session_state['main_entity_id_col'] = main_entity_id_col # Guarda la columna de ID en el estado de la sesión.


        # --- Mapeo Dinámico de Todas las Columnas ---
        st.subheader("2.2 Mapeo de Columnas a Propiedades RDF") # Subencabezado.
        st.markdown("Para cada columna de tu CSV, define cómo se mapeará a una propiedad RDF.") # Instrucciones.

        # Lógica para obtener columnas candidatas a generar entidades relacionadas.
        entity_generating_cols = []
        # Primero, se añaden las columnas que ya están configuradas como propiedades de objeto.
        for col in csv_columns_renamed:
            if st.session_state.get('column_rdf_mappings', {}).get(col, {}).get('mapping_type') == 'object_property':
                entity_generating_cols.append(col)
        # Luego, se añaden heurísticamente otras columnas que podrían ser entidades (ej. "author", "journal").
        for col in csv_columns_renamed:
            if any(keyword in col.lower() for keyword in ['author', 'creator', 'organization', 'institution', 'journal', 'source', 'keyword', 'topic']):
                if col not in entity_generating_cols: # Evita duplicados.
                    entity_generating_cols.append(col)
        
        # Opciones para el selector "Esta propiedad aplica a:", incluyendo la entidad principal y las columnas que generan entidades.
        applies_to_options = ["main_entity (Entidad Principal de la Fila)"] + sorted(entity_generating_cols)


        # Inicializa el estado de los mapeos de columnas si no existe o si las columnas han cambiado.
        if 'column_rdf_mappings' not in st.session_state or set(st.session_state.column_rdf_mappings.keys()) != set(csv_columns_renamed):
            st.session_state.column_rdf_mappings = {} # Inicializa el diccionario de mapeos.
            # Auto-sugiere mapeos iniciales para todas las columnas basándose en sus nombres y tipos de datos.
            for col in csv_columns_renamed:
                inferred_datatype = str(XSD.string) # Tipo de dato XSD por defecto.
                if col in df_limpio_para_rdf.columns: # Si la columna existe en el DataFrame limpio...
                    # Inferencia de tipos de datos basada en Pandas.
                    if pd.api.types.is_integer_dtype(df_limpio_para_rdf[col]):
                        inferred_datatype = str(XSD.integer)
                    elif pd.api.types.is_float_dtype(df_limpio_para_rdf[col]):
                        inferred_datatype = str(XSD.double)
                    elif pd.api.types.is_bool_dtype(df_limpio_para_rdf[col]):
                        inferred_datatype = str(XSD.boolean)
                    elif pd.api.types.is_datetime64_any_dtype(df_limpio_para_rdf[col]):
                        inferred_datatype = str(XSD.dateTime)
                    # Heurística para años (ej. "2023").
                    elif df_limpio_para_rdf[col].dropna().astype(str).str.match(r'^\d{4}$').all() and pd.api.types.is_numeric_dtype(df_limpio_para_rdf[col]):
                        inferred_datatype = str(XSD.gYear)
                    # Heurística para URIs (ej. "http://example.com").
                    elif df_limpio_para_rdf[col].dropna().astype(str).str.startswith(('http://', 'https://')).any():
                        inferred_datatype = str(XSD.anyURI)

                suggested_prop_uri = f"{DRBER}{col.replace(' ', '_').lower()}" # URI de propiedad sugerida (por defecto, del namespace DRBER).
                suggested_mapping_type = "literal" # Tipo de mapeo sugerido por defecto.
                suggested_is_multivalued = False # Multivaluada sugerida por defecto.
                suggested_related_entity_type_uri = None # URI de entidad relacionada sugerida.
                suggested_related_entity_id_col = None # Columna de ID para entidad relacionada sugerida.
                suggested_applies_to_entity = "main_entity (Entidad Principal de la Fila)" # Entidad a la que aplica sugerida.

                # Sugerencias para propiedades comunes basadas en el nombre de la columna (heurísticas).
                if "title" in col.lower() or "titulo" in col.lower():
                    suggested_prop_uri = str(DCT.title)
                elif "name" in col.lower() or "nombre" in col.lower():
                    suggested_prop_uri = str(SCHEMA.name)
                elif "year" in col.lower() or "año" in col.lower() or "date" in col.lower() or "fecha" in col.lower():
                    suggested_prop_uri = str(DCT.date)
                elif ("id" in col.lower() or "identificador" in col.lower()) and col.lower() != main_entity_id_col.lower():
                     suggested_prop_uri = str(DCT.identifier)
                elif "description" in col.lower() or "abstract" in col.lower() or "resumen" in col.lower():
                    suggested_prop_uri = str(DCT.description)
                elif "url" in col.lower() or "link" in col.lower():
                    suggested_prop_uri = str(SCHEMA.url)
                elif "author" in col.lower() or "autor" in col.lower() or "creador" in col.lower():
                    suggested_prop_uri = str(DCT.creator)
                    suggested_mapping_type = "object_property" # Sugiere propiedad de objeto.
                    suggested_is_multivalued = True # Sugiere multivaluada.
                    suggested_related_entity_type_uri = str(FOAF.Person) # Sugiere tipo foaf:Person.
                    suggested_related_entity_id_col = None # No sugiere una columna de ID específica.
                elif "journal" in col.lower() or "source_title" in col.lower() or "source title" in col.lower():
                    suggested_prop_uri = str(DCT.publisher)
                    suggested_mapping_type = "object_property"
                    suggested_is_multivalued = False
                    suggested_related_entity_type_uri = str(BIBO.Journal)
                    suggested_related_entity_id_col = None
                elif "institution" in col.lower() or "organization" in col.lower() or "funder" in col.lower() or "funding_details" in col.lower():
                    suggested_prop_uri = str(SCHEMA.funder)
                    suggested_mapping_type = "object_property"
                    suggested_is_multivalued = True
                    suggested_related_entity_type_uri = str(SCHEMA.Organization)
                    suggested_related_entity_id_col = None
                elif "keyword" in col.lower() or "subject" in col.lower() or "topic" in col.lower():
                    suggested_prop_uri = str(DCT.subject)
                    suggested_mapping_type = "object_property"
                    suggested_is_multivalued = True
                    suggested_related_entity_type_uri = str(BIBO.Topic)
                    suggested_related_entity_id_col = None

                # Almacena las sugerencias de mapeo en el estado de la sesión.
                st.session_state.column_rdf_mappings[col] = {
                    "map": True, # Por defecto, sugiere mapear la columna.
                    "prop_uri": suggested_prop_uri,
                    "mapping_type": suggested_mapping_type,
                    "datatype": inferred_datatype,
                    "is_multivalued": suggested_is_multivalued,
                    "delimiter": ";",
                    "related_entity_type_uri": suggested_related_entity_type_uri,
                    "related_entity_id_col": suggested_related_entity_id_col,
                    "applies_to_entity": suggested_applies_to_entity
                }

        final_column_rdf_mappings = {} # Diccionario para almacenar los mapeos finales que se usarán para generar el grafo.
        # Itera sobre cada columna renombrada para mostrar los widgets de mapeo individuales.
        for i, col_name in enumerate(csv_columns_renamed):
            st.markdown(f"---") # Separador visual para cada configuración de columna.
            st.markdown(f"**Configuración para Columna: `{col_name}`**") # Título para la configuración de la columna actual.

            # Recupera la configuración actual de mapeo para esta columna del estado de la sesión,
            # o usa valores por defecto si no existe (esto no debería ocurrir si la inicialización es correcta).
            current_mapping = st.session_state.column_rdf_mappings.get(col_name, {
                "map": False,
                "prop_uri": f"{DRBER}{col_name.replace(' ', '_').lower()}",
                "mapping_type": "literal",
                "datatype": str(XSD.string),
                "is_multivalued": False,
                "delimiter": ";",
                "related_entity_type_uri": None,
                "related_entity_id_col": None,
                "applies_to_entity": "main_entity (Entidad Principal de la Fila)"
            })

            # Casilla de verificación para decidir si mapear la columna.
            map_col = st.checkbox(
                f"Mapear `{col_name}` a RDF?",
                value=current_mapping["map"],
                key=f"map_col_{i}"
            )
            current_mapping["map"] = map_col # Actualiza el estado del mapeo.

            if map_col: # Si la columna está marcada para mapear...
                cols_map_detail = st.columns([0.4, 0.6]) # Divide la fila en dos columnas para los detalles del mapeo.
                with cols_map_detail[0]: # Columna izquierda para URI de propiedad y tipo de mapeo.
                    prop_uri = st.text_input(
                        f"URI de la Propiedad RDF para `{col_name}`:",
                        value=current_mapping["prop_uri"],
                        help="Ej: http://example.org/prop#miCampo o schema:name",
                        key=f"prop_uri_{i}"
                    )
                    current_mapping["prop_uri"] = prop_uri # Actualiza la URI de la propiedad.

                    mapping_type = st.radio(
                        f"Tipo de Mapeo para `{col_name}`:",
                        options=["Literal (Texto/Número/Fecha)", "Entidad Relacionada"],
                        index=0 if current_mapping["mapping_type"] == "literal" else 1, # Selecciona el tipo de mapeo actual.
                        key=f"mapping_type_{i}"
                    )
                    # Actualiza el tipo de mapeo en el diccionario.
                    current_mapping["mapping_type"] = "literal" if mapping_type == "Literal (Texto/Número/Fecha)" else "object_property"

                with cols_map_detail[1]: # Columna derecha para detalles específicos del tipo de mapeo.
                    if current_mapping["mapping_type"] == "literal": # Si el mapeo es a un literal...
                        datatype_options = [str(XSD.string), str(XSD.integer), str(XSD.double), str(XSD.boolean), str(XSD.date), str(XSD.dateTime), str(XSD.gYear), str(XSD.anyURI)]
                        
                        try: # Intenta encontrar el índice del tipo de dato actual en las opciones.
                            default_datatype_index = datatype_options.index(current_mapping.get("datatype", str(XSD.string)))
                        except ValueError: # Si no se encuentra, usa el primer elemento (string).
                            default_datatype_index = 0

                        datatype = st.selectbox(
                            f"Tipo de Dato RDF para `{col_name}`:",
                            options=datatype_options,
                            index=default_datatype_index,
                            key=f"datatype_{i}"
                        )
                        current_mapping["datatype"] = datatype # Actualiza el tipo de dato.
                        # Elimina las propiedades específicas de 'object_property' si el tipo es 'literal'.
                        current_mapping.pop("related_entity_type_uri", None)
                        current_mapping.pop("related_entity_id_col", None)
                        # Reinicia 'applies_to_entity' a la entidad principal para literales.
                        current_mapping["applies_to_entity"] = "main_entity (Entidad Principal de la Fila)"

                    else: # Si el mapeo es a una propiedad de objeto (entidad relacionada)...
                        related_entity_type_uri = st.text_input(
                            f"URI de la Clase de Entidad Relacionada para `{col_name}`:",
                            value=current_mapping.get("related_entity_type_uri", f"{DRBER}{col_name.replace(' ', '_').lower()}Type"), # Valor por defecto.
                            help="Ej: foaf:Person, schema:Organization, http://example.org/ns#MiClase",
                            key=f"related_entity_type_uri_{i}"
                        )
                        current_mapping["related_entity_type_uri"] = related_entity_type_uri # Actualiza la URI de la clase de la entidad relacionada.

                        related_entity_id_col_options = ["-- Usar Valor de Columna Actual --"] + csv_columns_renamed # Opciones para la columna de ID.
                        related_entity_id_col_default_index = 0
                        # Establece el índice por defecto si la columna de ID ya está seleccionada.
                        if current_mapping.get("related_entity_id_col") in csv_columns_renamed:
                            related_entity_id_col_default_index = csv_columns_renamed.index(current_mapping["related_entity_id_col"]) + 1

                        related_entity_id_col_selected = st.selectbox(
                            f"Columna para ID Único de Entidad Relacionada para `{col_name}` (opcional):",
                            options=related_entity_id_col_options,
                            index=related_entity_id_col_default_index,
                            help="Si la entidad relacionada tiene su propio ID en otra columna. Si no, se usa el valor de la columna actual.",
                            key=f"related_entity_id_col_{i}"
                        )
                        # Guarda la columna de ID seleccionada (o None si se usa el valor de la columna actual).
                        current_mapping["related_entity_id_col"] = related_entity_id_col_selected if related_entity_id_col_selected != "-- Usar Valor de Columna Actual --" else None
                        current_mapping.pop("datatype", None) # Elimina la propiedad 'datatype' si el tipo es 'object_property'.
                        # Reinicia 'applies_to_entity' a la entidad principal para propiedades de objeto.
                        current_mapping["applies_to_entity"] = "main_entity (Entidad Principal de la Fila)"

                # Casilla de verificación para indicar si la columna es multivaluada.
                is_multivalued = st.checkbox(
                    f"¿Es `{col_name}` una columna multivaluada?",
                    value=current_mapping["is_multivalued"],
                    key=f"is_multivalued_{i}"
                )
                current_mapping["is_multivalued"] = is_multivalued # Actualiza el estado.

                if is_multivalued: # Si es multivaluada, pide el delimitador.
                    delimiter = st.text_input(
                        f"Delimitador para `{col_name}`:",
                        value=current_mapping["delimiter"],
                        help="Ej: ';' para punto y coma, ',' para coma, '|' para barra vertical",
                        key=f"delimiter_{i}"
                    )
                    current_mapping["delimiter"] = delimiter # Actualiza el delimitador.
                else:
                    current_mapping["delimiter"] = ";" # Restablece el delimitador a ';' si no es multivaluada.

                # --- NUEVO WIDGET: A qué entidad aplica esta propiedad ---
                # Determina dinámicamente las opciones para el selector 'applies_to_entity'.
                current_applies_to_options = ["main_entity (Entidad Principal de la Fila)"]
                # Añade las columnas que ya están configuradas como propiedades de objeto (generadoras de entidades).
                for c_name, c_map in st.session_state.column_rdf_mappings.items():
                    if c_map.get('mapping_type') == 'object_property' and c_map.get('map') and c_name != col_name:
                        current_applies_to_options.append(c_name)
                # Elimina duplicados y ordena las opciones.
                current_applies_to_options = sorted(list(set(current_applies_to_options)))

                # Asegura que el valor actual esté en las opciones, si no, por defecto a 'main_entity'.
                default_applies_to_index = 0
                if current_mapping.get("applies_to_entity") in current_applies_to_options:
                    default_applies_to_index = current_applies_to_options.index(current_mapping["applies_to_entity"])
                elif current_mapping.get("applies_to_entity") == "main_entity": # Maneja el nombre de visualización.
                     default_applies_to_index = current_applies_to_options.index("main_entity (Entidad Principal de la Fila)")


                if current_mapping["mapping_type"] == "literal": # Si es una propiedad literal...
                    applies_to_entity_selected = st.selectbox(
                        f"Esta propiedad aplica a:",
                        options=current_applies_to_options,
                        index=default_applies_to_index,
                        help="Define si esta propiedad pertenece a la entidad principal de la fila o a una entidad relacionada creada por otra columna.",
                        key=f"applies_to_entity_{i}"
                    )
                    # Almacena solo el nombre de la columna (o "main_entity") sin el texto entre paréntesis.
                    current_mapping["applies_to_entity"] = applies_to_entity_selected.split(' ')[0]
                else: # Si es una propiedad de objeto...
                    # Siempre aplica a la entidad principal, ya que la propiedad de objeto crea la relación desde la principal.
                    current_mapping["applies_to_entity"] = "main_entity"


                final_column_rdf_mappings[col_name] = current_mapping # Añade la configuración final al diccionario.
            else:
                # Si la columna no se mapea, se asegura de que no esté en el diccionario final.
                if col_name in final_column_rdf_mappings:
                    del final_column_rdf_mappings[col_name]

            st.session_state.column_rdf_mappings[col_name] = current_mapping # Actualiza el estado de la sesión para persistencia.

        st.markdown("---") # Separador visual.
        # Botón para iniciar la generación del grafo RDF.
        if st.button("✨ Generar Grafo RDF", key="generate_rdf_btn"):
            my_bar_tab2.progress(10, text="Validando mapeos...") # Actualiza la barra de progreso.

            # --- Validaciones antes de la generación del grafo ---
            # Valida que la URI de la clase de la entidad principal no esté vacía.
            if not main_entity_type_uri.strip():
                st.error("Error: La URI de la Clase de la Entidad Principal no puede estar vacía.")
                my_bar_tab2.empty()
                st.stop() # Detiene la ejecución de la aplicación.

            # Valida que se haya seleccionado una columna para el ID de la entidad principal.
            if main_entity_id_col == "-- Seleccionar --":
                st.error("Error: Debes seleccionar una Columna CSV para el ID Único de la Entidad Principal.")
                my_bar_tab2.empty()
                st.stop()

            # Advertencia si no se ha configurado ningún mapeo de propiedades.
            if not final_column_rdf_mappings:
                st.warning("Advertencia: No se ha configurado ningún mapeo de columnas a propiedades RDF. El grafo resultante estará vacío o muy limitado.")

            # Valida cada mapeo de propiedad individualmente.
            for col_name, mapping in final_column_rdf_mappings.items():
                if not mapping["prop_uri"].strip(): # Valida que la URI de la propiedad no esté vacía.
                    st.error(f"Error: La URI de la propiedad para la columna '{col_name}' no puede estar vacía.")
                    my_bar_tab2.empty()
                    st.stop()
                # Si es una propiedad de objeto, valida que la URI de la clase de entidad relacionada no esté vacía.
                if mapping["mapping_type"] == "object_property" and not mapping["related_entity_type_uri"].strip():
                    st.error(f"Error: La URI de la Clase de Entidad Relacionada para la columna '{col_name}' no puede estar vacía si el tipo de mapeo es 'Entidad Relacionada'.")
                    my_bar_tab2.empty()
                    st.stop()
                
                # Advertencia si la URI de la propiedad no parece válida (no empieza con http/https ni tiene prefijo).
                if not (mapping["prop_uri"].startswith("http://") or mapping["prop_uri"].startswith("https://") or ":" in mapping["prop_uri"]):
                    st.warning(f"Advertencia: La URI de la propiedad '{mapping['prop_uri']}' para la columna '{col_name}' parece no ser una URI completa o un prefijo válido (ej. `foaf:name`). Asegúrate de que es correcta.")
                # Advertencia similar para la URI de la clase de entidad relacionada.
                if mapping["mapping_type"] == "object_property" and not (mapping["related_entity_type_uri"].startswith("http://") or mapping["related_entity_type_uri"].startswith("https://") or ":" in mapping["related_entity_type_uri"]):
                    st.warning(f"Advertencia: La URI de la Clase de Entidad Relacionada '{mapping['related_entity_type_uri']}' para la columna '{col_name}' parece no ser una URI completa o un prefijo válido. Asegúrate de que es correcta.")


            my_bar_tab2.progress(25, text="Mapeos completados. Iniciando conversión...") # Actualiza la barra de progreso.

            # Llama a la función principal de conversión a RDF.
            g = convertir_dataframe_a_rdf(
                df_limpio_para_rdf, # DataFrame limpio.
                main_entity_type_uri, # URI de la clase principal.
                main_entity_id_col, # Columna de ID principal.
                multivalued_delimiters_for_rdf, # Configuración de delimitadores multivaluados.
                final_column_rdf_mappings # Mapeos de columnas a RDF.
            )
            my_bar_tab2.progress(75, text="Grafo RDF generado. Serializando a Turtle y RDF/XML...") # Actualiza la barra de progreso.

            # Serializa el grafo a los formatos Turtle y RDF/XML.
            rdf_output_ttl = g.serialize(format='turtle')
            rdf_output_xml = g.serialize(format='xml')

            my_bar_tab2.progress(100, text="¡Conversión completada!") # Actualiza la barra de progreso a 100%.
            st.success("¡Grafo RDF generado exitosamente!") # Mensaje de éxito final.

            st.subheader("Archivos RDF Generados:") # Subencabezado.

            # Botón para descargar el grafo en formato Turtle.
            st.download_button(
                label="Descargar RDF en Turtle (.ttl)",
                data=rdf_output_ttl,
                file_name="knowledge_graph.ttl",
                mime="text/turtle",
                key="download_ttl_final"
            )

            # Botón para descargar el grafo en formato RDF/XML.
            st.download_button(
                label="Descargar RDF en RDF/XML (.rdf)",
                data=rdf_output_xml,
                file_name="knowledge_graph.rdf",
                mime="application/rdf+xml",
                key="download_rdfxml_final"
            )

            st.subheader("Contenido del Grafo RDF (formato Turtle):") # Subencabezado para la visualización del código.
            st.code(rdf_output_ttl, language='turtle') # Muestra el contenido del grafo en formato Turtle.

            st.subheader("Herramientas Útiles para RDF") # Subencabezado para herramientas externas.
            st.markdown(""" # Instrucciones para herramientas externas.
            Una vez descargados tus archivos RDF, puedes usar estas herramientas en línea
            para validarlos o convertirlos a otros formatos:
            """)

            st.markdown(
                """
                * **Validador de RDF del W3C:**
                    [https://www.w3.org/RDF/Validator/](https://www.w3.org/RDF/Validator/)
                    (Útil para verificar la sintaxis y la validez de tu archivo RDF)
                """
            )
            st.markdown(
                """
                * **EasyRDF Converter:**
                    [https://www.easyrdf.org/converter](https://www.easyrdf.org/converter)
                    (Permite convertir entre diferentes serializaciones de RDF, como Turtle, RDF/XML, N-Triples, etc.)
                """
            )
            st.info("Puedes copiar el contenido del grafo (arriba) y pegarlo directamente en estas herramientas, o subir los archivos que descargaste.") # Mensaje de ayuda.

    else: # Si la limpieza del CSV no ha sido completada.
        st.warning("Por favor, primero sube y prepara tu CSV en la pestaña '📊 Limpiar y Preparar CSV'.") # Mensaje de advertencia.

st.markdown("---") # Separador visual al final de la aplicación.