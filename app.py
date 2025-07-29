# app.py
# Este script es el punto de entrada de la aplicación Streamlit.
# Implementa una interfaz de usuario con pestañas para un flujo de trabajo claro:
# 1. Limpieza y preparación del CSV.
# 2. Mapeo de columnas a un modelo semántico y generación del grafo RDF.

import streamlit as st # Biblioteca principal para crear aplicaciones web interactivas.
import pandas as pd # Para manipulación y análisis de datos en DataFrames.
import io # Para trabajar con flujos de datos en memoria.

# Importa las funciones personalizadas de limpieza y conversión desde otros módulos Python.
from limpiar_csv import limpiar_dataframe_generico
# Importa la función de conversión. Ya no importamos 'semantic_fields' ni 'required_for_article'
# directamente, ya que el mapeo es dinámico.
from convertir_a_rdf import convertir_dataframe_a_rdf, DRBER, XSD, FOAF, SCHEMA, DCT, BIBO, RDFS, OWL, URIRef

# --- Configuración Inicial de Streamlit ---
# Establece las propiedades básicas de la página web.
st.set_page_config(layout="wide", page_title="Conversor CSV a Grafo RDF - Interactivo")

st.title("📚 Conversor Interactivo de CSV a Grafo de Conocimiento RDF")
st.markdown("""
Esta aplicación te guía a través de un proceso de dos pasos para transformar tus datos CSV
en un Grafo de Conocimiento RDF, permitiendo una personalización completa y dinámica.
""")

# --- Estructura de Pestañas ---
tab1, tab2 = st.tabs(["📊 Limpiar y Preparar CSV", "🔗 Generar Grafo RDF"])

# --- Pestaña 1: Limpiar y Preparar CSV ---
with tab1:
    st.header("Paso 1: Limpiar y Preparar tu Archivo CSV")
    st.info("Sube tu archivo CSV y configura cómo debe ser leído y preprocesado.")

    progress_text = "Operación en progreso. Por favor espera."
    my_bar = st.progress(0, text=progress_text)
    my_bar.empty()

    csv_columns = st.session_state.get('csv_columns', [])

    uploaded_file_tab1 = st.file_uploader("Sube tu archivo CSV aquí", type=["csv"], key="uploader_tab1")

    if uploaded_file_tab1 is not None:
        st.success("Archivo cargado exitosamente. Ahora, configura la lectura.")

        # --- Configuración del Delimitador Principal del CSV ---
        st.subheader("1.1 Configuración de Lectura del CSV")
        st.markdown("Por favor, especifica el delimitador principal de tu archivo CSV (ej. `,`, `;`, `\\t` para tabulador).")
        csv_delimiter = st.text_input("Delimitador principal del CSV:", value=st.session_state.get('csv_delimiter_main', ","), help="El carácter que separa las columnas en tu archivo CSV.")

        try:
            df_crudo_tab1 = pd.read_csv(uploaded_file_tab1, sep=csv_delimiter, on_bad_lines='skip')
            st.success(f"CSV leído correctamente con '{csv_delimiter}' como delimitador.")
            
            st.subheader("1.2 Previsualización del CSV Cargado")
            st.dataframe(df_crudo_tab1.head(10))

            st.session_state['df_crudo'] = df_crudo_tab1
            st.session_state['csv_columns'] = df_crudo_tab1.columns.tolist()
            csv_columns = st.session_state['csv_columns']
            st.session_state['csv_delimiter_main'] = csv_delimiter
            st.session_state['drber_namespace_uri'] = "http://drber.example.org/ns#"

            # --- Renombrar Columnas Dinámicamente ---
            st.subheader("1.3 Renombrar Columnas (Opcional)")
            st.info("Puedes renombrar las columnas de tu CSV aquí. Los nuevos nombres se usarán en el mapeo RDF.")
            
            if 'column_rename_df' not in st.session_state or st.session_state['column_rename_df'].shape[0] != len(csv_columns):
                st.session_state['column_rename_df'] = pd.DataFrame({
                    "Nombre Original": csv_columns,
                    "Nuevo Nombre (Opcional)": csv_columns
                })
            
            edited_df_renaming = st.data_editor(
                st.session_state['column_rename_df'],
                column_config={
                    "Nombre Original": st.column_config.TextColumn("Nombre Original", disabled=True),
                    "Nuevo Nombre (Opcional)": st.column_config.TextColumn("Nuevo Nombre (Opcional)", help="Deja vacío para usar el nombre original")
                },
                num_rows="dynamic",
                hide_index=True,
                key="column_renamer_editor"
            )
            st.session_state['column_rename_df'] = edited_df_renaming

            rename_map = {}
            for _, row in edited_df_renaming.iterrows():
                original = row["Nombre Original"]
                new = row["Nuevo Nombre (Opcional)"]
                if new and new != original:
                    rename_map[original] = new
                else:
                    rename_map[original] = original
            
            st.session_state['rename_map'] = rename_map

            # --- Manejo de Columnas Multivaluadas (Esta sección se mantiene) ---
            st.subheader("1.4 Configuración de Columnas Multivaluadas")
            st.info("Si alguna columna contiene múltiples valores separados por un delimitador (ej. 'Valor1;Valor2'), "
                    "selecciónala y especifica su delimitador interno.")
            
            if 'multivalued_delimiters' not in st.session_state:
                st.session_state.multivalued_delimiters = []

            def add_multivalued_mapping():
                st.session_state.multivalued_delimiters.append({"column": "-- Seleccionar --", "delimiter": ";"})
            
            st.button("➕ Añadir Columna Multivaluada", on_click=add_multivalued_mapping, key="add_multivalued_btn")

            multivalued_delimiters_to_process = []
            current_cols_for_cleaning = df_crudo_tab1.rename(columns=rename_map).columns.tolist()

            for i, mapping in enumerate(st.session_state.multivalued_delimiters):
                st.markdown(f"**Columna Multivaluada #{i+1}**")
                cols_mv = st.columns([0.4, 0.4, 0.2])
                with cols_mv[0]:
                    selected_col_name_mv = st.selectbox(
                        f"Columna CSV:",
                        options=["-- Seleccionar --"] + current_cols_for_cleaning,
                        index=(current_cols_for_cleaning.index(mapping["column"]) + 1 if mapping["column"] in current_cols_for_cleaning else 0),
                        key=f"mv_col_{i}"
                    )
                with cols_mv[1]:
                    delimiter_mv = st.text_input(
                        f"Delimitador interno:",
                        value=mapping["delimiter"],
                        help="Ej: ';' para punto y coma, ',' para coma, '|' para barra vertical",
                        key=f"mv_delimiter_{i}"
                    )
                with cols_mv[2]:
                    if st.button("🗑️", key=f"delete_mv_{i}"):
                        st.session_state.multivalued_delimiters.pop(i)
                        st.experimental_rerun()

                if selected_col_name_mv != "-- Seleccionar --" and delimiter_mv.strip():
                    multivalued_delimiters_to_process.append({"column": selected_col_name_mv, "delimiter": delimiter_mv.strip()})
            
            st.session_state['multivalued_delimiters_final'] = multivalued_delimiters_to_process

            st.markdown("---")
            if st.button("✅ Aplicar Limpieza y Preparar para RDF", key="apply_cleaning_btn"):
                my_bar.progress(70, text="Aplicando renombrado y preparando datos...")
                
                df_renamed = df_crudo_tab1.rename(columns=rename_map)
                
                # La función de limpieza genérica infiere las reglas internamente.
                df_limpio_final = limpiar_dataframe_generico(df_renamed.copy())
                
                st.session_state['df_limpio_para_rdf'] = df_limpio_final
                st.session_state['multivalued_delimiters_for_rdf'] = multivalued_delimiters_to_process
                st.session_state['cleaning_done'] = True
                my_bar.progress(100, text="¡CSV Limpio y listo para mapeo RDF!")
                st.success("CSV preparado. Ahora ve a la pestaña '🔗 Generar Grafo RDF' para continuar.")
                st.rerun()

        except Exception as e:
            my_bar.empty()
            st.error(f"¡Ocurrió un error inesperado durante la lectura o preparación del CSV! Por favor, verifica el archivo y el delimitador principal. Detalles del error: {e}")
            st.exception(e)

    else:
        st.info("Sube un archivo CSV en esta pestaña para comenzar la limpieza.")
        for key in ['df_crudo', 'csv_columns', 'cleaning_done', 'column_rename_df',
                    'multivalued_delimiters', 'multivalued_delimiters_final', 'rename_map',
                    'column_rdf_mappings', 'main_entity_type_uri', 'main_entity_id_col']:
            if key in st.session_state:
                del st.session_state[key]


# --- Pestaña 2: Generar Grafo RDF ---
with tab2:
    progress_text_tab2 = "Generación de Grafo RDF en progreso. Por favor espera."
    my_bar_tab2 = st.progress(0, text=progress_text_tab2)
    my_bar_tab2.empty()

    st.header("Paso 2: Generar tu Grafo de Conocimiento RDF")

    if 'cleaning_done' in st.session_state and st.session_state['cleaning_done']:
        df_limpio_para_rdf = st.session_state['df_limpio_para_rdf']
        csv_columns_renamed = df_limpio_para_rdf.columns.tolist()
        multivalued_delimiters_for_rdf = st.session_state['multivalued_delimiters_final']

        st.info("Tu CSV ha sido limpiado y preparado. Ahora, define el modelo de tu grafo de conocimiento RDF.")

        # --- Definición de la Entidad Principal ---
        st.subheader("2.1 Define la Entidad Principal de tu Grafo")
        st.markdown("Cada fila de tu CSV se convertirá en una instancia de esta entidad.")

        main_entity_type_uri = st.text_input(
            "URI de la Clase de la Entidad Principal:",
            value=st.session_state.get('main_entity_type_uri', f"{DRBER}Record"),
            help="Ej: http://example.org/ns#Producto, schema:Article, foaf:Person"
        )
        st.session_state['main_entity_type_uri'] = main_entity_type_uri

        main_entity_id_col = st.selectbox(
            "Columna CSV para el ID Único de la Entidad Principal (recomendado):",
            options=["-- Seleccionar --"] + csv_columns_renamed,
            index=(csv_columns_renamed.index(st.session_state.get('main_entity_id_col', "-- Seleccionar --")) + 1 if st.session_state.get('main_entity_id_col') in csv_columns_renamed else 0),
            help="Selecciona una columna que contenga un identificador único para cada fila (ej. DOI, ID de Producto). Si no se selecciona, se usará el índice de la fila."
        )
        st.session_state['main_entity_id_col'] = main_entity_id_col


        # --- Mapeo Dinámico de Todas las Columnas ---
        st.subheader("2.2 Mapeo de Columnas a Propiedades RDF")
        st.markdown("Para cada columna de tu CSV, define cómo se mapeará a una propiedad RDF.")

        # Obtener las columnas que son candidatas a generar entidades relacionadas
        entity_generating_cols = []
        # Primero, añadir las que ya están configuradas como object_property
        for col in csv_columns_renamed:
            if st.session_state.get('column_rdf_mappings', {}).get(col, {}).get('mapping_type') == 'object_property':
                entity_generating_cols.append(col)
        # Luego, añadir heurísticamente otras que podrían ser entidades
        for col in csv_columns_renamed:
            if any(keyword in col.lower() for keyword in ['author', 'creator', 'organization', 'institution', 'journal', 'source', 'keyword', 'topic']):
                if col not in entity_generating_cols: # Evitar duplicados
                    entity_generating_cols.append(col)
        
        # Añadir la opción 'main_entity' al principio
        applies_to_options = ["main_entity (Entidad Principal de la Fila)"] + sorted(entity_generating_cols)


        # Inicializar el estado de los mapeos de columnas si no existe
        if 'column_rdf_mappings' not in st.session_state or set(st.session_state.column_rdf_mappings.keys()) != set(csv_columns_renamed):
            st.session_state.column_rdf_mappings = {}
            # Auto-sugerir mapeos iniciales para todas las columnas
            for col in csv_columns_renamed:
                inferred_datatype = str(XSD.string)
                if col in df_limpio_para_rdf.columns:
                    if pd.api.types.is_integer_dtype(df_limpio_para_rdf[col]):
                        inferred_datatype = str(XSD.integer)
                    elif pd.api.types.is_float_dtype(df_limpio_para_rdf[col]):
                        inferred_datatype = str(XSD.double)
                    elif pd.api.types.is_bool_dtype(df_limpio_para_rdf[col]):
                        inferred_datatype = str(XSD.boolean)
                    elif pd.api.types.is_datetime64_any_dtype(df_limpio_para_rdf[col]):
                        inferred_datatype = str(XSD.dateTime)
                    elif df_limpio_para_rdf[col].dropna().astype(str).str.match(r'^\d{4}$').all() and pd.api.types.is_numeric_dtype(df_limpio_para_rdf[col]):
                        inferred_datatype = str(XSD.gYear)
                    elif df_limpio_para_rdf[col].dropna().astype(str).str.startswith(('http://', 'https://')).any():
                        inferred_datatype = str(XSD.anyURI)

                suggested_prop_uri = f"{DRBER}{col.replace(' ', '_').lower()}"
                suggested_mapping_type = "literal"
                suggested_is_multivalued = False
                suggested_related_entity_type_uri = None
                suggested_related_entity_id_col = None
                suggested_applies_to_entity = "main_entity (Entidad Principal de la Fila)"

                # Sugerencias para propiedades comunes (si la columna se parece a algo conocido)
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
                    suggested_mapping_type = "object_property"
                    suggested_is_multivalued = True
                    suggested_related_entity_type_uri = str(FOAF.Person)
                    suggested_related_entity_id_col = None
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

                st.session_state.column_rdf_mappings[col] = {
                    "map": True,
                    "prop_uri": suggested_prop_uri,
                    "mapping_type": suggested_mapping_type,
                    "datatype": inferred_datatype,
                    "is_multivalued": suggested_is_multivalued,
                    "delimiter": ";",
                    "related_entity_type_uri": suggested_related_entity_type_uri,
                    "related_entity_id_col": suggested_related_entity_id_col,
                    "applies_to_entity": suggested_applies_to_entity
                }

        # Iterar sobre cada columna renombrada para mostrar los widgets de mapeo
        final_column_rdf_mappings = {}
        for i, col_name in enumerate(csv_columns_renamed):
            st.markdown(f"---")
            st.markdown(f"**Configuración para Columna: `{col_name}`**")

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

            map_col = st.checkbox(
                f"Mapear `{col_name}` a RDF?",
                value=current_mapping["map"],
                key=f"map_col_{i}"
            )
            current_mapping["map"] = map_col

            if map_col:
                cols_map_detail = st.columns([0.4, 0.6])
                with cols_map_detail[0]:
                    prop_uri = st.text_input(
                        f"URI de la Propiedad RDF para `{col_name}`:",
                        value=current_mapping["prop_uri"],
                        help="Ej: http://example.org/prop#miCampo o schema:name",
                        key=f"prop_uri_{i}"
                    )
                    current_mapping["prop_uri"] = prop_uri

                    mapping_type = st.radio(
                        f"Tipo de Mapeo para `{col_name}`:",
                        options=["Literal (Texto/Número/Fecha)", "Entidad Relacionada"],
                        index=0 if current_mapping["mapping_type"] == "literal" else 1,
                        key=f"mapping_type_{i}"
                    )
                    current_mapping["mapping_type"] = "literal" if mapping_type == "Literal (Texto/Número/Fecha)" else "object_property"

                with cols_map_detail[1]:
                    if current_mapping["mapping_type"] == "literal":
                        datatype_options = [str(XSD.string), str(XSD.integer), str(XSD.double), str(XSD.boolean), str(XSD.date), str(XSD.dateTime), str(XSD.gYear), str(XSD.anyURI)]
                        
                        try:
                            default_datatype_index = datatype_options.index(current_mapping.get("datatype", str(XSD.string)))
                        except ValueError:
                            default_datatype_index = 0

                        datatype = st.selectbox(
                            f"Tipo de Dato RDF para `{col_name}`:",
                            options=datatype_options,
                            index=default_datatype_index,
                            key=f"datatype_{i}"
                        )
                        current_mapping["datatype"] = datatype
                        current_mapping.pop("related_entity_type_uri", None)
                        current_mapping.pop("related_entity_id_col", None)
                        current_mapping["applies_to_entity"] = "main_entity (Entidad Principal de la Fila)" # Reset for literal

                    else: # object_property
                        related_entity_type_uri = st.text_input(
                            f"URI de la Clase de Entidad Relacionada para `{col_name}`:",
                            value=current_mapping.get("related_entity_type_uri", f"{DRBER}{col_name.replace(' ', '_').lower()}Type"),
                            help="Ej: foaf:Person, schema:Organization, http://example.org/ns#MiClase",
                            key=f"related_entity_type_uri_{i}"
                        )
                        current_mapping["related_entity_type_uri"] = related_entity_type_uri

                        related_entity_id_col_options = ["-- Usar Valor de Columna Actual --"] + csv_columns_renamed
                        related_entity_id_col_default_index = 0
                        if current_mapping.get("related_entity_id_col") in csv_columns_renamed:
                            related_entity_id_col_default_index = csv_columns_renamed.index(current_mapping["related_entity_id_col"]) + 1

                        related_entity_id_col_selected = st.selectbox(
                            f"Columna para ID Único de Entidad Relacionada para `{col_name}` (opcional):",
                            options=related_entity_id_col_options,
                            index=related_entity_id_col_default_index,
                            help="Si la entidad relacionada tiene su propio ID en otra columna. Si no, se usa el valor de la columna actual.",
                            key=f"related_entity_id_col_{i}"
                        )
                        current_mapping["related_entity_id_col"] = related_entity_id_col_selected if related_entity_id_col_selected != "-- Usar Valor de Columna Actual --" else None
                        current_mapping.pop("datatype", None)
                        current_mapping["applies_to_entity"] = "main_entity (Entidad Principal de la Fila)" # Reset for object property

                is_multivalued = st.checkbox(
                    f"¿Es `{col_name}` una columna multivaluada?",
                    value=current_mapping["is_multivalued"],
                    key=f"is_multivalued_{i}"
                )
                current_mapping["is_multivalued"] = is_multivalued

                if is_multivalued:
                    delimiter = st.text_input(
                        f"Delimitador para `{col_name}`:",
                        value=current_mapping["delimiter"],
                        help="Ej: ';' para punto y coma, ',' para coma, '|' para barra vertical",
                        key=f"delimiter_{i}"
                    )
                    current_mapping["delimiter"] = delimiter
                else:
                    current_mapping["delimiter"] = ";" # Reset to default if not multivalued

                # --- NUEVO WIDGET: A qué entidad aplica esta propiedad ---
                # Determine options for 'applies_to_entity' dynamically
                current_applies_to_options = ["main_entity (Entidad Principal de la Fila)"]
                # Add columns that are currently configured as object properties
                for c_name, c_map in st.session_state.column_rdf_mappings.items():
                    if c_map.get('mapping_type') == 'object_property' and c_map.get('map') and c_name != col_name:
                        current_applies_to_options.append(c_name)
                current_applies_to_options = sorted(list(set(current_applies_to_options))) # Remove duplicates and sort

                # Ensure the current value is in the options, otherwise default to main_entity
                default_applies_to_index = 0
                if current_mapping.get("applies_to_entity") in current_applies_to_options:
                    default_applies_to_index = current_applies_to_options.index(current_mapping["applies_to_entity"])
                elif current_mapping.get("applies_to_entity") == "main_entity": # Handle the display name
                     default_applies_to_index = current_applies_to_options.index("main_entity (Entidad Principal de la Fila)")


                if current_mapping["mapping_type"] == "literal":
                    applies_to_entity_selected = st.selectbox(
                        f"Esta propiedad aplica a:",
                        options=current_applies_to_options,
                        index=default_applies_to_index,
                        help="Define si esta propiedad pertenece a la entidad principal de la fila o a una entidad relacionada creada por otra columna.",
                        key=f"applies_to_entity_{i}"
                    )
                    # Almacenar solo el nombre de la columna o "main_entity"
                    current_mapping["applies_to_entity"] = applies_to_entity_selected.split(' ')[0]
                else:
                    # Si es object_property, siempre aplica a la entidad principal
                    current_mapping["applies_to_entity"] = "main_entity"


                final_column_rdf_mappings[col_name] = current_mapping
            else:
                # Si no se mapea, asegurarse de que no esté en el diccionario final
                if col_name in final_column_rdf_mappings:
                    del final_column_rdf_mappings[col_name]

            st.session_state.column_rdf_mappings[col_name] = current_mapping # Actualizar estado para persistencia

        st.markdown("---")
        if st.button("✨ Generar Grafo RDF", key="generate_rdf_btn"):
            my_bar_tab2.progress(10, text="Validando mapeos...")

            # Validar que la URI de la entidad principal no esté vacía
            if not main_entity_type_uri.strip():
                st.error("Error: La URI de la Clase de la Entidad Principal no puede estar vacía.")
                my_bar_tab2.empty()
                st.stop()

            # Validar que se haya seleccionado una columna para el ID de la entidad principal
            if main_entity_id_col == "-- Seleccionar --":
                st.error("Error: Debes seleccionar una Columna CSV para el ID Único de la Entidad Principal.")
                my_bar_tab2.empty()
                st.stop()
            
            # Validar los mapeos de propiedades
            if not final_column_rdf_mappings:
                st.warning("Advertencia: No se ha configurado ningún mapeo de columnas a propiedades RDF. El grafo resultante estará vacío o muy limitado.")

            for col_name, mapping in final_column_rdf_mappings.items():
                if not mapping["prop_uri"].strip():
                    st.error(f"Error: La URI de la propiedad para la columna '{col_name}' no puede estar vacía.")
                    my_bar_tab2.empty()
                    st.stop()
                if mapping["mapping_type"] == "object_property" and not mapping["related_entity_type_uri"].strip():
                    st.error(f"Error: La URI de la Clase de Entidad Relacionada para la columna '{col_name}' no puede estar vacía si el tipo de mapeo es 'Entidad Relacionada'.")
                    my_bar_tab2.empty()
                    st.stop()
                
                # Advertencia si la URI no parece válida
                if not (mapping["prop_uri"].startswith("http://") or mapping["prop_uri"].startswith("https://") or ":" in mapping["prop_uri"]):
                    st.warning(f"Advertencia: La URI de la propiedad '{mapping['prop_uri']}' para la columna '{col_name}' parece no ser una URI completa o un prefijo válido (ej. `foaf:name`). Asegúrate de que es correcta.")
                if mapping["mapping_type"] == "object_property" and not (mapping["related_entity_type_uri"].startswith("http://") or mapping["related_entity_type_uri"].startswith("https://") or ":" in mapping["related_entity_type_uri"]):
                    st.warning(f"Advertencia: La URI de la Clase de Entidad Relacionada '{mapping['related_entity_type_uri']}' para la columna '{col_name}' parece no ser una URI completa o un prefijo válido. Asegúrate de que es correcta.")


            my_bar_tab2.progress(25, text="Mapeos completados. Iniciando conversión...")

            g = convertir_dataframe_a_rdf(
                df_limpio_para_rdf,
                main_entity_type_uri,
                main_entity_id_col,
                multivalued_delimiters_for_rdf, # Pasamos los delimitadores multivaluados
                final_column_rdf_mappings
            )
            my_bar_tab2.progress(75, text="Grafo RDF generado. Serializando a Turtle y RDF/XML...")

            rdf_output_ttl = g.serialize(format='turtle')
            rdf_output_xml = g.serialize(format='xml')

            my_bar_tab2.progress(100, text="¡Conversión completada!")
            st.success("¡Grafo RDF generado exitosamente!")

            st.subheader("Archivos RDF Generados:")

            st.download_button(
                label="Descargar RDF en Turtle (.ttl)",
                data=rdf_output_ttl,
                file_name="knowledge_graph.ttl",
                mime="text/turtle",
                key="download_ttl_final"
            )

            st.download_button(
                label="Descargar RDF en RDF/XML (.rdf)",
                data=rdf_output_xml,
                file_name="knowledge_graph.rdf",
                mime="application/rdf+xml",
                key="download_rdfxml_final"
            )

            st.subheader("Contenido del Grafo RDF (formato Turtle):")
            st.code(rdf_output_ttl, language='turtle')

            st.subheader("Herramientas Útiles para RDF")
            st.markdown("""
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
            st.info("Puedes copiar el contenido del grafo (arriba) y pegarlo directamente en estas herramientas, o subir los archivos que descargaste.")

    else:
        st.warning("Por favor, primero sube y prepara tu CSV en la pestaña '📊 Limpiar y Preparar CSV'.")

st.markdown("---")
st.markdown("Desarrollado con ❤️ y Streamlit.")