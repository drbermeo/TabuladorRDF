from rdflib import Graph, Literal, Namespace, URIRef # Importa clases necesarias de RDFLib para construir grafos RDF:
                                                    # - Graph: Para crear y manipular el grafo RDF.
                                                    # - Literal: Para representar valores de datos (cadenas, números, fechas).
                                                    # - Namespace: Para definir espacios de nombres (prefijos URI).
                                                    # - URIRef: Para crear referencias URI para recursos y propiedades.
from rdflib.namespace import RDF, RDFS, XSD, FOAF, OWL # Importa namespaces predefinidos de RDFLib para ontologías comunes:
                                                    # - RDF: Resource Description Framework (tipos básicos de RDF).
                                                    # - RDFS: RDF Schema (para clases y propiedades de esquema).
                                                    # - XSD: XML Schema Datatypes (para tipos de datos como string, integer, date).
                                                    # - FOAF: Friend of a Friend (para personas, organizaciones).
                                                    # - OWL: Web Ontology Language (para clases de ontología como owl:Class).
import pandas as pd # Importa la biblioteca Pandas, esencial para trabajar con DataFrames.
import re # Importa el módulo de expresiones regulares para operaciones de limpieza de texto.

# --- Definición de Namespaces (Espacios de Nombres) ---
# Se definen los URIs base para diferentes vocabularios y ontologías que se usarán en el grafo RDF.
# Esto ayuda a organizar y dar significado a los datos, y a generar URIs concisas.
BIBO = Namespace("http://purl.org/ontology/bibo/") # Namespace para la ontología Bibliographic Ontology (bibo),
                                                    # útil para tipos de documentos, artículos, etc.
SCHEMA = Namespace("http://schema.org/") # Namespace para Schema.org, un vocabulario amplio para describir
                                        # entidades web, como nombres, descripciones, URLs.
DCT = Namespace("http://purl.org/dc/terms/") # Namespace para Dublin Core Terms (dcterms),
                                            # para propiedades de metadatos como título, creador, fecha, identificador.
FOAF = Namespace("http://xmlns.com/foaf/0.1/") # Namespace para Friend of a Friend (FOAF),
                                            # para describir personas y organizaciones.
XSD = Namespace("http://www.w3.org/2001/XMLSchema#") # Namespace para XML Schema Datatypes (XSD),
                                                    # para especificar tipos de datos de literales (string, integer).
RDFS = Namespace("http://www.w3.org/2000/01/rdf-schema#") # Namespace para RDF Schema (RDFS).
DRBER = Namespace("http://drber.example.org/ns#") # Namespace personalizado para este proyecto (drber).
                                                # Se usará para propiedades y entidades específicas generadas.
PROV = Namespace("http://www.w3.org/ns/prov#") # Namespace para la ontología PROV-O, para describir la procedencia
                                                # de los datos (opcional, no se usa extensivamente en este ejemplo).

# --- Función Auxiliar para Limpiar Segmentos de URI ---
def clean_uri_segment(text):
    """
    Limpia una cadena de texto para que pueda ser utilizada de forma segura y consistente
    como un segmento de una URI (Uniform Resource Identifier).
    Esto es crucial para generar URIs válidas y legibles en el grafo RDF.
    Reemplaza caracteres especiales y espacios con guiones bajos y elimina caracteres no permitidos.

    Args:
        text (str): La cadena de texto a limpiar.

    Returns:
        str: La cadena de texto limpia, apta para ser parte de una URI.
    """
    if not isinstance(text, str): # Verifica si la entrada no es una cadena de texto.
        return str(text) # Si no es una cadena, la convierte a cadena.
    
    # Reemplaza cualquier secuencia de uno o más caracteres que no sean alfanuméricos,
    # puntos (.), o guiones (-) con un solo guion bajo (_).
    # Esto maneja espacios, barras, caracteres especiales, etc., convirtiéndolos en '_'.
    cleaned_text = re.sub(r'[^\w.-]+', '_', text)
    
    # Elimina cualquier guion bajo o guion al principio o al final de la cadena.
    cleaned_text = cleaned_text.strip('_')
    cleaned_text = cleaned_text.strip('-')

    if not cleaned_text: # Si después de la limpieza la cadena queda vacía...
        return "unknown" # ...devuelve "unknown" como un valor de respaldo para la URI.
    return cleaned_text # Devuelve la cadena limpia.

# --- Función Principal de Conversión de DataFrame a RDF ---
def convertir_dataframe_a_rdf(
    df_limpio, # El DataFrame de Pandas ya limpio y preprocesado.
    main_entity_type_uri, # La URI de la clase para la entidad principal que representa cada fila del DataFrame.
    main_entity_id_col, # El nombre de la columna en df_limpio que contiene el identificador único para la entidad principal.
    multivalued_delimiters_config, # Una lista de diccionarios que especifica las columnas multivaluadas
                                  # y sus respectivos delimitadores internos (ej. [{'column': 'Autores', 'delimiter': ';'}])
    column_rdf_mappings # Un diccionario que define cómo cada columna del DataFrame debe ser mapeada a RDF.
                        # Incluye detalles como la URI de la propiedad, el tipo de mapeo (literal o entidad relacionada),
                        # el tipo de dato XSD, si es multivaluada, y a qué entidad aplica la propiedad ('main_entity' o una columna que genera una entidad relacionada).
):
    """
    Convierte un DataFrame de Pandas en un grafo de conocimiento RDF.
    La conversión es completamente dinámica y se basa en un mapeo detallado
    de columnas a propiedades y entidades RDF, proporcionado externamente.

    Args:
        df_limpio (pd.DataFrame): DataFrame con los datos limpios y columnas renombradas.
        main_entity_type_uri (str): URI de la clase para la entidad principal de cada fila.
        main_entity_id_col (str): Nombre de la columna en df_limpio que se usará para
                                  generar URIs únicas para la entidad principal.
        multivalued_delimiters_config (list): Lista de diccionarios que especifican
                                              el delimitador para columnas multivaluadas.
                                              Cada dict: {'column': 'nombre_columna', 'delimiter': 'caracter_delimitador'}.
        column_rdf_mappings (dict): Un diccionario donde las claves son los nombres de las
                                    columnas del DataFrame (ya renombradas) y los valores
                                    son diccionarios que describen cómo mapear esa columna a RDF.
                                    Ahora incluye 'applies_to_entity'.

    Returns:
        rdflib.Graph: El grafo RDF generado, listo para ser serializado.
    """
    g = Graph() # Inicializa un nuevo grafo RDF vacío.

    # --- Enlace de Namespaces al Grafo ---
    # Vincula los prefijos de namespace al grafo para que las URIs puedan ser serializadas de forma concisa (ej. drber:articulo).
    g.bind("bibo", BIBO)
    g.bind("schema", SCHEMA)
    g.bind("dct", DCT)
    g.bind("foaf", FOAF)
    g.bind("xsd", XSD)
    g.bind("rdfs", RDFS)
    g.bind("drber", DRBER)
    g.bind("prov", PROV)

    # Caché global para evitar duplicidad de URIs de entidades relacionadas.
    # Esto es crucial para la deduplicación de entidades como autores o instituciones.
    # La clave es el URI de la clase de entidad relacionada (ej. foaf:Person),
    # el valor es un diccionario donde la clave es el ID del valor (ej. "Cabezas-Terán_K")
    # y el valor es la URIRef ya creada para esa entidad.
    global_entity_uris_cache = {}

    main_entity_type_ref = URIRef(main_entity_type_uri) # Convierte la URI de la clase principal a un objeto URIRef.
    g.add((main_entity_type_ref, RDF.type, OWL.Class)) # Declara explícitamente la clase principal como una clase OWL en el grafo.

    # Crea un diccionario para un acceso rápido a los delimitadores multivaluados por nombre de columna.
    # Esto optimiza la búsqueda del delimitador para cada columna.
    multivalued_delimiters_dict = {m['column']: m['delimiter'] for m in multivalued_delimiters_config}

    # --- Iterar sobre cada fila del DataFrame ---
    # Cada fila del DataFrame se convierte en una instancia de la entidad principal en el grafo RDF.
    for index, row in df_limpio.iterrows():
        # Caché local para entidades creadas en la fila actual.
        # Esto permite que las propiedades de una entidad relacionada (ej. nombre de un autor)
        # se añadan al nodo del autor que se creó a partir de otra columna en la misma fila.
        # Formato: {nombre_columna_que_genero_entidad: [uri_entidad1, uri_entidad2, ...]}
        current_row_entities_cache = {}

        # --- 1. Creación del Recurso Principal para la Fila ---
        # Obtiene el valor de la columna que se usará como ID único para la entidad principal de la fila.
        main_id_value = row.get(main_entity_id_col)
        # Si el valor del ID es nulo o vacío, se usa un ID generado basado en el índice de la fila.
        if pd.isna(main_id_value) or str(main_id_value).strip() == "":
            main_id_value = f"record_{index}"

        # Limpia el valor del ID para usarlo como un segmento seguro de la URI.
        main_entity_id_segment = clean_uri_segment(str(main_id_value))
        # Construye la URI completa para la entidad principal de la fila.
        # Ej: http://drber.example.org/ns#articulo/2-s2_0-105005256894
        main_entity_uri = URIRef(f"{DRBER}{clean_uri_segment(main_entity_type_uri.split('#')[-1].lower())}/{main_entity_id_segment}")

        # Añade los triples básicos para la entidad principal al grafo:
        # - La entidad es de un tipo específico (ej. drber:articulo).
        # - La entidad tiene una etiqueta legible (rdfs:label).
        g.add((main_entity_uri, RDF.type, main_entity_type_ref))
        g.add((main_entity_uri, RDFS.label, Literal(str(main_id_value))))

        # --- 2. Procesar Mapeos de Columnas a RDF ---
        # Se dividen los mapeos de columnas en dos categorías:
        # a) Propiedades que aplican directamente a la entidad principal de la fila.
        # b) Propiedades que aplican a entidades relacionadas (creadas por otras columnas en la misma fila).
        main_entity_property_mappings = []
        related_entity_property_mappings = []

        for col_name, mapping_config in column_rdf_mappings.items():
            # Determina si la propiedad aplica a la entidad principal o a una entidad relacionada.
            applies_to_entity = mapping_config.get("applies_to_entity", "main_entity")
            if applies_to_entity == "main_entity":
                main_entity_property_mappings.append((col_name, mapping_config))
            else:
                related_entity_property_mappings.append((col_name, mapping_config))

        # Procesar primero las propiedades de la entidad principal.
        # Esto es importante para que las entidades relacionadas (si se crean)
        # estén disponibles en el caché local antes de que otras propiedades intenten aplicarse a ellas.
        for col_name, mapping_config in main_entity_property_mappings:
            # Si la columna no existe en la fila o su valor es nulo, se salta.
            if col_name not in row or pd.isna(row[col_name]):
                continue

            prop_uri = URIRef(mapping_config["prop_uri"]) # URI de la propiedad RDF (ej. dct:creator).
            mapping_type = mapping_config["mapping_type"] # Tipo de mapeo: "literal" o "object_property".
            is_multivalued_col = mapping_config.get("is_multivalued", False) # Indica si la columna es multivaluada.
            delimiter_col = mapping_config.get("delimiter", ";") # Delimitador para valores multivaluados.

            raw_value = str(row[col_name]) # Obtiene el valor de la celda como cadena.
            # Divide el valor en una lista si es multivaluado, o lo mantiene como una lista de un solo elemento.
            values_to_process = [v.strip() for v in raw_value.split(delimiter_col) if v.strip()] if is_multivalued_col else [raw_value]

            # Caché temporal para las URIs de entidades relacionadas generadas por ESTA columna en ESTA fila.
            generated_related_uris_for_col = []

            # --- Lógica para manejar identificadores compuestos como "Nombre___Código" ---
            # Esta sección intenta mejorar la deduplicación y la creación de URIs para
            # entidades relacionadas cuando sus nombres pueden contener sub-identificadores.
            final_values_for_entity_creation = []
            if mapping_type == "object_property": # Esta lógica solo aplica a propiedades de objeto.
                # Ordena los valores por longitud descendente para procesar los más largos (y potencialmente más específicos) primero.
                values_to_process.sort(key=len, reverse=True)
                
                processed_sub_values = set() # Un conjunto para rastrear los valores limpios ya procesados.
                for val in values_to_process:
                    cleaned_val = clean_uri_segment(val) # Limpia el valor para comparación.
                    is_redundant = False # Bandera para indicar si el valor actual es redundante.
                    for existing_val in processed_sub_values:
                        # Si el valor limpio actual empieza con un valor ya procesado (y no es idéntico),
                        # y el valor actual es más específico (más largo), se considera una mejora.
                        if cleaned_val.startswith(existing_val) and cleaned_val != existing_val:
                            processed_sub_values.remove(existing_val) # Elimina el valor menos específico.
                            processed_sub_values.add(cleaned_val) # Añade el valor más específico.
                            is_redundant = False # No es redundante, es una versión mejorada.
                            break
                        # Si un valor ya existente es más específico que el actual, el actual es redundante.
                        elif existing_val.startswith(cleaned_val) and existing_val != cleaned_val:
                            is_redundant = True
                            break
                    
                    if not is_redundant: # Si el valor no es redundante, se añade a la lista final.
                        final_values_for_entity_creation.append(val)
                        processed_sub_values.add(cleaned_val) # Se marca como procesado.
                
                # Los valores filtrados se usan para la creación de entidades.
                values_to_process = final_values_for_entity_creation
                # Nota: El reordenamiento para mantener el orden original es más complejo
                # si hay solapamientos y se omite por simplicidad aquí.

            # Itera sobre cada valor (ya sea el valor original o los valores divididos/filtrados).
            for i, value in enumerate(values_to_process):
                if mapping_type == "literal": # Si el mapeo es a un literal (valor directo).
                    datatype_uri = mapping_config.get("datatype", str(XSD.string)) # Obtiene el tipo de dato XSD (por defecto string).
                    literal_value = Literal(value, datatype=URIRef(datatype_uri)) # Crea un objeto Literal con el valor y tipo de dato.
                    g.add((main_entity_uri, prop_uri, literal_value)) # Añade el triple al grafo.

                elif mapping_type == "object_property": # Si el mapeo es a una entidad relacionada.
                    related_entity_type_uri_str = mapping_config.get("related_entity_type_uri") # URI de la clase de la entidad relacionada (ej. foaf:Person).
                    related_entity_id_col = mapping_config.get("related_entity_id_col") # Columna opcional para un ID único de la entidad relacionada.

                    if not related_entity_type_uri_str: # Si no se especificó un tipo de entidad relacionada, usa uno genérico.
                        related_entity_type_uri_str = f"{DRBER}RelatedEntity"

                    related_entity_type_ref = URIRef(related_entity_type_uri_str) # Convierte la URI del tipo de entidad relacionada a URIRef.
                    g.add((related_entity_type_ref, RDF.type, OWL.Class)) # Declara la clase de la entidad relacionada en el grafo.

                    related_id_value_for_uri = value # Por defecto, el valor actual se usa para el ID de la URI.

                    # Si se especificó una columna de ID única para la entidad relacionada y es diferente a la columna actual.
                    if related_entity_id_col and related_entity_id_col != col_name:
                        # Si la columna de ID existe en la fila y no es nula.
                        if related_entity_id_col in row and pd.notna(row[related_entity_id_col]):
                            raw_id_col_value = str(row[related_entity_id_col]) # Obtiene el valor de la columna de ID.
                            
                            # Verifica si la columna de ID también es multivaluada.
                            is_related_id_col_multivalued = related_entity_id_col in multivalued_delimiters_dict
                            related_id_col_delimiter = multivalued_delimiters_dict.get(related_entity_id_col, ";")

                            if is_related_id_col_multivalued: # Si la columna de ID es multivaluada...
                                split_related_id_values = [v.strip() for v in raw_id_col_value.split(related_id_col_delimiter) if v.strip()]
                                # Intenta usar el ID correspondiente de la lista de IDs divididos.
                                if i < len(split_related_id_values) and split_related_id_values[i]:
                                    related_id_value_for_uri = split_related_id_values[i]
                                else: # Si no hay un ID correspondiente, usa el valor de la columna actual.
                                    related_id_value_for_uri = value
                            else: # Si la columna de ID no es multivaluada, usa su valor directamente.
                                related_id_value_for_uri = raw_id_col_value
                        else: # Si la columna de ID no existe o es nula, usa el valor de la columna actual.
                            related_id_value_for_uri = value
                    
                    # Limpia el valor que se usará como ID para construir el segmento de la URI.
                    related_entity_id_segment = clean_uri_segment(str(related_id_value_for_uri))
                    # Construye la URI completa para la entidad relacionada.
                    related_entity_uri = URIRef(f"{DRBER}{clean_uri_segment(related_entity_type_uri_str.split('#')[-1].lower())}/{related_entity_id_segment}")

                    # Deduplicación de entidades relacionadas usando el caché global.
                    if related_entity_type_uri_str not in global_entity_uris_cache:
                        global_entity_uris_cache[related_entity_type_uri_str] = {} # Inicializa el caché para este tipo de entidad.
                    # Si la URI de la entidad relacionada no está en el caché global...
                    if related_entity_uri not in global_entity_uris_cache[related_entity_type_uri_str]:
                        g.add((related_entity_uri, RDF.type, related_entity_type_ref)) # Añade el tipo de la entidad.
                        g.add((related_entity_uri, RDFS.label, Literal(str(related_id_value_for_uri)))) # Añade una etiqueta.
                        global_entity_uris_cache[related_entity_type_uri_str][related_entity_uri] = True # Marca como creada.
                    
                    g.add((main_entity_uri, prop_uri, related_entity_uri)) # Añade el triple que vincula la entidad principal a la relacionada.
                    generated_related_uris_for_col.append(related_entity_uri) # Almacena la URI de la entidad relacionada en el caché local de la fila.
            
            # Almacena las URIs de las entidades relacionadas generadas por esta columna
            # en el caché local de la fila para su uso posterior por otras propiedades.
            if generated_related_uris_for_col:
                current_row_entities_cache[col_name] = generated_related_uris_for_col

        # Después de procesar todas las propiedades que aplican a la entidad principal,
        # se procesan las propiedades que aplican a las entidades relacionadas (ej. nombre completo del autor).
        for col_name, mapping_config in related_entity_property_mappings:
            applies_to_col_name = mapping_config["applies_to_entity"] # Nombre de la columna que generó la entidad a la que aplica esta propiedad.
            
            # Solo se procesa si la entidad a la que aplica fue creada en esta misma fila.
            if applies_to_col_name in current_row_entities_cache:
                prop_uri = URIRef(mapping_config["prop_uri"]) # URI de la propiedad RDF (ej. foaf:name).
                mapping_type = mapping_config["mapping_type"] # Tipo de mapeo (debería ser 'literal' para estas propiedades).
                is_multivalued_col = mapping_config.get("is_multivalued", False) # Indica si la columna es multivaluada.
                delimiter_col = mapping_config.get("delimiter", ";") # Delimitador para valores multivaluados.

                raw_value = str(row[col_name]) # Obtiene el valor de la celda.
                values_to_process = [v.strip() for v in raw_value.split(delimiter_col) if v.strip()] if is_multivalued_col else [raw_value]

                target_entities = current_row_entities_cache[applies_to_col_name] # Obtiene las URIs de las entidades a las que aplicar la propiedad.

                # Lógica para manejar la correspondencia entre valores de la propiedad y entidades objetivo.
                # Si el número de valores no coincide con el número de entidades y hay valores para procesar...
                if len(values_to_process) != len(target_entities) and len(values_to_process) > 0:
                    # Si la columna de la propiedad tiene un solo valor, se aplica a todas las entidades relacionadas.
                    if len(values_to_process) == 1:
                        for target_entity_uri in target_entities:
                            datatype_uri = mapping_config.get("datatype", str(XSD.string))
                            literal_value = Literal(values_to_process[0], datatype=URIRef(datatype_uri))
                            g.add((target_entity_uri, prop_uri, literal_value))
                            # Heurística adicional: Si el valor parece un código (alfanuméricos, 5+ caracteres),
                            # se añade también como una propiedad drber:code.
                            if re.match(r'^[A-Z0-9]{5,}$', values_to_process[0]):
                                g.add((target_entity_uri, DRBER.code, Literal(values_to_process[0])))
                    else:
                        # Si hay un desajuste y más de un valor, se itera hasta el mínimo de elementos.
                        for i, target_entity_uri in enumerate(target_entities):
                            if i < len(values_to_process):
                                datatype_uri = mapping_config.get("datatype", str(XSD.string))
                                literal_value = Literal(values_to_process[i], datatype=URIRef(datatype_uri))
                                g.add((target_entity_uri, prop_uri, literal_value))
                                # Heurística adicional para códigos.
                                if re.match(r'^[A-Z0-9]{5,}$', values_to_process[i]):
                                    g.add((target_entity_uri, DRBER.code, Literal(values_to_process[i])))
                else: # Si hay una correspondencia 1:1 o ambas listas están vacías.
                    for i, target_entity_uri in enumerate(target_entities):
                        if i < len(values_to_process) and values_to_process[i]: # Asegura que el valor exista.
                            datatype_uri = mapping_config.get("datatype", str(XSD.string))
                            literal_value = Literal(values_to_process[i], datatype=URIRef(datatype_uri))
                            g.add((target_entity_uri, prop_uri, literal_value))
                            # Heurística adicional para códigos.
                            if re.match(r'^[A-Z0-9]{5,}$', values_to_process[i]):
                                g.add((target_entity_uri, DRBER.code, Literal(values_to_process[i])))

    return g # Devuelve el grafo RDF completo.