# convertir_a_rdf.py
# Este script es el corazón de la conversión de datos.
# Toma un DataFrame limpio y estandarizado y lo transforma en un grafo de conocimiento RDF.
# Es completamente genérico y construye el grafo basándose en un mapeo de columnas
# proporcionado dinámicamente, sin asumir un modelo de datos específico.

from rdflib import Graph, Literal, Namespace, URIRef
from rdflib.namespace import RDF, RDFS, XSD, FOAF, OWL
import pandas as pd
import re

# --- Definición de Namespaces (Espacios de Nombres) ---
BIBO = Namespace("http://purl.org/ontology/bibo/") # Para tipos de documentos, etc. (se usará si se mapea)
SCHEMA = Namespace("http://schema.org/") # Para propiedades generales como name, description, url
DCT = Namespace("http://purl.org/dc/terms/") # Para propiedades de metadatos como title, creator, date, identifier
FOAF = Namespace("http://xmlns.com/foaf/0.1/") # Para personas y organizaciones
XSD = Namespace("http://www.w3.org/2001/XMLSchema#") # Para tipos de datos
RDFS = Namespace("http://www.w3.org/2000/01/rdf-schema#")
DRBER = Namespace("http://drber.example.org/ns#") # Namespace por defecto para propiedades/entidades generadas
PROV = Namespace("http://www.w3.org/ns/prov#") # Para procedencia (opcional, no usado en este ejemplo simplificado)

# --- Función Auxiliar para Limpiar Segmentos de URI ---
def clean_uri_segment(text):
    """
    Limpia una cadena de texto para que pueda ser utilizada de forma segura
    como un segmento de una URI (Uniform Resource Identifier).
    Reemplaza caracteres especiales y espacios con guiones bajos y elimina caracteres no permitidos.
    Esto es fundamental para generar URIs válidas y consistentes en el grafo RDF.
    """
    if not isinstance(text, str):
        return str(text)
    
    # Reemplazar cualquier secuencia de caracteres no alfanuméricos (excepto . y -) con un solo guion bajo
    # Esto manejará '___' o ' / ' convirtiéndolos en '_'
    cleaned_text = re.sub(r'[^\w.-]+', '_', text)
    
    # Eliminar guiones bajos o guiones al inicio/final
    cleaned_text = cleaned_text.strip('_')
    cleaned_text = cleaned_text.strip('-')

    if not cleaned_text:
        return "unknown" # Fallback si después de limpiar queda vacío
    return cleaned_text

# --- Función Principal de Conversión de DataFrame a RDF ---
def convertir_dataframe_a_rdf(
    df_limpio,
    main_entity_type_uri,
    main_entity_id_col,
    multivalued_delimiters_config, # Añadido este parámetro para acceder a los delimitadores
    column_rdf_mappings
):
    """
    Convierte un DataFrame de Pandas en un grafo de conocimiento RDF.
    La conversión es completamente dinámica, basándose en un mapeo detallado
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
    g = Graph()

    # --- Enlace de Namespaces al Grafo ---
    g.bind("bibo", BIBO)
    g.bind("schema", SCHEMA)
    g.bind("dct", DCT)
    g.bind("foaf", FOAF)
    g.bind("xsd", XSD)
    g.bind("rdfs", RDFS)
    g.bind("drber", DRBER)
    g.bind("prov", PROV)

    # Caché global para evitar duplicidad de URIs de entidades relacionadas
    # La clave es el URI de la clase de entidad relacionada, el valor es un dict de {ID_valor: URIRef}
    global_entity_uris_cache = {}

    main_entity_type_ref = URIRef(main_entity_type_uri)
    g.add((main_entity_type_ref, RDF.type, OWL.Class)) # Declarar la clase principal en la ontología

    # Crear un diccionario para un acceso rápido a los delimitadores multivaluados por nombre de columna
    multivalued_delimiters_dict = {m['column']: m['delimiter'] for m in multivalued_delimiters_config}

    # --- Iterar sobre cada fila del DataFrame ---
    for index, row in df_limpio.iterrows():
        # Caché local para entidades creadas en la fila actual (para propiedades que aplican a ellas)
        current_row_entities_cache = {} # {col_name_que_genero_entidad: [uri_entidad1, uri_entidad2, ...]}

        # --- 1. Creación del Recurso Principal para la Fila ---
        main_id_value = row.get(main_entity_id_col)
        if pd.isna(main_id_value) or str(main_id_value).strip() == "":
            main_id_value = f"record_{index}"

        main_entity_id_segment = clean_uri_segment(str(main_id_value))
        main_entity_uri = URIRef(f"{DRBER}{clean_uri_segment(main_entity_type_uri.split('#')[-1].lower())}/{main_entity_id_segment}")

        g.add((main_entity_uri, RDF.type, main_entity_type_ref))
        g.add((main_entity_uri, RDFS.label, Literal(str(main_id_value))))

        # --- 2. Procesar Mapeos de Columnas a RDF ---
        # Primero, procesar las columnas que generan entidades relacionadas
        # para que estén disponibles para otras propiedades.
        
        # Separar mapeos para la entidad principal y para entidades relacionadas
        main_entity_property_mappings = []
        related_entity_property_mappings = []

        for col_name, mapping_config in column_rdf_mappings.items():
            applies_to_entity = mapping_config.get("applies_to_entity", "main_entity")
            if applies_to_entity == "main_entity":
                main_entity_property_mappings.append((col_name, mapping_config))
            else:
                related_entity_property_mappings.append((col_name, mapping_config))

        # Procesar primero las propiedades de la entidad principal, especialmente las de objeto
        for col_name, mapping_config in main_entity_property_mappings:
            if col_name not in row or pd.isna(row[col_name]):
                continue

            prop_uri = URIRef(mapping_config["prop_uri"])
            mapping_type = mapping_config["mapping_type"]
            is_multivalued_col = mapping_config.get("is_multivalued", False)
            delimiter_col = mapping_config.get("delimiter", ";")

            raw_value = str(row[col_name])
            values_to_process = [v.strip() for v in raw_value.split(delimiter_col) if v.strip()] if is_multivalued_col else [raw_value]

            # Cache para las URIs de entidades relacionadas generadas por esta columna en esta fila
            generated_related_uris_for_col = []

            # --- Lógica para manejar identificadores compuestos como "Nombre___Código" ---
            # Identificar si hay valores que son prefijos de otros valores en la misma lista
            # Esto ayuda a evitar duplicados y a priorizar el identificador más completo.
            final_values_for_entity_creation = []
            if mapping_type == "object_property":
                # Ordenar por longitud descendente para procesar los más largos primero
                values_to_process.sort(key=len, reverse=True)
                
                processed_sub_values = set()
                for val in values_to_process:
                    cleaned_val = clean_uri_segment(val)
                    # Si el valor actual no es un prefijo de algo ya procesado (más largo)
                    # o si no es un sufijo de algo ya procesado (más corto)
                    is_redundant = False
                    for existing_val in processed_sub_values:
                        if cleaned_val.startswith(existing_val) and cleaned_val != existing_val:
                            # 'val' es más específico que 'existing_val', así que lo reemplazamos
                            processed_sub_values.remove(existing_val)
                            processed_sub_values.add(cleaned_val)
                            is_redundant = False # No es redundante, es una versión mejorada
                            break
                        elif existing_val.startswith(cleaned_val) and existing_val != cleaned_val:
                            # 'existing_val' es más específico que 'val', así que 'val' es redundante
                            is_redundant = True
                            break
                    
                    if not is_redundant:
                        final_values_for_entity_creation.append(val)
                        processed_sub_values.add(cleaned_val)
                
                # Usar los valores filtrados para la creación de entidades
                values_to_process = final_values_for_entity_creation
                # Reordenar para mantener el orden original si es importante (opcional)
                # values_to_process.sort(key=lambda x: raw_value.find(x)) # Esto es más complejo si hay solapamientos

            for i, value in enumerate(values_to_process):
                if mapping_type == "literal":
                    datatype_uri = mapping_config.get("datatype", str(XSD.string))
                    literal_value = Literal(value, datatype=URIRef(datatype_uri))
                    g.add((main_entity_uri, prop_uri, literal_value))

                elif mapping_type == "object_property":
                    related_entity_type_uri_str = mapping_config.get("related_entity_type_uri")
                    related_entity_id_col = mapping_config.get("related_entity_id_col")

                    if not related_entity_type_uri_str:
                        related_entity_type_uri_str = f"{DRBER}RelatedEntity"

                    related_entity_type_ref = URIRef(related_entity_type_uri_str)
                    g.add((related_entity_type_ref, RDF.type, OWL.Class))

                    related_id_value_for_uri = value # Default to the individual value from the current column

                    if related_entity_id_col and related_entity_id_col != col_name:
                        if related_entity_id_col in row and pd.notna(row[related_entity_id_col]):
                            raw_id_col_value = str(row[related_entity_id_col])
                            
                            is_related_id_col_multivalued = related_entity_id_col in multivalued_delimiters_dict
                            related_id_col_delimiter = multivalued_delimiters_dict.get(related_entity_id_col, ";")

                            if is_related_id_col_multivalued:
                                split_related_id_values = [v.strip() for v in raw_id_col_value.split(related_id_col_delimiter) if v.strip()]
                                if i < len(split_related_id_values) and split_related_id_values[i]:
                                    related_id_value_for_uri = split_related_id_values[i]
                                else:
                                    related_id_value_for_uri = value
                            else:
                                related_id_value_for_uri = raw_id_col_value
                        else:
                            related_id_value_for_uri = value
                    
                    related_entity_id_segment = clean_uri_segment(str(related_id_value_for_uri))
                    related_entity_uri = URIRef(f"{DRBER}{clean_uri_segment(related_entity_type_uri_str.split('#')[-1].lower())}/{related_entity_id_segment}")

                    if related_entity_type_uri_str not in global_entity_uris_cache:
                        global_entity_uris_cache[related_entity_type_uri_str] = {}
                    if related_entity_uri not in global_entity_uris_cache[related_entity_type_uri_str]:
                        g.add((related_entity_uri, RDF.type, related_entity_type_ref))
                        g.add((related_entity_uri, RDFS.label, Literal(str(related_id_value_for_uri))))
                        global_entity_uris_cache[related_entity_type_uri_str][related_entity_uri] = True
                    
                    g.add((main_entity_uri, prop_uri, related_entity_uri))
                    generated_related_uris_for_col.append(related_entity_uri)
            
            # Almacenar las URIs generadas por esta columna para su uso posterior
            if generated_related_uris_for_col:
                current_row_entities_cache[col_name] = generated_related_uris_for_col

        # Después de procesar todas las propiedades de la entidad principal,
        # procesar las propiedades que aplican a entidades relacionadas.
        for col_name, mapping_config in related_entity_property_mappings:
            applies_to_col_name = mapping_config["applies_to_entity"]
            
            # Solo procesar si la entidad a la que aplica fue creada en esta fila
            if applies_to_col_name in current_row_entities_cache:
                prop_uri = URIRef(mapping_config["prop_uri"])
                mapping_type = mapping_config["mapping_type"] # Debería ser 'literal' para estas propiedades
                is_multivalued_col = mapping_config.get("is_multivalued", False)
                delimiter_col = mapping_config.get("delimiter", ";")

                raw_value = str(row[col_name])
                values_to_process = [v.strip() for v in raw_value.split(delimiter_col) if v.strip()] if is_multivalued_col else [raw_value]

                target_entities = current_row_entities_cache[applies_to_col_name]

                # Asegurarse de que el número de valores coincida con el número de entidades relacionadas
                # Si no coinciden, aplicar el primer valor a todas o iterar de forma más compleja.
                # Por simplicidad, si no hay correspondencia 1:1, aplicamos el primer valor a todas las entidades.
                if len(values_to_process) != len(target_entities) and len(values_to_process) > 0:
                    # Si la columna de la propiedad tiene un solo valor, aplicarlo a todas las entidades relacionadas.
                    if len(values_to_process) == 1:
                        for target_entity_uri in target_entities:
                            datatype_uri = mapping_config.get("datatype", str(XSD.string))
                            literal_value = Literal(values_to_process[0], datatype=URIRef(datatype_uri))
                            g.add((target_entity_uri, prop_uri, literal_value))
                            # Heurística: Si el valor de la propiedad es un código, añadirlo como drber:code
                            if re.match(r'^[A-Z0-9]{5,}$', values_to_process[0]): # Patrón simple para códigos
                                g.add((target_entity_uri, DRBER.code, Literal(values_to_process[0])))
                    else:
                        # Si hay un desajuste y más de un valor, iterar hasta el mínimo
                        for i, target_entity_uri in enumerate(target_entities):
                            if i < len(values_to_process):
                                datatype_uri = mapping_config.get("datatype", str(XSD.string))
                                literal_value = Literal(values_to_process[i], datatype=URIRef(datatype_uri))
                                g.add((target_entity_uri, prop_uri, literal_value))
                                # Heurística: Si el valor de la propiedad es un código, añadirlo como drber:code
                                if re.match(r'^[A-Z0-9]{5,}$', values_to_process[i]): # Patrón simple para códigos
                                    g.add((target_entity_uri, DRBER.code, Literal(values_to_process[i])))
                else: # Correspondencia 1:1 o ambos vacíos
                    for i, target_entity_uri in enumerate(target_entities):
                        if i < len(values_to_process) and values_to_process[i]:
                            datatype_uri = mapping_config.get("datatype", str(XSD.string))
                            literal_value = Literal(values_to_process[i], datatype=URIRef(datatype_uri))
                            g.add((target_entity_uri, prop_uri, literal_value))
                            # Heurística: Si el valor de la propiedad es un código, añadirlo como drber:code
                            if re.match(r'^[A-Z0-9]{5,}$', values_to_process[i]): # Patrón simple para códigos
                                g.add((target_entity_uri, DRBER.code, Literal(values_to_process[i])))

    return g