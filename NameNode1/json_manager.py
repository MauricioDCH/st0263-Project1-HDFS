import json
import os
from datetime import datetime
from dotenv import load_dotenv
load_dotenv(dotenv_path="./configs/.env.namenode")  # Para NameNode

def cargar_datos_desde_json(ruta_archivo_json):
    # Verifica si el archivo existe
    if not os.path.exists(ruta_archivo_json):
        # Si no existe, crea un archivo con el formato requerido
        datos_iniciales = {
                "data_nodes": [],  
                "data_nodes_lideres": [],
                "data_nodes_seguidores": [],
                "data_nodes_archivos": {}  # Mapa para archivos de cada DataNode
        }
        
        with open(ruta_archivo_json, 'w') as archivo:
            json.dump(datos_iniciales, archivo, indent=4)  # Guarda el formato inicial
        return datos_iniciales  # Retorna la estructura inicial

    # Carga los datos del archivo JSON
    with open(ruta_archivo_json, 'r') as archivo:
        try:
            datos = json.load(archivo)
        except json.JSONDecodeError:
            # Si hay un error en el formato JSON, inicializa una estructura vacía
            datos = {
                "data_nodes": [],
                "data_nodes_lideres": [],
                "data_nodes_seguidores": [],
                "data_nodes_archivos": {}  # Mapa para archivos de cada DataNode
            }
            # Guardar la estructura inicial en el archivo
            with open(ruta_archivo_json, 'w') as archivo:
                json.dump(datos, archivo, indent=4)
            return datos  # Retorna la estructura recién creada

    # Verifica y formatea los datos si no tienen el formato esperado
    if not isinstance(datos, dict):
        datos = {
            "data_nodes": [],
            "data_nodes_lideres": [],
            "data_nodes_seguidores": [],
            "data_nodes_archivos": {}
        }
    else:
        # Asegurarse de que las claves existan y tengan el formato correcto
        for key in ["data_nodes", "data_nodes_lideres", "data_nodes_seguidores", "data_nodes_archivos"]:
            if key not in datos:
                datos[key] = {}  # Inicializa como diccionario

    # Guardar el formato correcto de nuevo en el archivo
    with open(ruta_archivo_json, 'w') as archivo:
        json.dump(datos, archivo, indent=4)
    
    return datos  # Retorna los datos formateados

def guardar_datos_json(ruta_json, datos):
        """
        Guarda datos en un archivo JSON.

        Parámetros:
        datos (dict): Los datos a guardar en formato diccionario.

        Excepciones:
        Raises IOError si hay un problema al abrir o escribir en el archivo.
        Raises ValueError si los datos no son serializables a JSON.
        """
        # Verifica que los datos sean un diccionario
        if not isinstance(datos, dict):
            raise ValueError("Los datos deben ser un diccionario.")

        try:
            with open(ruta_json, 'w') as archivo:
                json.dump(datos, archivo, indent=4)  # Guarda los datos en el archivo JSON
        except IOError as e:
            print(f"Error al guardar los datos en el archivo JSON: {e}")  # Manejo de errores

def agregar_datos_localizador(nombre_archivo, ids_lideres, urls_lideres, ids_seguidores, urls_seguidores):
    """
    Agrega una nueva entrada al archivo JSON especificado. Si el archivo no existe, lo crea.
    Si existe pero no está correctamente formateado, lo reformatea.

    Args:
        nombre_archivo (str): Nombre del archivo JSON a crear o modificar.
        ids_lideres (list): Lista de IDs de los data nodes líderes.
        urls_lideres (list): Lista de URLs de los data nodes líderes.
        ids_seguidores (list): Lista de IDs de los data nodes seguidores.
        urls_seguidores (list): Lista de URLs de los data nodes seguidores.
    """
    
    # Crear una nueva entrada con la estructura deseada
    # Generar timestamp para el nuevo reporte
    timestamp_reporte = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    nueva_entrada = {
        nombre_archivo: {
            "timestamp": timestamp_reporte,
            "data_nodes_lideres": [
                {str(ids_lideres[i]): urls_lideres[i] for i in range(len(ids_lideres))}
            ],
            "data_nodes_seguidores": [
                {str(ids_seguidores[i]): urls_seguidores[i] for i in range(len(ids_seguidores))}
            ]
        }
    }
    
    ruta_bloques = os.path.join('NameNode1','database_namenode', 'localization_folder.json')

    # Verificar si el archivo ya existe
    if os.path.exists(ruta_bloques):
        try:
            # Intentar leer el contenido existente
            with open(ruta_bloques, 'r') as file:
                contenido = json.load(file)
            # Si el contenido no es una lista, reformatear el archivo
            if not isinstance(contenido, list):
                print("Archivo existente, pero no está formateado correctamente. Se formateará de nuevo.")
                contenido = []
        except json.JSONDecodeError:
            # Si el archivo no contiene un JSON válido, reformatearlo
            print("El archivo existe pero está corrupto. Se reformateará.")
            contenido = []
    else:
        # Si no existe, inicializar un objeto vacío
        contenido = []

    # Agregar la nueva entrada al contenido
    contenido.append(nueva_entrada)

    # Guardar la estructura actualizada en el archivo JSON
    with open(ruta_bloques, 'w') as file:
        json.dump(contenido, file, indent=4)

    print(f'Archivo JSON "{ruta_bloques}" actualizado con una nueva entrada.')

def get_last_entry_data_nodes(file_name):
    """
    Esta función toma el nombre de un archivo (file_name) y un archivo JSON (json_file),
    busca la última entrada basada en el timestamp del archivo especificado,
    y devuelve listas de IDs y URLs de los data nodes.
    
    Parámetros:
    - file_name: str - El nombre del archivo a buscar en el JSON (ej. "datos.txt").
    - json_file: str - El nombre del archivo JSON que contiene los datos.
    
    Retorna:
    - Una tupla que contiene:
        1. lista de ids de data_nodes_lideres
        2. lista de urls de data_nodes_lideres
        3. lista de ids de data_nodes_seguidores
        4. lista de urls de data_nodes_seguidores
    """
    json_file = os.getenv("LOCALIZATION_FOLDER_1")
    with open(json_file, 'r') as file:
        data = json.load(file)

    # Inicializar listas para los datos
    last_entry = None
    last_timestamp = None

    # Iterar sobre las entradas para encontrar la última correspondiente al file_name
    for entry in data:
        if file_name in entry:  # Comprobar si el file_name está en la entrada
            details = entry[file_name]
            current_timestamp = details['timestamp']
            if last_timestamp is None or current_timestamp > last_timestamp:
                last_timestamp = current_timestamp
                last_entry = details

    # Si no se encontró ninguna entrada
    if last_entry is None:
        return [], [], [], []

    # Extraer datos de los nodos
    ids_lideres = []
    urls_lideres = []
    ids_seguidores = []
    urls_seguidores = []

    # Extraer data_nodes_lideres
    for node in last_entry['data_nodes_lideres']:
        for node_id, url in node.items():
            ids_lideres.append(int(node_id))
            urls_lideres.append(url)

    # Extraer data_nodes_seguidores
    for node in last_entry['data_nodes_seguidores']:
        for node_id, url in node.items():
            ids_seguidores.append(int(node_id))
            urls_seguidores.append(url)

    return ids_lideres, urls_lideres, ids_seguidores, urls_seguidores

# Función para buscar rutas en nodos según la variable 'verificar_en'
def buscar_rutas_en_nodos(lista_nodos, verificar_en, nombre_archivo):
    # Ruta del archivo JSON a la base de datos NameNode
    archivo_json = os.getenv("DATABASE_PATH_NAMENODE_1")
    # Cargar el archivo JSON
    with open(archivo_json, 'r') as f:
        data = json.load(f)

    # Determinar la clave de búsqueda según 'verificar_en'
    if verificar_en == "lideres":
        clave_ruta = "lista_rutas_bloques_lider"
    elif verificar_en == "seguidores":
        clave_ruta = "lista_rutas_bloques_seguidor"
    else:
        return "Valor inválido para 'verificar_en'. Debe ser 'lideres' o 'seguidores'."

    # Inicializar un diccionario para almacenar las rutas encontradas
    resultados = {}  # Diccionario para almacenar resultados de rutas

    # Recorrer la lista de nodos para buscar las rutas
    for node_id in lista_nodos:
        rutas_encontradas = []  # Lista para almacenar rutas del nodo actual
        
        if str(node_id) in data:
            # Obtener la última entrada para ese nodo (último reporte)
            ultimo_reporte = list(data[str(node_id)].keys())[-1]  # Último timestamp
            
            # Obtener el contenido del último reporte
            contenido = data[str(node_id)][ultimo_reporte]["contenido"]
            
            # Verificar si la clave correspondiente existe en el contenido
            if clave_ruta in contenido:
                rutas = contenido[clave_ruta]
                
                # Mostrar rutas para depuración
                #print(f"Rutas encontradas para nodo {node_id} ({verificar_en}): {rutas}")
                
                # Comprobar si el nombre_archivo está en las rutas
                for ruta in rutas:
                    if nombre_archivo in ruta:  # Verificar si el nombre_archivo está presente en la ruta
                        rutas_encontradas.append(ruta)  # Almacenar rutas que contienen el nombre_archivo
                
                # Verificar si se encontraron rutas
                if not rutas_encontradas:
                    print(f"El archivo '{nombre_archivo}' no se encontró en las rutas del nodo {node_id} para {verificar_en}.")
            else:
                print(f"No se encontró '{clave_ruta}' para el nodo {node_id}.\n")
        else:
            print(f"Nodo ID {node_id} no encontrado en el archivo JSON.\n")
        
        # Agregar las rutas encontradas al diccionario de resultados
        resultados[node_id] = rutas_encontradas  # Usar node_id como clave en el diccionario
    
    return resultados