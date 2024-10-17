import os
import json
import time

class GestorArchivos:
    
    def __init__(self, carpeta_base, archivo_salida):
        self.carpeta_base = carpeta_base
        self.archivo_salida = archivo_salida

    # Función para obtener los metadatos de los archivos y carpetas
    def obtener_metadata_archivos(self, carpeta):
        metadatos = []
        # Recorrer todos los elementos en la carpeta
        for nombre_archivo in os.listdir(carpeta):
            ruta_completa = os.path.join(carpeta, nombre_archivo)

            # Obtener metadatos si es un archivo
            if os.path.isfile(ruta_completa):
                info = os.stat(ruta_completa)
                metadatos.append({
                    "nombre": nombre_archivo,
                    "tipo": "archivo",
                    "tamano-bytes": info.st_size,  # Tamaño en bytes
                    "fecha_modificacion": time.ctime(info.st_mtime),  # Fecha de modificación
                    "usuario": os.getlogin(),  # Usuario que ejecuta el script
                })

            # Obtener metadatos si es un directorio
            elif os.path.isdir(ruta_completa):
                info = os.stat(ruta_completa)
                cantidad_archivos = len(os.listdir(ruta_completa))  # Contar archivos y carpetas dentro
                metadatos.append({
                    "nombre": nombre_archivo,
                    "tipo": "carpeta",
                    "tamano-bytes": None,  # Directorios no tienen tamaño en bytes
                    "fecha_modificacion": time.ctime(info.st_mtime),  # Fecha de modificación
                    "usuario": os.getlogin(),  # Usuario que ejecuta el script
                    "cantidad_archivos": cantidad_archivos,  # Contar elementos dentro de la carpeta
                    "subcarpetas-archivos-internos": self.obtener_metadata_archivos(ruta_completa)  # Llamada recursiva para subcarpetas
                })

        return metadatos

    # Función para guardar los metadatos en un archivo JSON
    def guardar_metadata_en_json(self):
        metadatos = self.obtener_metadata_archivos(self.carpeta_base)
        with open(self.archivo_salida, 'w') as f:
            json.dump(metadatos, f, indent=4)  # Guardar en formato JSON con una sangría de 4 espacios

    # Función para monitorizar la carpeta
    def monitorizar_carpeta(self, intervalo):
        while True:
            # Obtener y mostrar la cantidad de archivos y carpetas
            metadatos = self.obtener_metadata_archivos(self.carpeta_base)
            print(f"\nMetadatos de la carpeta '{self.carpeta_base}':")
            for item in metadatos:
                if item['tipo'] == 'carpeta':
                    print(f"{item['nombre']} - {item['cantidad_archivos']} elementos")
            
            time.sleep(intervalo)  # Espera el intervalo especificado antes de la siguiente verificación

    # Función para buscar un archivo dentro de la carpeta "uploadable_files"
    def buscar_archivo_en_uploadable_files(self, nombre_archivo, estructura):
        for elemento in estructura:
            # Si encontramos la carpeta "uploadable_files", buscamos dentro de ella
            if elemento['tipo'] == 'carpeta' and elemento['nombre'] == 'uploadable_files':
                return self.buscar_archivo_en_catalogo(nombre_archivo, elemento['subcarpetas-archivos-internos'])
        return None

    # Función para buscar recursivamente en el catálogo
    def buscar_archivo_en_catalogo(self, nombre_archivo, estructura):
        for elemento in estructura:
            # Si es un archivo y el nombre coincide, lo retorna
            if elemento['tipo'] == 'archivo' and elemento['nombre'] == nombre_archivo:
                return elemento['nombre']
            # Si es una carpeta, busca dentro de sus subcarpetas/archivos
            elif elemento['tipo'] == 'carpeta' and 'subcarpetas-archivos-internos' in elemento:
                resultado = self.buscar_archivo_en_catalogo(nombre_archivo, elemento['subcarpetas-archivos-internos'])
                if resultado:
                    return resultado
        return None

    # Función para verificar si un archivo está en "uploadable_files"
    def verificar_archivo_en_uploadable_files(self, nombre_archivo):
        # Abre y carga el archivo JSON
        with open(self.archivo_salida, 'r') as archivo:
            catalogo = json.load(archivo)

        # Busca el archivo solo en la carpeta uploadable_files
        resultado = self.buscar_archivo_en_uploadable_files(nombre_archivo, catalogo)
        
        # Verifica si el archivo fue encontrado
        if resultado:
            return f"{resultado}"
        else:
            return f""

# Función para cargar el archivo JSON y obtener el último id_data_node
def obtener_ultimo_id_datanode(ruta_json):
    # Cargar el archivo JSON
    with open(ruta_json, 'r') as archivo:
        datos = json.load(archivo)
    
    # Acceder a la lista de DataNodes
    lista_datanodes = datos.get("data_nodes", [])
    
    if lista_datanodes:
        # Obtener el último DataNode de la lista
        ultimo_datanode = lista_datanodes[-1]
        id_data_node = ultimo_datanode.get("id_data_node")
        return id_data_node
    else:
        return None
