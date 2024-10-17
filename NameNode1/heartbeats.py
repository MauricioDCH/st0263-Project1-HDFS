import os
import json
from datetime import datetime, timedelta
import threading
import time

from dotenv import load_dotenv
load_dotenv(dotenv_path="./configs/.env.namenode")  # Para NameNode
datanodes_registry = os.getenv("DATANODES_REGISTRY_NAMENODE_1")

class Heartbeats:
    def __init__(self, active_datanodes_file):
        """
        Constructor de la clase Heartbeats.
        
        :param active_datanodes_file: Ruta del archivo JSON para almacenar los DataNodes activos.
        """
        self.active_datanodes_file = active_datanodes_file

    def cargar_datos_desde_json_heartbeat(self):
        """
        Cargar los datos del archivo JSON que contiene los DataNodes activos.

        :return: Un diccionario con los datos del archivo JSON.
        :raises FileNotFoundError: Si el archivo no existe.
        :raises ValueError: Si el archivo está vacío o malformado.
        """
        if not os.path.exists(self.active_datanodes_file):
            # Crear el archivo si no existe con el formato adecuado.
            with open(self.active_datanodes_file, 'w') as file:
                json.dump({"data_nodes_activos": []}, file, indent=4)
        
        try:
            with open(self.active_datanodes_file, 'r') as file:
                data = json.load(file)
                if not data:  # Archivo vacío
                    raise ValueError("El archivo JSON está vacío.")
                return data
        except json.JSONDecodeError:
            # Si hay un error al decodificar, recrea el archivo con el formato adecuado
            with open(self.active_datanodes_file, 'w') as file:
                json.dump({"data_nodes_activos": []}, file, indent=4)
            print(f"Problemas al decodificar el archivo {self.active_datanodes_file}. Formato JSON inválido. Se ha recreado un nuevo archivo y los datos se han perdido.")
            with open(self.active_datanodes_file, 'r') as file:
                data = json.load(file)
                if not data:  # Archivo vacío
                    raise ValueError("El archivo JSON está vacío.")
                return data

    def guardar_datos_en_json_heartbeat(self, data):
        """
        Guardar los datos actualizados en el archivo JSON.

        :param data: Diccionario con los datos de los DataNodes que se guardarán.
        """
        with open(self.active_datanodes_file, 'w') as file:
            json.dump(data, file, indent=4)

    def actualizar_heartbeat(self, id_datanode):
        """
        Actualiza el timestamp y la URL de un DataNode en el archivo JSON. Si el DataNode no existe, lo crea.
        
        :param id_datanode: ID del DataNode que envía el heartbeat.
        :param url_datanode: URL del DataNode (por ejemplo, "127.0.0.1:6000").
        :return: Un diccionario con el resultado de la operación.
        """
        try:
            datanodes_existentes = self.cargar_datos_desde_json_heartbeat()
        except (FileNotFoundError, ValueError) as e:
            return {"estado_exitoso": False, "mensaje": str(e)}
        
        # Buscar el DataNode por su ID
        data_node_encontrado = False
        for dn in datanodes_existentes.get("data_nodes_activos", []):
            if dn["id"] == id_datanode:
                # Generar un timestamp actual y actualizar
                timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                dn["ultimo_heartbeat"] = timestamp
                dn["url"] = self.obtener_url_datanode(id_datanode)
                data_node_encontrado = True
                break
        
        if not data_node_encontrado:
            # Si no se encuentra el DataNode, agregarlo al archivo
            timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            nuevo_datanode = {
                "id": id_datanode,
                "url": self.obtener_url_datanode(id_datanode),
                "ultimo_heartbeat": timestamp
            }
            datanodes_existentes["data_nodes_activos"].append(nuevo_datanode)
            self.guardar_datos_en_json_heartbeat(datanodes_existentes)
            return {"estado_exitoso": True, "mensaje": "DataNode agregado por primera vez", "timestamp": timestamp}
        
        # Guardar los cambios en el archivo JSON si se actualizó un DataNode existente
        self.guardar_datos_en_json_heartbeat(datanodes_existentes)
        return {"estado_exitoso": True, "mensaje": "DataNode actualizado", "timestamp": timestamp}


    def eliminar_datanodes_inactivos(self, tiempo_expiracion=30):
        """
        Elimina los DataNodes inactivos que no han enviado un heartbeat en el último tiempo_expiracion segundos.
        
        :param tiempo_expiracion: Tiempo en segundos antes de considerar un DataNode inactivo (por defecto 30 segundos).
        """
        try:
            datanodes_existentes = self.cargar_datos_desde_json_heartbeat()
        except (FileNotFoundError, ValueError) as e:
            print(f"Error al cargar los datos: {str(e)}")
            return
        
        ahora = datetime.now()
        nuevos_datanodes = []

        for dn in datanodes_existentes.get("data_nodes_activos", []):
            ultimo_heartbeat = datetime.strptime(dn["ultimo_heartbeat"], "%Y-%m-%d %H:%M:%S")
            if (ahora - ultimo_heartbeat) <= timedelta(seconds=tiempo_expiracion):
                nuevos_datanodes.append(dn)  # DataNode activo
        
        if len(nuevos_datanodes) != len(datanodes_existentes["data_nodes_activos"]):
            # Actualizar el archivo JSON si se han eliminado DataNodes
            datanodes_existentes["data_nodes_activos"] = nuevos_datanodes
            self.guardar_datos_en_json_heartbeat(datanodes_existentes)
            print(f"Se han eliminado DataNodes inactivos.")

    # Función que obtiene la URL de un data node dado su ID
    def obtener_url_datanode(self, datanode_id):
        try:
            # Abrir y cargar el archivo JSON
            with open(datanodes_registry, 'r') as file:
                registro = json.load(file)
            
            # Función interna para buscar en una lista de nodos
            def buscar_nodo(nodos, id_buscado):
                for nodo in nodos:
                    if nodo['id'] == id_buscado:
                        return nodo['url']
                return None
            
            # Buscar en las tres categorías
            url = buscar_nodo(registro['data_nodes'], datanode_id)
            
            # Si la URL fue encontrada, retornarla, sino, indicar que no se encontró
            return f"{url}"
        except FileNotFoundError:
            return f"Error: No se encontró el archivo {datanodes_registry}."
        except json.JSONDecodeError:
            return f"Error: No se pudo decodificar el archivo JSON."

def ejecutar_eliminacion_inactivos(heartbeats, intervalo_eliminacion=2):
    """
    Función que se ejecuta en un hilo separado para eliminar DataNodes inactivos cada cierto tiempo.

    :param heartbeats: Instancia de la clase Heartbeats.
    :param intervalo_eliminacion: Intervalo de tiempo (en segundos) entre cada ejecución de eliminación de DataNodes inactivos.
    """
    while True:
        heartbeats.eliminar_datanodes_inactivos()
        time.sleep(intervalo_eliminacion)  # Esperar antes de la siguiente eliminación