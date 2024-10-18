import grpc
import sys
import os
import json
import random
import threading

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from datetime import datetime
from NameNode1.autentication import AuthServer
from concurrent import futures
from dotenv import load_dotenv
from json_manager import cargar_datos_desde_json, guardar_datos_json, agregar_datos_localizador, get_last_entry_data_nodes, buscar_rutas_en_nodos
from heartbeats import Heartbeats,ejecutar_eliminacion_inactivos
load_dotenv(dotenv_path="./configs/.env.namenode")  # Para NameNode

import protos.hdfs_pb2 as hdfs_pb2
import protos.hdfs_pb2_grpc as hdfs_pb2_grpc

namenode_ip = os.getenv("NAMENODE_IP_1")
namenode_port = os.getenv("NAMENODE_PORT_1")
datanodes_registry = os.getenv("DATANODES_REGISTRY_NAMENODE_1")
database_path = os.getenv("DATABASE_PATH_NAMENODE_1")
active_datanodes = os.getenv("ACTIVE_DATA_NODES_1")

class FullServicesServicer(hdfs_pb2_grpc.FullServicesServicer):
    def __init__(self):
        self.id_data_node = 0  # ID del DataNode
        self.bloques_info = {}  # Almacenar información sobre bloques
    
    def HandShakeNameNodeDataNode(self, request, context):
        # Procesa la solicitud de handshake
        ip_datanode = request.data_node_ip
        puerto = request.data_node_port
        
        # Intentar cargar datos del JSON
        try:
            datanodes_existentes = cargar_datos_desde_json(datanodes_registry)
        except FileNotFoundError:
            # Si el archivo no existe, inicializar una estructura vacía
            datanodes_existentes = {
                "data_nodes": [],
                "data_nodes_lideres": [],
                "data_nodes_seguidores": []
            }

        # Obtener la lista de IDs y calcular el nuevo ID
        if datanodes_existentes["data_nodes"]:
            used_ids = [datanode["id"] for datanode in datanodes_existentes["data_nodes"]]
            self.id_data_node = max(used_ids)  # Obtener el máximo ID existente
        else:
            self.id_data_node = 0  # Si no hay nodos, iniciar desde 0

        # Asignar un nuevo ID al DataNode
        self.id_data_node += 1  # Incrementar para el nuevo DataNode

        nuevo_datanode = {
            "id": self.id_data_node,
            "url": f"{ip_datanode}:{puerto}"
        }

        # Agregar el nuevo DataNode a la lista de data_nodes
        datanodes_existentes["data_nodes"].append(nuevo_datanode)
        datanodes_existentes["data_nodes_lideres"].append(nuevo_datanode)  # Asignado como líder
        datanodes_existentes["data_nodes_seguidores"].append(nuevo_datanode)  # Asignado como seguidor

        # Guardar los datos actualizados en el archivo JSON
        guardar_datos_json(datanodes_registry, datanodes_existentes)

        # Respuesta con el ID del DataNode
        response = hdfs_pb2.HandShakeNameNodeResponse()
        response.id_data_node = self.id_data_node  # ID real del DataNode
        response.estado_exitoso = True  # Indica que la conexión fue exitosa
        
        print(f"Recibido HandShake de DataNode: IP {ip_datanode}, Puerto {puerto}. Asignado el ID: {self.id_data_node}")
        return response

    def DataNodeDesignationNameNodeClient(self, request, context):
        heartbeats = Heartbeats(active_datanodes)
        datanodes = heartbeats.cargar_datos_desde_json_heartbeat()
        nombre__archivo = request.nombre_archivo
        
        print(f"\n\nDataNodes disponibles: {datanodes['data_nodes_activos']}\n\n")
        lista_todos_los_datanodes = datanodes["data_nodes_activos"]
        
        # Inicializar listas aleatorias para líderes y seguidores
        lideres_aleatorios = []
        seguidores_aleatorios = []
        # Verificar la cantidad de DataNodes disponibles para líderes
        if len(lista_todos_los_datanodes) <= 3:
            lideres_aleatorios = lista_todos_los_datanodes  # Asignar todos los líderes disponibles
        else:
            lideres_aleatorios = random.sample(lista_todos_los_datanodes, 3)  # Seleccionar 3 aleatorios

        # Verificar la cantidad de DataNodes disponibles para seguidores
        if len(lista_todos_los_datanodes) <= 3:
            seguidores_aleatorios = lista_todos_los_datanodes  # Asignar todos los seguidores disponibles
        else:
            seguidores_aleatorios = random.sample(lista_todos_los_datanodes, 3)  # Seleccionar 3 aleatorios
        
        print(f"\n\nDataNodes líderes seleccionados: {lideres_aleatorios}\n\n")
        print(f"DataNodes seguidores seleccionados: {seguidores_aleatorios}\n\n")
        
        # Extraer las IDs y URLs de los DataNodes seleccionados
        lista_id_lider = [dn["id"] for dn in lideres_aleatorios]
        lista_url_lider = [dn["url"] for dn in lideres_aleatorios]
        lista_id_seguidor = [dn["id"] for dn in seguidores_aleatorios]
        lista_url_seguidor = [dn["url"] for dn in seguidores_aleatorios]

        print(f"\n\n -- DataNodes líderes seleccionados: {lista_id_lider}\n\n")
        print(f" -- URLs DataNodes lideres seleccionados: {lista_url_lider}\n\n")
        print(f" -- DataNodes seguidores seleccionados: {lista_id_seguidor}\n\n")
        print(f" -- URLs DataNodes seguidores seleccionados: {lista_url_seguidor}\n\n")
        
        agregar_datos_localizador(nombre__archivo, lista_id_lider, lista_url_lider, lista_id_seguidor, lista_url_seguidor)

        # Crear la respuesta con los DataNodes seleccionados
        response = hdfs_pb2.DataNodeDesignationNameNodeResponse(
            lista_id_data_node_lider=lista_id_lider,
            lista_id_data_node_seguidor=lista_id_seguidor,
            lista_url_data_node_lider=lista_url_lider,
            lista_url_data_node_seguidor=lista_url_seguidor
        )
        
        return response
    
    def BlockReportNameNodeDataNode(self, request, context):
        # Extraer los datos recibidos del DataNode
        lista_rutas_bloques_seguidor = list(request.lista_rutas_bloques_seguidor)
        lista_rutas_bloques_lider = list(request.lista_rutas_bloques_lider)

        # Convertir los metadatos de bloques de JSON a diccionario
        try:
            metadatos_seguidor = json.loads(request.json_diccionario_metadatos_bloques_seguidor)
            metadatos_lider = json.loads(request.json_diccionario_metadatos_bloques_lider)
        except json.JSONDecodeError as e:
            context.set_details(f'Error al procesar JSON: {str(e)}')
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            return hdfs_pb2.BlockReportNameNodeResponse(estado_exitoso=False)

        id_datanode = str(request.id_data_node)  # Convertir el ID del DataNode a string

        # Cargar bloques existentes desde el JSON
        try:
            bloques = cargar_datos_desde_json(database_path)
        except Exception as e:
            print(f"Error al cargar datos desde JSON: {e}")
            context.set_details("Error al cargar datos desde JSON.")
            context.set_code(grpc.StatusCode.INTERNAL)
            return hdfs_pb2.BlockReportNameNodeResponse(estado_exitoso=False)

        # Generar timestamp para el nuevo reporte
        timestamp_reporte = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        # Inicializar la estructura del DataNode si no existe
        if id_datanode not in bloques:
            bloques[id_datanode] = {}

        # Crear un nuevo reporte con el timestamp como el número de reporte
        
        nuevo_reporte = {
            "n_reporte": timestamp_reporte,
            "contenido": {
                "lista_rutas_bloques_seguidor": lista_rutas_bloques_seguidor,
                "lista_rutas_bloques_lider": lista_rutas_bloques_lider,
                "metadatos": {
                    "seguidores": metadatos_seguidor,
                    "lider": metadatos_lider
                }
            }
        }

        # Agregar el nuevo reporte al DataNode, usando el timestamp como clave
        bloques[id_datanode][timestamp_reporte] = nuevo_reporte

        # Guardar los datos actualizados en el archivo JSON
        try:
            guardar_datos_json(database_path, bloques)
            estado_exitoso = True
        except Exception as e:
            print(f"Error al guardar los datos: {e}")
            estado_exitoso = False
            context.set_details(f"Error al guardar datos: {str(e)}")
            context.set_code(grpc.StatusCode.INTERNAL)

        # Crear y devolver la respuesta
        return hdfs_pb2.BlockReportNameNodeResponse(estado_exitoso=estado_exitoso)


    def FileLocationNameNodeClient(self, request, context):
        # Extraer información de la solicitud
        nombre_archivo = request.nombre_archivo
        nombre_usuario = request.nombre_usuario
        url_cliente = request.url_cliente

        lista_id_data_node_lider, lista_url_data_node_lider, lista_id_data_node_seguidor, lista_url_data_node_seguidor = get_last_entry_data_nodes(nombre_archivo)
        # Cargar los datos desde el archivo JSON donde se almacenan las ubicaciones de los bloques
        datos_bloques = cargar_datos_desde_json(database_path)
        
        
        # Buscar rutas para líderes
        verificar_en = "lideres"  
        resultados_rutas_lideres = buscar_rutas_en_nodos(lista_id_data_node_lider, verificar_en, nombre_archivo)
        lista_rutas_bloques_lider = [f"{{'{key}': {value}}}" for key, value in resultados_rutas_lideres.items()]
        
        # Imprimir la lista de rutas formateadas
        
        # Buscar rutas para seguidores
        verificar_en = "seguidores"
        resultados_rutas_seguidores = buscar_rutas_en_nodos(lista_id_data_node_seguidor, verificar_en, nombre_archivo)
        
        lista_rutas_bloques_seguidor = [f"{{'{key}': {value}}}" for key, value in resultados_rutas_seguidores.items()]
        
        print(f'\nIDs de data_nodes_lideres: {lista_id_data_node_lider}\n')
        print(f'\nURLs de data_nodes_lideres: {lista_url_data_node_lider}\n')
        print(f'\nIDs de data_nodes_seguidores: {lista_id_data_node_seguidor}\n')
        print(f'\nURLs de data_nodes_seguidores: {lista_url_data_node_seguidor}\n')
        print(f'\nLista_rutas_formateadas: {lista_rutas_bloques_lider}\n')
        print(f'\nLista_rutas_formateadas: {lista_rutas_bloques_seguidor}\n')
        


        # Devolver la respuesta con los datos correspondientes
        return hdfs_pb2.FileLocationNameNodeResponse(
            lista_id_data_node_lider=lista_id_data_node_lider,
            lista_id_data_node_seguidor=lista_id_data_node_seguidor,
            
            lista_url_data_node_lider=lista_url_data_node_lider,
            lista_url_data_node_seguidor=lista_url_data_node_seguidor,
            
            lista_rutas_bloques_lider=lista_rutas_bloques_lider,
            lista_rutas_bloques_seguidor=lista_rutas_bloques_seguidor
        )
        
    def HeartBeatNameNodeDataNode(self, request, context):
        # Obtener el ID del DataNode de la solicitud
        id_datanode = request.id_data_node
        
        heartbeats = Heartbeats(active_datanodes)
        
        # Actualizar el heartbeat del DataNode
        resultado = heartbeats.actualizar_heartbeat(id_datanode)
        
        print(f"Heartbeat recibido de DataNode {id_datanode} a las {resultado['timestamp']}")
        # Cargar los datos del archivo JSON que contiene la lista de DataNodes
        try:
            datanodes_existentes = cargar_datos_desde_json(datanodes_registry)
        except FileNotFoundError:
            print(f"Error: No se encontró el archivo {datanodes_registry}")
            response = hdfs_pb2.HeartBeatNameNodeResponse(
                estado_exitoso=False,
                timestrap=""
            )
            return response

        # Verificar si el DataNode con el ID proporcionado existe en la lista
        data_node_encontrado = any(dn["id"] == id_datanode for dn in datanodes_existentes["data_nodes"])

        if data_node_encontrado:
            # Generar un timestamp actual
            timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

            # Devolver una respuesta exitosa con el timestamp
            response = hdfs_pb2.HeartBeatNameNodeResponse(
                estado_exitoso=True,
                timestrap=timestamp
            )
            #print(f"Heartbeat recibido de DataNode {id_datanode} a las {timestamp}")
        else:
            # Si el DataNode no existe, marcar la operación como fallida
            response = hdfs_pb2.HeartBeatNameNodeResponse(
                estado_exitoso=False,
                timestrap=""
            )
            #print(f"DataNode {id_datanode} no encontrado. Heartbeat fallido.")

        return response
    
    def ReceiveBackupFromLeader(self, request, context):
        # Recibir los datos enviados por el NameNode Líder
        try:
            # Extraer la lista de archivos, contenidos y metadatos de la solicitud
            lista_archivos = request.lista_todos_los_archivos_en_namenodeleader
            lista_contenidos = request.lista_todos_contenidos_los_archivos_en_namenodeleader
            lista_metadatos = [json.loads(metadato) for metadato in request.lista_diccionario_metadatos_archivos]

            # Guardar los datos en un archivo JSON o base de datos
            datos_backup = {
                "archivos": lista_archivos,
                "contenidos": lista_contenidos,
                "metadatos": lista_metadatos
            }

            # Guardar los datos recibidos
            guardar_datos_json(database_path, datos_backup)
            estado_exitoso = True

        except Exception as e:
            print(f"Error al recibir o guardar los datos de respaldo: {e}")
            estado_exitoso = False

        # Devolver la respuesta con el estado del respaldo
        response = hdfs_pb2.BackUpNameNodeFollowerResponse(
            estado_exitoso=estado_exitoso
        )
        return response
    
    def DeleteFileNameNodeDataNode(self, request, context):
        nombre_archivo = request.nombre_archivo
        json_file_path = os.getenv("LOCALIZATION_FOLDER_1")
        try:
            # Abrir el archivo localization_folder.json
            if os.path.exists(json_file_path):
                with open(json_file_path, 'r') as json_file:
                    data = json.load(json_file)

                # Filtrar las entradas para eliminar el archivo con nombre_archivo
                data_filtrada = [entry for entry in data if nombre_archivo not in entry]

                # Escribir los datos actualizados de nuevo en el archivo
                with open(json_file_path, 'w') as json_file:
                    json.dump(data_filtrada, json_file, indent=4)
                
                return hdfs_pb2.DeleteFileNameNodeResponse(estado_exitoso=True)
            else:
                # Archivo localization_folder.json no existe
                return hdfs_pb2.DeleteFileNameNodeResponse(estado_exitoso=False)
        
        except Exception as e:
            print(f"Error eliminando archivo: {e}")
            return hdfs_pb2.DeleteFileNameNodeResponse(estado_exitoso=False)

namenode_port = namenode_port

def serve_grpc():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=1000))
    hdfs_pb2_grpc.add_FullServicesServicer_to_server(FullServicesServicer(), server)
    
    server.add_insecure_port(f'[::]:{namenode_port}')  # Puerto del servidor
    server.start()
    print(f"Servidor de HDFS escuchando en el puerto {namenode_port}...")
    heartbeats = Heartbeats(active_datanodes)

    # Lanzar el hilo de eliminación de DataNodes inactivos
    hilo_eliminacion = threading.Thread(target=ejecutar_eliminacion_inactivos, args=(heartbeats,))
    hilo_eliminacion.daemon = True  # El hilo se detendrá cuando el programa principal termine
    hilo_eliminacion.start()
    
    cargar_datos_desde_json(datanodes_registry)
    server.wait_for_termination()


if __name__ == '__main__':
    # Iniciar el servidor gRPC de NameNode en un hilo separado
    datanodes_registry = os.path.join('NameNode1','datanodes', 'datanodes_registry.json')
    cargar_datos_desde_json(datanodes_registry)
    cargar_datos_desde_json(database_path)
    grpc_thread = threading.Thread(target=serve_grpc)
    grpc_thread.start()
    autentication_server_port = os.getenv("AUTHENTICATION_SERVER_PORT_1")
    # Iniciar el servidor de autenticación con Flask en un hilo separado
    auth_server = AuthServer()
    flask_thread = threading.Thread(target=auth_server.app.run, kwargs={'host': namenode_ip, 'port': autentication_server_port })
    flask_thread.start()

    # Unir ambos hilos para que el programa no termine
    grpc_thread.join()
    flask_thread.join()
