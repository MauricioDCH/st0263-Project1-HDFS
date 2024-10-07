import grpc
import sys
import os
import json
import random
import threading
from datetime import datetime
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
sys.path.append(os.path.join(os.path.dirname(__file__), '../protos'))
from auth import AuthServer
from concurrent import futures
from dotenv import load_dotenv
load_dotenv(dotenv_path="./configs/.env.namenode")  # Para NameNode

import protos.hdfs_pb2 as hdfs_pb2
import protos.hdfs_pb2_grpc as hdfs_pb2_grpc




namenode_ip = os.getenv("NAMENODE_IP")
namenode_port = os.getenv("NAMENODE_PORT")
registered_users_db = os.getenv("REGISTERED_USERS_DB")
logged_users_db = os.getenv("LOGGED_USERS_DB")
datanodes_registry = os.getenv("DATANODES_REGISTRY")
database_path = os.getenv("DATABASE_PATH")

def cargar_datos_desde_json(ruta_archivo_json):
    # Verifica si el archivo existe
    if not os.path.exists(ruta_archivo_json):
        # Si no existe, crea un archivo con el formato requerido
        datos_iniciales = {
            "data_nodes": [],  # Lista de DataNodes
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
                datos[key] = [] if key != "data_nodes_archivos" else {}  # Inicializa como lista o dict

    # Guardar el formato correcto de nuevo en el archivo
    with open(ruta_archivo_json, 'w') as archivo:
        json.dump(datos, archivo, indent=4)
    
    return datos  # Retorna los datos formateados

def guardar_datos_json(ruta_json, datos):
    with open(ruta_json, 'w') as archivo:
        json.dump(datos, archivo, indent=4)

ruta_datanodes = os.path.join('NameNode','datanodes', 'datanodes_registry.json')
ruta_bloques = os.path.join('NameNode','database_namenode', 'DB_NameNode.json')
ruta_login = os.path.join('NameNode','users', 'registered_users')

class FullServicesServicer(hdfs_pb2_grpc.FullServicesServicer):
    def __init__(self):
        self.id_data_node = 0  # ID del DataNode
        self.bloques_info = {}  # Almacenar información sobre bloques
    
    
    def registrar_bloque(id_bloque, id_lider, url_lider, ruta_lider, lista_seguidores):
        # Cargar datos existentes desde el archivo JSON
        try:
            with open(ruta_bloques, 'r') as f:
                bloques = json.load(f)
        except FileNotFoundError:
            # Si el archivo no existe, inicializar la estructura
            bloques = {"bloques": {}}
        
        # Registrar el bloque con el líder y seguidores
        bloques["bloques"][id_bloque] = {
            "lider": {
                "id": id_lider,
                "url": url_lider,
                "ruta": ruta_lider
            },
            "seguidores": []
        }

        # Agregar seguidores
        for seguidor in lista_seguidores:
            bloques["bloques"][id_bloque]["seguidores"].append({
                "id": seguidor["id"],
                "url": seguidor["url"],
                "ruta": seguidor["ruta"]
            })

        # Guardar los datos actualizados en el archivo JSON
        with open(ruta_bloques, 'w') as f:
            json.dump(bloques, f, indent=4)

    
    
    
    
    
    
    
    
    
    
    
    def HandShakeNameNodeDataNode(self, request, context):
        # Procesa la solicitud de handshake
        ip_datanode = request.data_node_ip
        puerto = request.data_node_port
        
        # Intentar cargar datos del JSON
        try:
            datanodes_existentes = cargar_datos_desde_json(ruta_datanodes)
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
        guardar_datos_json(ruta_datanodes, datanodes_existentes)

        # Respuesta con el ID del DataNode
        response = hdfs_pb2.HandShakeNameNodeResponse()
        response.id_data_node = self.id_data_node  # ID real del DataNode
        response.estado_exitoso = True  # Indica que la conexión fue exitosa
        
        print(f"Recibido HandShake de DataNode: IP {ip_datanode}, Puerto {puerto}. Asignado el ID: {self.id_data_node}")
        return response
    


    def DataNodeDesignationNameNodeClient(self, request, context):
        datanodes = cargar_datos_desde_json(ruta_datanodes)
        
        # Extraer listas de IDs y URLs de los DataNodes líderes y seguidores
        lista_lideres = datanodes["data_nodes_lideres"]
        lista_seguidores = datanodes["data_nodes_seguidores"]

        # Inicializar listas aleatorias para líderes y seguidores
        lideres_aleatorios = []
        seguidores_aleatorios = []

        # Verificar la cantidad de DataNodes disponibles para líderes
        if len(lista_lideres) <= 3:
            lideres_aleatorios = lista_lideres  # Asignar todos los líderes disponibles
        else:
            lideres_aleatorios = random.sample(lista_lideres, 3)  # Seleccionar 3 aleatorios

        # Verificar la cantidad de DataNodes disponibles para seguidores
        if len(lista_seguidores) <= 3:
            seguidores_aleatorios = lista_seguidores  # Asignar todos los seguidores disponibles
        else:
            seguidores_aleatorios = random.sample(lista_seguidores, 3)  # Seleccionar 3 aleatorios

        # Extraer las IDs y URLs de los DataNodes seleccionados
        lista_id_lider = [dn["id"] for dn in lideres_aleatorios]
        lista_url_lider = [dn["url"] for dn in lideres_aleatorios]
        lista_id_seguidor = [dn["id"] for dn in seguidores_aleatorios]
        lista_url_seguidor = [dn["url"] for dn in seguidores_aleatorios]

        print(f"\n\nDataNodes líderes seleccionados: {lista_id_lider}\n\n")
        print(f"DataNodes seguidores seleccionados: {lista_id_seguidor}\n\n")


        # Crear la respuesta con los DataNodes seleccionados
        response = hdfs_pb2.DataNodeDesignationNameNodeResponse(
            lista_id_data_node_lider=lista_id_lider,
            lista_id_data_node_seguidor=lista_id_seguidor,
            lista_url_data_node_lider=lista_url_lider,
            lista_url_data_node_seguidor=lista_url_seguidor
        )
        
        return response





    """        
    def BlockReportNameNodeDataNode(self, request, context):
        # Extraer los datos recibidos del DataNode y convertir a listas de Python
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

        # ID del DataNode (se asume que se envía en la solicitud)
        id_datanode = request.id_data_node  # Asegúrate de que coincida con `id_data_node`

        # Cargar bloques existentes desde el JSON
        try:
            bloques = cargar_datos_desde_json(database_path)
        except Exception as e:
            print(f"Error al cargar datos desde JSON: {e}")
            context.set_details("Error al cargar datos desde JSON.")
            context.set_code(grpc.StatusCode.INTERNAL)
            return hdfs_pb2.BlockReportNameNodeResponse(estado_exitoso=False)

        # Inicializar una entrada para el nuevo DataNode si no existe
        if id_datanode not in bloques["data_nodes_archivos"]:
            bloques["data_nodes_archivos"][id_datanode] = {
                "lista_rutas_bloques_seguidor": [],
                "lista_rutas_bloques_lider": [],
                "json_diccionario_metadatos_bloques_seguidor": {},
                "json_diccionario_metadatos_bloques_lider": {}
            }

        # Agregar la información a los datos existentes
        bloques["data_nodes_archivos"][id_datanode]["lista_rutas_bloques_seguidor"].extend(lista_rutas_bloques_seguidor)
        bloques["data_nodes_archivos"][id_datanode]["lista_rutas_bloques_lider"].extend(lista_rutas_bloques_lider)
        bloques["data_nodes_archivos"][id_datanode]["json_diccionario_metadatos_bloques_seguidor"] = metadatos_seguidor
        bloques["data_nodes_archivos"][id_datanode]["json_diccionario_metadatos_bloques_lider"] = metadatos_lider

        # Guardar los datos actualizados en el archivo JSON
        try:
            guardar_datos_json(database_path, bloques)  # Guardamos directamente el diccionario bloques
            estado_exitoso = True
        except Exception as e:
            print(f"Error al guardar los datos: {e}")
            estado_exitoso = False
            context.set_details(f"Error al guardar datos: {str(e)}")
            context.set_code(grpc.StatusCode.INTERNAL)

        # Crear y devolver la respuesta
        response = hdfs_pb2.BlockReportNameNodeResponse(estado_exitoso=estado_exitoso)
        return response
        """
        
    def BlockReportNameNodeDataNode(self, request, context):
        # Extraer los datos recibidos del DataNode y convertir a listas de Python
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

        id_datanode = request.id_data_node  # ID del DataNode

        # Cargar bloques existentes desde el JSON
        try:
            bloques = cargar_datos_desde_json(database_path)
        except Exception as e:
            print(f"Error al cargar datos desde JSON: {e}")
            context.set_details("Error al cargar datos desde JSON.")
            context.set_code(grpc.StatusCode.INTERNAL)
            return hdfs_pb2.BlockReportNameNodeResponse(estado_exitoso=False)

        # Inicializar entrada para el nuevo DataNode si no existe
        if id_datanode not in bloques["data_nodes_archivos"]:
            bloques["data_nodes_archivos"][id_datanode] = {
                "lista_rutas_bloques_seguidor": [],
                "lista_rutas_bloques_lider": [],
                "metadatos": {
                    "seguidores": {},
                    "lider": {}
                }
            }

        # Agregar información a los datos existentes
        bloques["data_nodes_archivos"][id_datanode]["lista_rutas_bloques_seguidor"].extend(lista_rutas_bloques_seguidor)
        bloques["data_nodes_archivos"][id_datanode]["lista_rutas_bloques_lider"].extend(lista_rutas_bloques_lider)
        bloques["data_nodes_archivos"][id_datanode]["metadatos"]["seguidores"] = metadatos_seguidor
        bloques["data_nodes_archivos"][id_datanode]["metadatos"]["lider"] = metadatos_lider

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

        # Cargar los datos desde el archivo JSON donde se almacenan las ubicaciones de los bloques
        datos_bloques = cargar_datos_desde_json(ruta_bloques)

        bloques_lider = json.loads(datos_bloques["metadatos_bloque_lider"])
        bloques_seguidor = json.loads(datos_bloques["metadatos_bloque_seguidor"])

        bloque_encontrado_lider = next((bloque for bloque in bloques_lider if bloque["nombre"] == nombre_archivo and bloque["usuario"] == nombre_usuario), None)
        bloque_encontrado_seguidor = next((bloque for bloque in bloques_seguidor if bloque["nombre"] == nombre_archivo and bloque["usuario"] == nombre_usuario), None)


        if not bloque_encontrado_lider and not bloque_encontrado_seguidor:
            context.set_details(f"El archivo '{nombre_archivo}' no fue encontrado.")
            context.set_code(grpc.StatusCode.NOT_FOUND)
            return hdfs_pb2.FileLocationNameNodeResponse()  # Respuesta vacía en caso de error

        lista_id_data_node_lider = [dn["id"] for dn in datos_bloques["data_nodes_lideres"]]
        lista_id_data_node_seguidor = [dn["id"] for dn in datos_bloques["data_nodes_seguidores"]]
        lista_url_data_node_lider = [dn["url"] for dn in datos_bloques["data_nodes_lideres"]]
        lista_url_data_node_seguidor = [dn["url"] for dn in datos_bloques["data_nodes_seguidores"]]

        # Obtener rutas de los bloques
        lista_rutas_bloques_lider = datos_bloques["rutas_bloques_lider"]
        lista_rutas_bloques_seguidor = datos_bloques["rutas_bloques_seguidor"]

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

        # Cargar los datos del archivo JSON que contiene la lista de DataNodes
        try:
            datanodes_existentes = cargar_datos_desde_json(ruta_datanodes)
        except FileNotFoundError:
            print(f"Error: No se encontró el archivo {ruta_datanodes}")
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
            print(f"Heartbeat recibido de DataNode {id_datanode} a las {timestamp}")
        else:
            # Si el DataNode no existe, marcar la operación como fallida
            response = hdfs_pb2.HeartBeatNameNodeResponse(
                estado_exitoso=False,
                timestrap=""
            )
            print(f"DataNode {id_datanode} no encontrado. Heartbeat fallido.")

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
            guardar_datos_json(ruta_bloques, datos_backup)
            estado_exitoso = True

        except Exception as e:
            print(f"Error al recibir o guardar los datos de respaldo: {e}")
            estado_exitoso = False

        # Devolver la respuesta con el estado del respaldo
        response = hdfs_pb2.BackUpNameNodeFollowerResponse(
            estado_exitoso=estado_exitoso
        )
        return response



    
    
namenode_port = namenode_port

def serve_grpc():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    hdfs_pb2_grpc.add_FullServicesServicer_to_server(FullServicesServicer(), server)
    
    server.add_insecure_port(f'[::]:{namenode_port}')  # Puerto del servidor
    server.start()
    print(f"Servidor de HDFS escuchando en el puerto {namenode_port}...")
    cargar_datos_desde_json(ruta_datanodes)
    server.wait_for_termination()


if __name__ == '__main__':
    # Iniciar el servidor gRPC de NameNode en un hilo separado
    ruta_datanodes = os.path.join('NameNode','datanodes', 'datanodes_registry.json')
    cargar_datos_desde_json(ruta_datanodes)
    grpc_thread = threading.Thread(target=serve_grpc)
    grpc_thread.start()
    
    # Iniciar el servidor de autenticación con Flask en un hilo separado
    auth_server = AuthServer()
    flask_thread = threading.Thread(target=auth_server.app.run, kwargs={'host': namenode_ip, 'port': 5000 })
    flask_thread.start()

    # Unir ambos hilos para que el programa no termine
    grpc_thread.join()
    flask_thread.join()

