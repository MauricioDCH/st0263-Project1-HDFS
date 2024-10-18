import traceback
import grpc
import threading
import time
import json
import os
import sys
import shutil

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from concurrent import futures
from dotenv import load_dotenv
from gestor_archivos import GestorArchivos, obtener_ultimo_id_datanode
import protos.hdfs_pb2_grpc as pb2_grpc
import protos.hdfs_pb2 as pb2

load_dotenv(dotenv_path="./configs/.env.namenode")  # Para configuración de NameNode
load_dotenv(dotenv_path="./configs/.env.datanode")  # Para configuración de DataNode

namenode_ip = os.getenv("NAMENODE_IP_1")
namenode_port = os.getenv("NAMENODE_PORT_1")

datanode_ip = os.getenv("DATANODE_IP_2")
datanode_port = int(os.getenv("DATANODE_PORT_2"))
follower_resources = os.getenv("FOLLOWER_RESOURCES_2")
database_path = os.getenv("DATABASE_PATH_DATANODE_2")
datanode_register = os.getenv("DATANODES_REGISTRY_2")

datanode_ip_3 = os.getenv("DATANODE_IP_3")
datanode_port_3 = os.getenv("DATANODE_PORT_3")


def generar_y_llenar_archivo_DB():
    # Ejemplo de uso
    carpeta_base = os.getenv("FOLDER_RESOURCES_2")
    archivo_salida = os.getenv("DATABASE_PATH_DATANODE_2")
    gestor = GestorArchivos(carpeta_base, archivo_salida)
    # Guardar los metadatos en JSON inicialmente
    gestor.guardar_metadata_en_json()
    print(f"Los metadatos se han guardado en '{archivo_salida}'.")

class FullServicesServicer(pb2_grpc.FullServicesServicer):
    def _init_(self):
        self.id_data_node = None
        self.ruta_datanodes_registry = os.getenv("DATANODES_REGISTRY_2")
        self.metadata_file = os.path.join('database_datanode', 'DB_DataNode.json')

        if not os.path.exists(self.metadata_file):
            os.makedirs(os.path.join(self.resources_path, 'database_datanode'), exist_ok=True)
            with open(self.metadata_file, 'w') as f:
                json.dump({}, f)
    
    # ------------------------------------------------------------------------------------------------------------------------------------------------------------
    # ------------------------------------------------------------------------------------------------------------------------------------------------------------
    ## FUNCIONES PROCESAMIENTO
    # ------------------------------------------------------------------------------------------------------------------------------------------------------------
    # ------------------------------------------------------------------------------------------------------------------------------------------------------------
    
    # Función para procesar el archivo JSON y extraer rutas y metadatos
    def procesar_datanode_json(self, ruta_archivo):
        try:
            # Leer el archivo DB_DataNode.json
            with open(ruta_archivo, 'r') as archivo:
                datos = json.load(archivo)
            
            # Inicializar listas para las rutas de los bloques y diccionarios para los metadatos
            rutas_bloques_seguidor = []
            rutas_bloques_lider = []
            metadatos_bloques_seguidor = []
            metadatos_bloques_lider = []
            
            # Procesar los datos JSON
            for entrada in datos:
                if entrada["nombre"] == "follower":  # Procesar los datos del seguidor
                    self.procesar_carpetas(entrada["subcarpetas-archivos-internos"], "", rutas_bloques_seguidor, metadatos_bloques_seguidor)
                elif entrada["nombre"] == "leader":  # Procesar los datos del líder
                    self.procesar_carpetas(entrada["subcarpetas-archivos-internos"], "", rutas_bloques_lider, metadatos_bloques_lider)

            # Convertir los metadatos a JSON
            json_metadatos_seguidor = json.dumps(metadatos_bloques_seguidor, indent=4)
            json_metadatos_lider = json.dumps(metadatos_bloques_lider, indent=4)

            return {
                "lista_rutas_bloques_seguidor": rutas_bloques_seguidor,
                "lista_rutas_bloques_lider": rutas_bloques_lider,
                "json_diccionario_metadatos_bloques_seguidor": json_metadatos_seguidor,
                "json_diccionario_metadatos_bloques_lider": json_metadatos_lider
            }

        except Exception as e:
            print(f"Error al procesar el archivo JSON: {e}")
            return None

    # Función recursiva para procesar las subcarpetas y extraer rutas y metadatos
    def procesar_carpetas(self, subcarpetas, ruta_actual, lista_rutas, lista_metadatos):
        for item in subcarpetas:
            # Construir la ruta completa
            ruta_completa = f"{ruta_actual}/{item['nombre']}" if ruta_actual else item["nombre"]
            
            if item["tipo"] == "archivo":
                lista_rutas.append(ruta_completa)  # Aquí se agrega la ruta completa
                lista_metadatos.append({
                    "nombre": item["nombre"],
                    "tamano-bytes": item["tamano-bytes"],
                    "fecha_modificacion": item["fecha_modificacion"],
                    "usuario": item["usuario"]
                })
            elif item["tipo"] == "carpeta":
                # Llamada recursiva para procesar subcarpetas
                self.procesar_carpetas(item["subcarpetas-archivos-internos"], ruta_completa, lista_rutas, lista_metadatos)


    # ------------------------------------------------------------------------------------------------------------------------------------------------------------
    # ------------------------------------------------------------------------------------------------------------------------------------------------------------
    ## FUNCIONES DE SERVICIO QUE RECIBE DEL CLIENTE Y OTROS DATANODES.
    # ------------------------------------------------------------------------------------------------------------------------------------------------------------
    # ------------------------------------------------------------------------------------------------------------------------------------------------------------
    

    def UploadFileDataNodeClient(self, request, context):
        response = pb2.UploadFileDataNodeResponse()
        resources_path = os.getenv("LEADER_RESOURCES_2")
        try:
            dir_name_leader = os.path.join(resources_path, request.nombre_archivo)
            os.makedirs(dir_name_leader, exist_ok=True)
            
            file_path = os.path.join(dir_name_leader, f'bloque_{request.lista_id_data_node_lider[1]}.bin')
            os.makedirs(os.path.dirname(file_path), exist_ok=True)
            with open(file_path, 'wb') as f:
                f.write(request.lista_contenido_bloques_lider[1])

            if len(request.lista_id_data_node_lider) >= 1 or len(request.lista_id_data_node_seguidor) >= 1:

                pipeline_request = pb2.PipeLineDataNodeRequest()
                pipeline_request.nombre_archivo = request.nombre_archivo  
                pipeline_request.id_data_node_lider.extend(request.lista_id_data_node_lider)  
                pipeline_request.id_data_node_seguidor.extend(request.lista_id_data_node_seguidor)
                pipeline_request.contenido_bloques_lider.extend(request.lista_contenido_bloques_lider)
                pipeline_request.contenido_bloques_seguidor.extend(request.lista_contenido_bloques_lider)

                print("\n\n\nContenido de la petición que se va a hacer a datanode siguiente...")
                print(f'Nombre del archivo: {pipeline_request.nombre_archivo}')
                print(f'Lista de ID de DataNode líder: {pipeline_request.id_data_node_lider}')
                print(f'Lista de ID de DataNode seguidor: {pipeline_request.id_data_node_seguidor}')
                print(f'Lista de contenido de bloques líder: {pipeline_request.contenido_bloques_lider}')
                print(f'Lista de contenido de bloques seguidor: {pipeline_request.contenido_bloques_seguidor}')

                self.connectToDataNode(datanode_ip_3, datanode_port_3, pipeline_request)
                response.estado_exitoso = True

        except Exception as e:
            response.estado_exitoso = False
            print(f"Error al guardar los bloques: {e}")

        return response

    def PipeLineDataNodeResponseDataNodeRequest(self, request, context):
        response = pb2.PipeLineDataNodeResponse()
        resources_path = os.getenv("LEADER_RESOURCES_2")
        
        # Imprimir contenido de la solicitud
        print(f'Nombre del archivo: {request.nombre_archivo}')
        print(f'ID DataNode líder: {request.id_data_node_lider}')
        print(f'ID DataNode seguidor: {request.id_data_node_seguidor}')
        print(f'Contenido de los bloques líder: {request.contenido_bloques_lider}')
        print(f'Contenido de los bloques seguidor: {request.contenido_bloques_seguidor}')

        try:
            dir_name_leader = os.path.join(resources_path, request.nombre_archivo)
            os.makedirs(dir_name_leader, exist_ok=True)
            
            file_path = os.path.join(dir_name_leader, f'bloque_{request.id_data_node_lider[1]}.bin')
            os.makedirs(os.path.dirname(file_path), exist_ok=True)
            
            with open(file_path, 'wb') as f:
                f.write(request.contenido_bloques_lider[1])

            dir_name_follower = os.path.join(follower_resources, request.nombre_archivo)
            os.makedirs(dir_name_follower, exist_ok=True)
            
            file_path = os.path.join(dir_name_follower, f'bloque_{request.id_data_node_seguidor[0]}.bin')
            os.makedirs(os.path.dirname(file_path), exist_ok=True)
            print(request.contenido_bloques_seguidor)
            print("iiiiiiiiiiiiiiiii")
            with open(file_path, 'wb') as f:
                f.write(request.contenido_bloques_seguidor[0])
            
            if len(request.id_data_node_lider) >= 1 or len(request.id_data_node_seguidor) >= 1:
                pipeline_request = pb2.PipeLineDataNodeRequest()
                pipeline_request.nombre_archivo = request.nombre_archivo  
                pipeline_request.id_data_node_lider.extend(request.id_data_node_lider)  
                pipeline_request.id_data_node_seguidor.extend(request.id_data_node_seguidor)
                pipeline_request.contenido_bloques_lider.extend(request.contenido_bloques_lider)
                pipeline_request.contenido_bloques_seguidor.extend(request.contenido_bloques_seguidor)
            
                self.connectToDataNode(datanode_ip_3, datanode_port_3, pipeline_request)
            
            response.estado_exitoso = True
            response.contenido_bloques_seguidor.extend(request.contenido_bloques_seguidor)
        except Exception as e:
            response.estado_exitoso = False
            print(f"Error en el pipeline de replicación: {e}")
        return response

    def DownloadFileDataNodeClient(self, request, context):
        response = pb2.DownloadFileDataNodeResponse()  # Suponiendo que tienes una respuesta definida
        print(f'Ingreso a la función DownloadFileDataNodeClient con el bloque...')
        print(f'Nombre del archivo: {request.nombre_archivo}')
        print(f'Nombre del usuario: {request.nombre_usuario}')
        lista_id_data_node_seguidor = request.lista_id_data_node_seguidor
        print(f'Lista de ID de DataNode seguidor: {lista_id_data_node_seguidor}')
        print(f'Primer ID de DataNode seguidor: {lista_id_data_node_seguidor[0]}')
        print(f'URL del cliente: {request.url_cliente}')
        primer_ruta = request.rutas_bloques_seguidor[0]
        
        diccionario_rutas = eval(primer_ruta)
        
        # Imprimir el diccionario
        print(diccionario_rutas)
        
        # Optener la lista de rutas de los bloques teniendo el id_data_node
        lista_rutas = diccionario_rutas[str(lista_id_data_node_seguidor[0])]
        print(f'Lista de rutas de bloques: {lista_rutas}')
        
        try:
            # Construir la ruta del archivo a descargar
            file_paths = []
            ruta_carpeta_seguidor = os.getenv("FOLLOWER_RESOURCES_2")
            for ruta in lista_rutas:
                file_path = os.path.join(ruta_carpeta_seguidor, ruta)
                file_paths.append(file_path)
            
            print(f'\n\nRutas de los bloques a descargar: {file_paths}\n\n')
            
            lista_contenido_bloques_seguidor = []
            # Verificar si los archivos existen
            for file_path in file_paths:
                if not os.path.isfile(file_path):
                    context.set_code(grpc.StatusCode.NOT_FOUND)
                    context.set_details(f"El archivo {file_path} no existe.")
                    response.estado_exitoso = False
                    return response
                else:
                    print(f'\nArchivo encontrado: {file_path}')
                    # Leer el contenido del archivo, agregarlo a la lista y envía el contenido al cliente
                    with open(file_path, 'rb') as f:
                        lista_contenido_bloques_seguidor.append(f.read())
            
            # Limitar el número de bloques a procesar para evitar sobrepasar el tamaño de la lista
            num_bloques = len(lista_contenido_bloques_seguidor) - 1

            # Imprimir los primeros 40 bytes de cada bloque sin repetir ni exceder el número de bloques
            for idx, bloque in enumerate(lista_contenido_bloques_seguidor[:num_bloques + 1]):  # Limitar la iteración
                print(f'Primeros 40 bytes del bloque {idx}: {bloque[:40]}')
            
            # Si la lista de bloques es mayor a 0, se retornan la lista de contenido de bloques y el estado exitoso
            if len(lista_contenido_bloques_seguidor) > 0:
                # Debes agregar cada bloque manualmente a la lista `repeated bytes` en el response
                for bloque in lista_contenido_bloques_seguidor:
                    response.lista_contenido_bloques_seguidor.append(bloque)  # Agregar cada bloque al repeated
                
                response.estado_exitoso = True
            else:
                response.estado_exitoso = False

        except FileNotFoundError as e:
            context.set_code(grpc.StatusCode.NOT_FOUND)
            context.set_details(str(e))
            response.estado_exitoso = False
            print(f"Error al descargar el archivo: {e}")
        except Exception as e:
            context.set_code(grpc.StatusCode.UNKNOWN)
            context.set_details("Error inesperado en la aplicación.")
            response.estado_exitoso = False
            print(f"Error inesperado: {e}")

        return response


    def PipeLineForGetDataNodeResponseDataNodeRequest(self, request, context):
        response = pb2.PipeLineForGetDataNodeResponse()
        """
        string nombre_archivo = 1; // Nombre del archivo
        repeated int32 lista_id_data_node_seguidor = 2; // IDs de los DataNodes donde se replicará este bloque
        repeated string rutas_bloques_seguidor = 3; // Rutas de bloques del DataNode
        """
        print(f'Ingreso a la función PipeLineForGetDataNodeResponseDataNodeRequest con el bloque...')
        print(f'Nombre del archivo: {request.nombre_archivo}')
        print(f'Lista de ID de DataNode seguidor: {request.lista_id_data_node_seguidor}')
        print(f'Rutas de bloques seguidor: {request.rutas_bloques_seguidor}')
        
        primer_ruta = request.rutas_bloques_seguidor[1]
        diccionario_rutas = eval(primer_ruta)
        # Imprimir el diccionario
        print(diccionario_rutas)
        
        # Optener la lista de rutas de los bloques teniendo el id_data_node
        lista_rutas = diccionario_rutas[str(request.lista_id_data_node_seguidor[1])]
        print(f'Lista de rutas de bloques: {lista_rutas}')
        try:
            # Construir la ruta del archivo a descargar
            file_paths = []
            ruta_carpeta_seguidor = os.getenv("FOLLOWER_RESOURCES_2")
            for ruta in lista_rutas:
                file_path = os.path.join(ruta_carpeta_seguidor, ruta)
                file_paths.append(file_path)
            
            print(f'\n\nRutas de los bloques a descargar: {file_paths}\n\n')
            
            lista_contenido_bloques_seguidor = []
            
            # Verificar si los archivos existen
            for file_path in file_paths:
                if not os.path.isfile(file_path):
                    print(file_path)
                    context.set_code(grpc.StatusCode.NOT_FOUND)
                    context.set_details(f"El archivo {file_path} no existe.")
                    response.estado_exitoso = False
                    return response
                else:
                    print(f'\nArchivo encontrado: {file_path}')
                    # Leer el contenido del archivo, agregarlo a la lista y envía el contenido al cliente
                    with open(file_path, 'rb') as f:
                        lista_contenido_bloques_seguidor.append(f.read())
            
            # Limitar el número de bloques a procesar para evitar sobrepasar el tamaño de la lista
            num_bloques = len(lista_contenido_bloques_seguidor)
            
            print(f'Número de bloques: {num_bloques}')
            # Imprimir los primeros 40 bytes de cada bloque sin repetir ni exceder el número de bloques
            for idx, bloque in enumerate(lista_contenido_bloques_seguidor[:num_bloques]):  # Limitar la iteración
                print(f'Primeros 40 bytes del bloque {idx}: {bloque[:40]}')
                
            # Si la lista de lista_id_data_node_seguidor es mayor a 1, se conecta con el siguiente DataNode
            if len(request.lista_id_data_node_seguidor) >= 1:
                pipeline_request = pb2.PipeLineForGetDataNodeRequest()
                pipeline_request.nombre_archivo = request.nombre_archivo
                pipeline_request.lista_id_data_node_seguidor.extend(request.lista_id_data_node_seguidor)
                pipeline_request.rutas_bloques_seguidor.extend(request.rutas_bloques_seguidor)
                
                print("\n\n\nContenido de la petición que se va a hacer a datanode siguiente...")
                print(f'Nombre del archivo: {pipeline_request.nombre_archivo}')
                print(f'Lista de ID de DataNode seguidor: {pipeline_request.lista_id_data_node_seguidor}')
                print(f'Rutas de bloques seguidor: {pipeline_request.rutas_bloques_seguidor}')
                
                dato = self.connectToDataNodeForDownload(datanode_ip_3, datanode_port_3, pipeline_request)
                response.lista_contenido_bloques_seguidor.extend(dato.lista_contenido_bloques_seguidor)
                # Pausar el programa por 5 segundos
                time.sleep(5)
            
            
            # Si la lista de bloques es mayor a 0, se retornan la lista de contenido de bloques y el estado exitoso
            if len(lista_contenido_bloques_seguidor) > 0:
                # Debes agregar cada bloque manualmente a la lista `repeated bytes` en el response
                for bloque in lista_contenido_bloques_seguidor:
                    response.lista_contenido_bloques_seguidor.append(bloque)
            
                response.estado_exitoso = True
            else:
                response.estado_exitoso = False
            
            print(f'Estado exitoso: {response.estado_exitoso}')
        except FileNotFoundError as e:
            context.set_code(grpc.StatusCode.NOT_FOUND)
            context.set_details(str(e))
            response.estado_exitoso = False
            print(f"Error al descargar el archivo: {e}")
        except Exception as e:
            context.set_code(grpc.StatusCode.UNKNOWN)
            context.set_details("Error inesperado en la aplicación.")
            response.estado_exitoso = False
            print(f"Error inesperado: {e}")

        return response

    def PipeLineForDeleteDataNodeResponseDataNodeRequest(self, request, context):
        response = pb2.PipeLineForDeleteDataNodeResponse()
        """
        string nombre_archivo = 1; // Nombre del archivo
        """
        leader_resources = os.getenv("LEADER_RESOURCES_2")
        follower_resources = os.getenv("FOLLOWER_RESOURCES_2")
        
        print(f'Ingreso a la función PipeLineForDeleteDataNodeResponseDataNodeRequest con el bloque...')
        
        print(f'Nombre del archivo: {request.nombre_archivo}')
        
        try:
            dir_name_leader = os.path.join(leader_resources, request.nombre_archivo)
            dir_name_follower = os.path.join(follower_resources, request.nombre_archivo)
            print(f'Directorio líder: {dir_name_leader}')
            print(f'Directorio seguidor: {dir_name_follower}')
            
            # Si el directorio existe, imprimir que existe y lo borra. Si no, imprimir que no existe
            if os.path.exists(dir_name_leader):
                print(f'El directorio \'{request.nombre_archivo}\' existe en líderes.')
                shutil.rmtree(dir_name_leader)
                print(f'El directorio \'{request.nombre_archivo}\' ha sido eliminado en líderes.')
            else:
                print(f'El directorio \'{request.nombre_archivo}\' no existe en líderes.')
            
            # Si el directorio existe, imprimir que existe y lo borra. Si no, imprimir que no existe
            if os.path.exists(dir_name_follower):
                print(f'El directorio \'{request.nombre_archivo}\' existe en seguidores.')
                shutil.rmtree(dir_name_follower)
                print(f'El directorio \'{request.nombre_archivo}\' ha sido eliminado en seguidores.')                
            else:
                print(f'El directorio \'{request.nombre_archivo}\' existe en seguidores')
                    # Enviar petición a NameNode para eliminar el archivo en el archivo localization_folder.json
            self.delete_file(request.nombre_archivo)
            
            if len(request.lista_id_bloque_lider) >= 1 or len(request.lista_id_bloque_seguidor) >= 1:
                pipeline_request = pb2.PipeLineForDeleteDataNodeRequest()
                pipeline_request.nombre_archivo = request.nombre_archivo
                pipeline_request.lista_id_bloque_lider.extend(request.lista_id_bloque_lider)
                pipeline_request.lista_id_bloque_seguidor.extend(request.lista_id_bloque_seguidor)
                
                self.connectToDataNodeForDelete(datanode_ip_3, datanode_port_3, pipeline_request)
            response.estado_exitoso = True
        except Exception as e:
            response.estado_exitoso = False
            print(f"Error eliminando archivos: {e}")

        return response





    def DeleteFileDataNodeClient(self, request, context):
        response = pb2.DeleteFileDataNodeResponse()
        leader_resources = os.getenv("LEADER_RESOURCES_2")
        follower_resources = os.getenv("FOLLOWER_RESOURCES_2")

        print(f'Nombre del archivo: {request.nombre_archivo}')
        print(f'Nombre del usuario: {request.nombre_usuario}')
        print(f'URL del cliente: {request.url_cliente}')
        
        
        try:
            dir_name_leader = os.path.join(leader_resources, request.nombre_archivo)
            dir_name_follower = os.path.join(follower_resources, request.nombre_archivo)
            print(f'Directorio líder: {dir_name_leader}')
            print(f'Directorio seguidor: {dir_name_follower}')
            
            # Si el directorio existe, imprimir que existe y lo borra. Si no, imprimir que no existe
            if os.path.exists(dir_name_leader):
                print(f'El directorio \'{request.nombre_archivo}\' existe en líderes.')
                shutil.rmtree(dir_name_leader)
                print(f'El directorio \'{request.nombre_archivo}\' ha sido eliminado en líderes.')
            else:
                print(f'El directorio \'{request.nombre_archivo}\' no existe en líderes.')
            
            # Si el directorio existe, imprimir que existe y lo borra. Si no, imprimir que no existe
            if os.path.exists(dir_name_follower):
                print(f'El directorio \'{request.nombre_archivo}\' existe en seguidores.')
                shutil.rmtree(dir_name_follower)
                print(f'El directorio \'{request.nombre_archivo}\' ha sido eliminado en seguidores.')                
            else:
                print(f'El directorio \'{request.nombre_archivo}\' existe en seguidores')
                        
            # Enviar petición a NameNode para eliminar el archivo en el archivo localization_folder.json
            self.delete_file(request.nombre_archivo)
            
            # Enviar petición a NameNode para eliminar el archivo en el archivo localization_folder.json
            self.delete_file(request.nombre_archivo)
            if len(request.lista_id_bloque_lider) >= 1 or len(request.lista_id_bloque_seguidor) >= 1:
                pipeline_request = pb2.PipeLineForDeleteDataNodeRequest()
                pipeline_request.nombre_archivo = request.nombre_archivo
                
                self.connectToDataNodeForDelete(datanode_ip_3, datanode_port_3, pipeline_request)
            # Enviar la respuesta exitosa
            response.estado_exitoso = True
        except Exception as e:
            response.estado_exitoso = False
            print(f"Error eliminando archivos: {e}")

        return response

    def ReadFileDataNodeClient(self, request, context):
        print(f'Ingreso a la función ReadFileDataNodeClient con el bloque...')
        response = pb2.ReadFileDataNodeResponse()

        print(f'Nombre del archivo: {request.nombre_archivo}')
        print(f'Nombre del usuario: {request.nombre_usuario}')
        lista_id_data_node_seguidor = request.lista_id_bloque_seguidor
        print(f'Lista de ID de bloques seguidor: {lista_id_data_node_seguidor}')
        print(f'Primer ID de bloque seguidor: {lista_id_data_node_seguidor[0]}')
        print(f'URL del cliente: {request.url_cliente}')
        primer_ruta = request.lista_rutas_bloques_seguidor[0]
        print(f'Primer ruta de bloque seguidor: {primer_ruta}')
        
        diccionario_rutas = eval(primer_ruta)
        
        # Imprimir el diccionario
        print(diccionario_rutas)
        
        # Optener la lista de rutas de los bloques teniendo el id_data_node
        lista_rutas = diccionario_rutas[str(lista_id_data_node_seguidor[0])]
        print(f'Lista de rutas de bloques: {lista_rutas}')
        try:
            # Construir la ruta del archivo a descargar
            file_paths = []
            ruta_carpeta_seguidor = os.getenv("FOLLOWER_RESOURCES_2")
            for ruta in lista_rutas:
                file_path = os.path.join(ruta_carpeta_seguidor, ruta)
                file_paths.append(file_path)
            
            print(f'\n\nRutas de los bloques a descargar: {file_paths}\n\n')
            
            lista_contenido_bloques_seguidor = []
            # Verificar si los archivos existen
            for file_path in file_paths:
                if not os.path.isfile(file_path):
                    context.set_code(grpc.StatusCode.NOT_FOUND)
                    context.set_details(f"El archivo {file_path} no existe.")
                    response.estado_exitoso = False
                    return response
                else:
                    print(f'\nArchivo encontrado: {file_path}')
                    # Leer el contenido del archivo, agregarlo a la lista y envía el contenido al cliente
                    with open(file_path, 'rb') as f:
                        lista_contenido_bloques_seguidor.append(f.read())


            if len(lista_id_data_node_seguidor) >= 1:
                pipeline_request = pb2.PipeLineForGetDataNodeRequest()
                pipeline_request.nombre_archivo = request.nombre_archivo
                pipeline_request.lista_id_data_node_seguidor.extend(request.lista_id_data_node_seguidor)
                pipeline_request.rutas_bloques_seguidor.extend(request.rutas_bloques_seguidor)
                
                print("\n\n\nContenido de la petición que se va a hacer a datanode siguiente...")
                print(f'Nombre del archivo: {pipeline_request.nombre_archivo}')
                print(f'Lista de ID de DataNode seguidor: {pipeline_request.lista_id_data_node_seguidor}')
                print(f'Rutas de bloques seguidor: {pipeline_request.rutas_bloques_seguidor}')
                
                dato = self.connectToDataNodeForDownload(datanode_ip_3, datanode_port_3, pipeline_request)
                print(dato)
                response.lista_contenido_bloques_seguidor.extend(dato.lista_contenido_bloques_seguidor)
                # Pausar el programa por 5 segundos
                response.estado_exitoso = True
            
            print(f'Easdasdasdasd Este es la lista de contenido de bloques seguidor: {response.lista_contenido_bloques_seguidor}')
            # Limitar el número de bloques a procesar para evitar sobrepasar el tamaño de la lista
            num_bloques = len(response.lista_contenido_bloques_seguidor) - 1

            # Imprimir los primeros 40 bytes de cada bloque sin repetir ni exceder el número de bloques
            for idx, bloque in enumerate(response.lista_contenido_bloques_seguidor[:num_bloques + 1]):  # Limitar la iteración
                print(f'Primeros 40 bytes del bloque {idx}: {bloque[:40]}')
            
            # Si la lista de bloques es mayor a 0, se retornan la lista de contenido de bloques y el estado exitoso
            if len(response.lista_contenido_bloques_seguidor) > 0:
                # Debes agregar cada bloque manualmente a la lista `repeated bytes` en el response
                print("Holaa, ya voy aquí")
                print(len(response.lista_contenido_bloques_seguidor))
                print(response.lista_contenido_bloques_seguidor)
                
                for bloque in range(len(response.lista_contenido_bloques_seguidor)):
                    print(f"Holaa, ya voy aquí", bloque)
                    print(response.lista_contenido_bloques_seguidor[bloque])
                    response.lista_contenido_bloques_seguidor[bloque]
                
                response.estado_exitoso = True
            else:
                response.estado_exitoso = False

        except FileNotFoundError as e:
            context.set_code(grpc.StatusCode.NOT_FOUND)
            context.set_details(str(e))
            response.estado_exitoso = False
            print(f"Error al descargar el archivo: {e}")
        except Exception as e:
            context.set_code(grpc.StatusCode.UNKNOWN)
            context.set_details("Error inesperado en la aplicación.")
            response.estado_exitoso = False
            print(f"Error inesperado: {e}")

        return response

    # ------------------------------------------------------------------------------------------------------------------------------------------------------------
    # ------------------------------------------------------------------------------------------------------------------------------------------------------------
    ## FUNCIONES QUE SE ENVÍAN AL NAMENODE.
    # ------------------------------------------------------------------------------------------------------------------------------------------------------------
    # ------------------------------------------------------------------------------------------------------------------------------------------------------------

    def registerToNameNode(self, datanode_register, namenode_ip, namenode_port, datanode_ip, datanode_port):
        registry_file_path = datanode_register
        try:
            if not os.path.exists(registry_file_path):
                print(f"Archivo {registry_file_path} no encontrado, creando archivo...")
                os.makedirs(os.path.dirname(registry_file_path), exist_ok=True)
                with open(registry_file_path, 'w') as f:
                    json.dump([], f)

            # Realizar la solicitud al NameNode
            with grpc.insecure_channel(f'{namenode_ip}:{namenode_port}') as channel:
                stub = pb2_grpc.FullServicesStub(channel)
                request = pb2.HandShakeDataNodeRequest(
                    data_node_ip=datanode_ip,
                    data_node_port=datanode_port
                )
                print("\nPetición hecha al NameNode")
                response = stub.HandShakeNameNodeDataNode(request)

                # Verificar si la respuesta contiene el id_data_node
                if not hasattr(response, 'id_data_node') or response.id_data_node is None:
                    raise ValueError("La respuesta del NameNode no contiene 'id_data_node'.")

                # Almacenar el ID del DataNode devuelto por el NameNode
                self.id_data_node = response.id_data_node

                # Verifica si el archivo existe, si no, lo crea con la estructura básica
                if not os.path.exists(datanode_register):
                    with open(datanode_register, 'w') as f:
                        json.dump({"data_nodes": []}, f, indent=4)

                # Abrir y leer el archivo JSON
                with open(datanode_register, 'r+') as f:
                    try:
                        data = json.load(f)
                    except json.JSONDecodeError:
                        data = {"data_nodes": []}
                    
                    if 'data_nodes' not in data:
                        data['data_nodes'] = []

                    # Registrar el nuevo DataNode si no está ya registrado
                    if not any(dn['id_data_node'] == self.id_data_node for dn in data['data_nodes']):
                        new_datanode_info = {
                            "id_data_node": self.id_data_node,
                            "data_node_ip": datanode_ip,
                            "data_node_port": datanode_port
                        }
                        data['data_nodes'].append(new_datanode_info)

                        # Volver a escribir el archivo
                        f.seek(0)
                        json.dump(data, f, indent=4)
                        f.truncate()
                    else:
                        print("Este DataNode ya está registrado en el archivo.")

                # Imprimir el resultado del registro
                if response.estado_exitoso:
                    print(f'ID asignado al dataNode: {self.id_data_node}, asignado y registrado exitosamente en NameNode.')
                else:
                    print(f'Error al registrar DataNode {self.id_data_node} en NameNode.')

        except FileNotFoundError as e:
            print(f"Error: {e}. Verifica que el archivo o la ruta sean correctos.")
        except grpc.RpcError as e:
            print(f"Error en la comunicación gRPC: {e.details()}")
        except Exception as e:
            print(f"Ocurrió un error inesperado: {e}")
            traceback.print_exc()


    # Función para enviar el BlockReport al NameNode usando gRPC
    def enviar_block_report(self, servidor_namenode):
        ruta_json = os.getenv("DATABASE_PATH_DATANODE_2")
        
        print(f"Obteniendo los datos del DataNode desde '{ruta_json}'...")
        
        # Procesar el archivo DB_DataNode.json
        datos_datanode = self.procesar_datanode_json(ruta_json)

        if datos_datanode and self.id_data_node:
            # Conectar con el NameNode mediante gRPC
            with grpc.insecure_channel(servidor_namenode) as canal:
                stub = pb2_grpc.FullServicesStub(canal)

                # Convertir las listas de diccionarios a cadenas JSON
                metadatos_bloques_seguidor = json.dumps(datos_datanode["json_diccionario_metadatos_bloques_seguidor"])
                metadatos_bloques_lider = json.dumps(datos_datanode["json_diccionario_metadatos_bloques_lider"])

                # Crear la solicitud BlockReportDataNodeRequest con cadenas JSON
                request = pb2.BlockReportDataNodeRequest(
                    id_data_node=self.id_data_node,  # Usar el ID obtenido del archivo
                    lista_rutas_bloques_seguidor=datos_datanode["lista_rutas_bloques_seguidor"],
                    lista_rutas_bloques_lider=datos_datanode["lista_rutas_bloques_lider"],
                    json_diccionario_metadatos_bloques_seguidor=metadatos_bloques_seguidor,  # Convertido a string JSON
                    json_diccionario_metadatos_bloques_lider=metadatos_bloques_lider         # Convertido a string JSON
                )
                
                # Enviar la solicitud y obtener la respuesta
                respuesta = stub.BlockReportNameNodeDataNode(request)
                if respuesta.estado_exitoso:
                    print("BlockReport enviado al NameNode exitosamente.\n----\n")
                else:
                    print("Error al enviar el BlockReport al NameNode.")

    def connectToDataNode(self, datanode_ip, datanode_port, pipeline_request):
        try:
            with grpc.insecure_channel(f'{datanode_ip}:{datanode_port}') as channel:
                print("oooooooooooooooo")
                stub = pb2_grpc.FullServicesStub(channel) 

                response = stub.PipeLineDataNodeResponseDataNodeRequest(pipeline_request)
                print("rrrrrrrrrrrrrrrr")

                if response.estado_exitoso:
                    print(f'Conexión exitosa con DataNode en {datanode_ip}:{datanode_port}')
                else:
                    print(f'Error en la conexión hacia DataNode en {datanode_ip}:{datanode_port}')
            
        except Exception as e:
            print(f'Error al conectar con DataNode {datanode_ip}:{datanode_port}: {e}')

    def connectToDataNodeForDownload(self, datanode_ip, datanode_port, pipeline_request):
            try:
                with grpc.insecure_channel(f'{datanode_ip}:{datanode_port}') as channel:
                    print("aaaaaaaaaa")
                    stub = pb2_grpc.FullServicesStub(channel) 

                    response = stub.PipeLineForGetDataNodeResponseDataNodeRequest(pipeline_request)
                    print("xxxxxxxxxxxxxxxxxxxx")

                    if response.estado_exitoso:
                        print(f'Conexión exitosa con DataNode en {datanode_ip}:{datanode_port}')
                        return response
                    else:
                        print(f'Error en la conexión hacia DataNode en {datanode_ip}:{datanode_port}')
                        return None
                
            except Exception as e:
                print(f'Error al conectar con DataNode {datanode_ip}:{datanode_port}: {e}')

    def connectToDataNodeForDelete(self, datanode_ip, datanode_port, pipeline_request):
            try:
                with grpc.insecure_channel(f'{datanode_ip}:{datanode_port}') as channel:
                    print("aaaaaaaaaa")
                    stub = pb2_grpc.FullServicesStub(channel) 

                    response = stub.PipeLineForDeleteDataNodeResponseDataNodeRequest(pipeline_request)
                    print("xxxxxxxxxxxxxxxxxxxx")

                    if response.estado_exitoso:
                        print(f'Conexión exitosa con DataNode en {datanode_ip}:{datanode_port}')
                        return response
                    else:
                        print(f'Error en la conexión hacia DataNode en {datanode_ip}:{datanode_port}')
                        return None
                
            except Exception as e:
                print(f'Error al conectar con DataNode {datanode_ip}:{datanode_port}: {e}')
              

    # Función para eliminar un archivo del NameNode
    def delete_file(self, nombre_archivo):
        print(f"Eliminando el archivo '{nombre_archivo}' del NameNode...")
        with grpc.insecure_channel(f'{namenode_ip}:{namenode_port}') as channel:
            stub = pb2_grpc.FullServicesStub(channel)
            request = pb2.DeleteFileDataNodeRequest(nombre_archivo=nombre_archivo)
            
            # Llamar al servicio DeleteFileNameNodeDataNode
            response = stub.DeleteFileNameNodeDataNode(request)
            
            if response.estado_exitoso:
                print(f"El archivo '{nombre_archivo}' fue eliminado con éxito.")
            else:
                print(f"Error: No se pudo eliminar el archivo '{nombre_archivo}'.")
    
    def heartBeatDataNodeRequest(self, servidor_namenode):
        datanode_register = os.getenv("DATANODES_REGISTRY_2")
        print(f'Ingreso a la función heartBeatDataNodeRequest...')
        ultimo_id = obtener_ultimo_id_datanode(datanode_register)
        with grpc.insecure_channel(servidor_namenode) as channel:
            stub = pb2_grpc.FullServicesStub(channel)
            
            # Crear y enviar la solicitud de latido con el ID del DataNode
            request = pb2.HeartBeatDataNodeRequest(id_data_node=ultimo_id)
            response = stub.HeartBeatNameNodeDataNode(request)
            
            # Imprimir la respuesta del NameNode
            print(f"Estado exitoso: {response.estado_exitoso}")
            print(f"Timestamp del latido: {response.timestrap}")

# Función para generar y llenar el archivo DB periódicamente
def generar_y_llenar_archivo_DB_periodicamente():
    while True:
        print("\n----\nGenerando y llenando el archivo DB...")
        generar_y_llenar_archivo_DB()
        print("Archivo DB generado y llenado exitosamente.\n----\n")
        time.sleep(20) # Esperar 20 segundos antes de generar el archivo nuevamente

# Función para enviar el Block Report cada 30 segundos
def enviar_block_report_periodicamente(datanode_servicer, namenode_ip, namenode_port):
    while True:
        print("\n----\nEnviando Block Report al NameNode...")
        datanode_servicer.enviar_block_report(f'{namenode_ip}:{namenode_port}')
        time.sleep(25)  # Esperar 25 segundos antes de enviar el siguiente Block Report

def enviar_heart_beat_periodicamente(datanode_servicer, namenode_ip, namenode_port):
    while True:
        print("\n----\nEnviando HeartBeat al NameNode...")
        datanode_servicer.heartBeatDataNodeRequest(f'{namenode_ip}:{namenode_port}')
        print("\n----\n")
        time.sleep(30)  # Esperar 30 segundos antes de enviar el siguiente HeartBeat

def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=1000))
    datanode_servicer = FullServicesServicer()
    
    
    # Registrar el DataNode al iniciar el servidor
    pb2_grpc.add_FullServicesServicer_to_server(datanode_servicer, server)
    print(f'Iniciando servidor DataNode en {datanode_ip}:{datanode_port}...')
    server.add_insecure_port(f'{datanode_ip}:{datanode_port}')
    server.start()

    # Registrar el DataNode al NameNode
    datanode_servicer.registerToNameNode(datanode_register, namenode_ip, namenode_port, datanode_ip, datanode_port)
    # Iniciar la generación y llenado del archivo DB periódicamente en un hilo separado
    
    thread_generar_db = threading.Thread(target=generar_y_llenar_archivo_DB_periodicamente)
    thread_generar_db.daemon = True
    thread_generar_db.start()
    time.sleep(1)  # Esperar 1 segundo antes de enviar el primer Block Report
    
    # Iniciar el envío periódico del Block Report en un hilo separado
    thread_block_report = threading.Thread(target=enviar_block_report_periodicamente, args=(datanode_servicer, namenode_ip, namenode_port))
    thread_block_report.daemon = True  # El hilo se cerrará cuando el programa principal termine
    thread_block_report.start()
    time.sleep(1)  # Esperar 1 segundo antes de enviar el primer Block Report
    
    # Iniciar el envío periódico del HeartBeat en un hilo separado
    thread_heart_beat = threading.Thread(target=enviar_heart_beat_periodicamente, args=(datanode_servicer, namenode_ip, namenode_port))
    thread_heart_beat.daemon = True  # El hilo se cerrará cuando el programa principal termine
    thread_heart_beat.start()
    time.sleep(1)  # Esperar 1 segundo antes de enviar el primer Block Report
    

    try:
        # Mantener el servidor activo
        while True:
            time.sleep(86400)  # Mantiene el servidor activo (1 día)
    except KeyboardInterrupt:
        server.stop(0)

if __name__ == '__main__':
    serve()