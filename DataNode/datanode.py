import grpc
from concurrent import futures
import time
import json
import os
import sys
from dotenv import load_dotenv
from gestor_archivos import GestorArchivos

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import protos.hdfs_pb2_grpc as pb2_grpc
import protos.hdfs_pb2 as pb2

project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
load_dotenv(dotenv_path="./configs/.env.datanode")  # Para configuración de NameNode
load_dotenv(dotenv_path="./configs/.env.namenode")  # Para configuración de NameNode

datanode_ip = os.getenv("DATANODE_IP")
datanode_port = int(os.getenv("DATANODE_PORT"))
folder_resources = os.getenv("FOLDER_RESOURCES")
leader_resources = os.getenv("LEADER_RESOURCES")
follower_resources = os.getenv("FOLLOWER_RESOURCES")
database_path = os.getenv("DATABASE_PATH")
nextnode_ip = os.getenv("NEXTNODE_IP")
nextnode_port = os.getenv("NEXTNODE_PORT")
datanode_register = os.getenv("DATANODES_REGISTRY")

namenode_ip = os.getenv("NAMENODE_IP")
namenode_port = os.getenv("NAMENODE_PORT")

def generar_y_llenar_archivo_DB():
    # Ejemplo de uso
    carpeta_base = os.getenv("FOLDER_RESOURCES")
    archivo_salida = os.getenv("DATABASE_PATH")
    gestor = GestorArchivos(carpeta_base, archivo_salida)
    # Guardar los metadatos en JSON inicialmente
    gestor.guardar_metadata_en_json()
    print(f"Los metadatos se han guardado en '{archivo_salida}'.")

class FullServicesServicer(pb2_grpc.FullServicesServicer):

    def _init_(self):
        self.id_data_node = self.obtener_id_data_node()
        #resources_path = os.getenv("LEADER_RESOURCES")
        self.ruta_datanodes_registry = os.getenv("DATANODES_REGISTRY")
        self.metadata_file = os.path.join('database_datanode', 'DB_DataNode.json')

        if not os.path.exists(self.metadata_file):
            #os.makedirs(os.path.join(resources_path, 'database_datanode'), exist_ok=True)
            os.makedirs(os.path.join(self.resources_path, 'database_datanode'), exist_ok=True)
            with open(self.metadata_file, 'w') as f:
                json.dump({}, f)
    
    def obtener_id_data_node(self):
        """
        Obtiene el ID del DataNode desde el archivo datanodes_registry.json.
        Devuelve el id_data_node de la última entrada en data_nodes.
        """
        try:
            with open(self.ruta_datanodes_registry, 'r') as archivo:
                datos = json.load(archivo)

                # Obtener la última entrada en la lista de data_nodes
                if "data_nodes" in datos and datos["data_nodes"]:
                    # Acceder a la última entrada
                    ultima_entrada = datos["data_nodes"][-1]  # Obtiene el último DataNode
                    id_data_node = ultima_entrada.get("id_data_node", None)  # Obtener el id_data_node

                    if id_data_node is None:
                        print("No se encontró id_data_node en la última entrada.")
                    return id_data_node
                else:
                    print("No hay entradas en data_nodes.")
                    return None
        except FileNotFoundError:
            print(f"Error: El archivo {self.ruta_datanodes_registry} no fue encontrado.")
            return None
        except json.JSONDecodeError:
            print("Error al decodificar el archivo JSON.")
            return None

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

    def UploadFileDataNodeClient(self, request, context):
        response = pb2.UploadFileDataNodeResponse()
        resources_path = os.getenv("LEADER_RESOURCES")
        try:
            dir_name_leader = os.path.join(resources_path, request.nombre_archivo)
            os.makedirs(dir_name_leader, exist_ok=True)

            blocks_leader = []
            for idx, bloque in enumerate(request.lista_contenido_bloques_lider):
                file_path = os.path.join(dir_name_leader, f'bloque_{request.lista_id_data_node_lider[idx]}.bin')
                os.makedirs(os.path.dirname(file_path), exist_ok=True) 
                with open(file_path, 'wb') as f:
                    f.write(bloque)
                
                blocks_leader.append(bloque)

            dir_name_follower = os.path.join(follower_resources, request.nombre_archivo)
            os.makedirs(dir_name_follower, exist_ok=True)

            blocks_follower = []
            for idx, bloque in enumerate(request.lista_contenido_bloques_lider):
                file_path = os.path.join(dir_name_follower, f'bloque_{request.lista_id_data_node_seguidor[idx]}.bin')
                os.makedirs(os.path.dirname(file_path), exist_ok=True) 
                with open(file_path, 'wb') as f:
                    f.write(bloque)
                
                blocks_follower.append(bloque)

            self.ConnectToDataNode(nextnode_ip, nextnode_port, blocks_leader)

            response.estado_exitoso = True
        except Exception as e:
            response.estado_exitoso = False
            print(f"Error al guardar los bloques: {e}")

        return response


    def PipeLineDataNodeResponseDataNodeRequest(self, request, context):
        response = pb2.PipeLineDataNodeResponse()

        try:
            for idx, bloque in enumerate(request.contenido_bloques_lider):
                file_path = os.path.join(follower_resources, f'bloque_{request.id_bloque_seguidor[idx]}.bin')
                os.makedirs(os.path.dirname(file_path), exist_ok=True)
                with open(file_path, 'wb') as f:
                    f.write(bloque)

            response.id_bloque_lider = request.id_bloque_lider[0]  # Ejemplo, puedes ajustar según tus necesidades
            response.id_bloque_seguidor = request.id_bloque_seguidor[0]  # Ejemplo, puedes ajustar según tus necesidades
            response.estado_exitoso = True
            response.tamano_bloque_lider = str(len(request.contenido_bloques_lider))  # Tamaño como string
            response.tamano_bloque_seguidor = str(len(request.contenido_bloques_seguidor))  # Tamaño como string
        except Exception as e:
            response.estado_exitoso = False
            print(f"Error en el pipeline de replicación: {e}")

        return response

    def DownloadFileDataNodeClient(self, request, context):
        response = pb2.DownloadFileDataNodeResponse()  # Suponiendo que tienes una respuesta definida

        try:
            # Construir la ruta del archivo a descargar
            file_path = os.path.join(self.resources_path, f'bloque_{request.block_id}.txt')

            # Verificar si el archivo existe
            if not os.path.isfile(file_path):
                context.set_code(grpc.StatusCode.NOT_FOUND)
                context.set_details(f"El archivo {file_path} no existe.")
                response.estado_exitoso = False
                return response

            # Leer el contenido del archivo
            with open(file_path, 'rb') as f:
                response.file_content = f.read()  # Asignar el contenido del archivo a la respuesta
                response.estado_exitoso = True

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




    def DeleteFileDataNodeClient(self, request, context):
        response = pb2.DeleteFileDataNodeResponse()
        
        try:
            for ruta in request.lista_rutas_bloques_lider:
                os.remove(ruta)

            response.estado_exitoso = True
        except Exception as e:
            response.estado_exitoso = False
            print(f"Error eliminando archivos: {e}")

        return response

    def ReadFileDataNodeClient(self, request, context):
        response = pb2.ReadFileDataNodeResponse()
        
        for ruta in request.lista_rutas_bloques_seguidor:
            with open(ruta, 'rb') as f:
                response.lista_contenido_bloques_seguidor.append(f.read())

        response.estado_exitoso = True
        return response

    # Función para enviar el BlockReport al NameNode usando gRPC
    def enviar_block_report(self, servidor_namenode):
        ruta_json = os.getenv("DATABASE_PATH")
        
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
                
                # Imprimir las rutas de los bloques para depuración
                print("Rutas de bloques seguidor:", request.lista_rutas_bloques_seguidor)
                print("Rutas de bloques líder:", request.lista_rutas_bloques_lider)
                print("ID del DataNode:", request.id_data_node)
                print("Tipo de id_data_node:", type(request.id_data_node))
                
                # Enviar la solicitud y obtener la respuesta
                respuesta = stub.BlockReportNameNodeDataNode(request)
                if respuesta.estado_exitoso:
                    print("BlockReport enviado al NameNode exitosamente.")
                else:
                    print("Error al enviar el BlockReport al NameNode.")


    def RegisterToNameNode(self, datanode_register, namenode_ip, namenode_port, datanode_ip, datanode_port):
        # Verificar si el archivo datanodes_registry.json existe, si no lo crea
        registry_file_path = datanode_register
        try:
            if not os.path.exists(registry_file_path):
                print(f"Archivo {registry_file_path} no encontrado, creando archivo...")
                os.makedirs(os.path.dirname(registry_file_path), exist_ok=True)  # Crea el directorio si no existe
                with open(registry_file_path, 'w') as f:
                    json.dump([], f)  # Crear archivo vacío o con datos iniciales

            # Realizar la solicitud al NameNode
            with grpc.insecure_channel(f'{namenode_ip}:{namenode_port}') as channel:
                stub = pb2_grpc.FullServicesStub(channel)
                request = pb2.HandShakeDataNodeRequest(
                    data_node_ip=datanode_ip,
                    data_node_port=datanode_port
                )
                print("Petición hecha al NameNode")
                response = stub.HandShakeNameNodeDataNode(request)

                # Almacenar el ID del DataNode devuelto por el NameNode
                self.id_data_node = response.id_data_node
                
                #file_path = 'DataNode/datanodes_info/datanodes_registry.json'
                file_path =datanode_register

                # Verifica si el archivo existe, si no, lo crea con la estructura básica
                if not os.path.exists(file_path):
                    with open(file_path, 'w') as f:
                        # Inicializa el archivo con la estructura deseada
                        json.dump({"data_nodes": []}, f, indent=4)

                # Abre el archivo JSON (asegúrate de que exista o lo crea si no está disponible)
                with open(file_path, 'r+') as f:
                    try:
                        # Intenta cargar el contenido del archivo JSON
                        data = json.load(f)
                    except json.JSONDecodeError:
                        # Si el archivo está vacío o corrupto, inicializa la estructura
                        data = {"data_nodes": []}
                    
                    # Asegúrate de que existe la clave 'data_nodes' y que sea una lista
                    if 'data_nodes' not in data:
                        data['data_nodes'] = []

                    # Verifica si el DataNode ya está registrado en la lista 'data_nodes'
                    if not any(dn['id_data_node'] == self.id_data_node for dn in data['data_nodes']):
                        # Crea el nuevo DataNode en la estructura
                        new_datanode_info = {
                            "id_data_node": self.id_data_node,
                            "data_node_ip": datanode_ip,
                            "data_node_port": datanode_port
                        }

                        # Añade el nuevo DataNode a la lista
                        data['data_nodes'].append(new_datanode_info)

                        # Vuelve al principio del archivo para sobreescribirlo
                        f.seek(0)
                        json.dump(data, f, indent=4)
                        f.truncate()  # Elimina cualquier contenido anterior que sobre
                    else:
                        print("Este DataNode ya está registrado en el archivo.")

                # Imprimir el resultado del registro
                if response.estado_exitoso:
                    print(f'DataNode {self.id_data_node} registrado exitosamente en NameNode.')
                else:
                    print(f'Error al registrar DataNode {self.id_data_node} en NameNode.')

        except FileNotFoundError as e:
            print(f"Error: {e}. Verifica que el archivo o la ruta sean correctos.")
        except grpc.RpcError as e:
            print(f"Error en la comunicación gRPC: {e.details()}")
        except Exception as e:
            print(f"Ocurrió un error inesperado: {e}")


    def ConnectToDataNode(self, datanode_ip, datanode_port, bloques_a_replicar):
        try:
            with grpc.insecure_channel(f'{datanode_ip}:{datanode_port}') as channel:
                stub = pb2_grpc.FullServicesStub(channel)
                request = pb2.PipeLineDataNodeRequest(
                    contenido_bloques_lider=bloques_a_replicar,
                    id_bloque_lider=[response.id_data_node],  
                    id_bloque_seguidor=[response.id_data_node]
                )
                response = stub.PipeLineDataNodeResponseDataNodeRequest(request)

                if response.estado_exitoso:
                    print(f'Replicación exitosa de bloques hacia DataNode en {datanode_ip}:{datanode_port}')
                else:
                    print(f'Error en la replicación hacia DataNode en {datanode_ip}:{datanode_port}')
        except Exception as e:
            print(f'Error al conectar con DataNode {datanode_ip}:{datanode_port}: {e}')


def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    datanode_servicer = FullServicesServicer()
    generar_y_llenar_archivo_DB()
    # Registrar el DataNode al iniciar el servidor

    pb2_grpc.add_FullServicesServicer_to_server(datanode_servicer, server)

    print(f'Iniciando servidor DataNode en {datanode_ip}:{datanode_port}...')
    server.add_insecure_port(f'{datanode_ip}:{datanode_port}')
    server.start()
    datanode_servicer.RegisterToNameNode(datanode_register, namenode_ip, namenode_port, datanode_ip, datanode_port)
    datanode_servicer.enviar_block_report(f'{namenode_ip}:{namenode_port}')
    try:
        while True:
            time.sleep(86400)
    except KeyboardInterrupt:
        server.stop(0)

if __name__ == '__main__':
    serve()