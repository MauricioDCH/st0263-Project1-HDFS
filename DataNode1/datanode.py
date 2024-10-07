import grpc
from concurrent import futures
import time
import json
import os
import sys
from dotenv import load_dotenv

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import protos.hdfs_pb2_grpc as pb2_grpc
import protos.hdfs_pb2 as pb2

project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
load_dotenv(dotenv_path="./configs/.env.datanode")  # Para configuración de NameNode
load_dotenv(dotenv_path="./configs/.env.namenode")  # Para configuración de NameNode


nextnode_ip = os.getenv("NEXT_NODE_IP")
nextnode_port = os.getenv("NEXT_NODE_PORT")

datanode_ip = nextnode_ip
datanode_port = int(nextnode_port)

datanode_register = os.getenv("NEXT_DATANODES_REGISTRY")
leader_resources = os.getenv("NEXT_LEADER_RESOURCES")
follower_resources = os.getenv("NEXT_FOLLOWER_RESOURCES")
database_path = os.getenv("NEXT_DATABASE_PATH")

namenode_ip = os.getenv("NAMENODE_IP")
namenode_port = os.getenv("NAMENODE_PORT")

class FullServicesServicer(pb2_grpc.FullServicesServicer):

    def _init_(self,  resources_path):
        self.id_data_node = None
        self.resources_path = resources_path
        self.metadata_file = os.path.join('database_datanode', 'DB_DataNode.json')

        if not os.path.exists(self.metadata_file):
            os.makedirs(os.path.join(resources_path, 'database_datanode'), exist_ok=True)
            with open(self.metadata_file, 'w') as f:
                json.dump({}, f)

    def UploadFileDataNodeClient(self, request, context):
        response = pb2.UploadFileDataNodeResponse()

        try:
            blocks = []
            for idx, bloque in enumerate(request.lista_contenido_bloques_lider):
                file_path = os.path.join(self.resources_path, f'bloque_{request.lista_id_data_node_lider[idx]}.bin')
                os.makedirs(os.path.dirname(file_path), exist_ok=True) 
                with open(file_path, 'wb') as f:
                    f.write(bloque)
                
                blocks.append(bloque)

            self.ConnectToDataNode(nextnode_ip, nextnode_port, blocks)

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
        response = pb2.DownloadFileDataNodeResponse()
        
        for ruta in request.rutas_bloques_seguidor:
            with open(ruta, 'rb') as f:
                response.lista_contenido_bloques_seguidor.append(f.read())

        response.estado_exitoso = True
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

    def BlockReportNameNodeDataNode(self, request, context):
        response = pb2.BlockReportNameNodeResponse()

        try:
            with open(self.metadata_file, 'r+') as f:
                data = json.load(f)
                for idx, ruta in enumerate(request.lista_rutas_bloques_lider):
                    data[ruta] = request.json_diccionario_metadatos_bloques_lider[idx]
                f.seek(0)
                json.dump(data, f)
            
            response.estado_exitoso = True
        except Exception as e:
            response.estado_exitoso = False
            print(f"Error actualizando metadatos: {e}")

        return response


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
    datanode_servicer = FullServicesServicer(
        #id_data_node=1,
        #resources_path=leader_resources
    )

    # Registrar el DataNode al iniciar el servidor

    pb2_grpc.add_FullServicesServicer_to_server(datanode_servicer, server)

    print(f'Iniciando servidor DataNode en {datanode_ip}:{datanode_port}...')
    server.add_insecure_port(f'{datanode_ip}:{datanode_port}')
    server.start()
    datanode_servicer.RegisterToNameNode(datanode_register, namenode_ip, namenode_port, datanode_ip, datanode_port)
    try:
        while True:
            time.sleep(86400)
    except KeyboardInterrupt:
        server.stop(0)

if __name__ == '__main__':
    serve()