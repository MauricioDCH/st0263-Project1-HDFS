import grpc
import sys
import os
from dotenv import load_dotenv
from Cliente.split_merge_methods import split_file, merge_file

# Añade la ruta a los módulos generados por gRPC del archivo .proto
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import protos.hdfs_pb2 as hdfs_pb2
import protos.hdfs_pb2_grpc as hdfs_pb2_grpc

# Cargar las configuraciones desde los archivos .env
load_dotenv(dotenv_path="./configs/.env.client")  # Para configuración del cliente
load_dotenv(dotenv_path="./configs/.env.datanode")  # Para configuración de DataNode
load_dotenv(dotenv_path="./configs/.env.namenode")  # Para configuración de NameNode

# Obtener las variables de entorno
client_ip = os.getenv("CLIENT_IP")
client_port = int(os.getenv("CLIENT_PORT"))

datanode_ip = os.getenv("DATANODE_IP_1")
datanode_port = int(os.getenv("DATANODE_PORT_1"))

namenode_ip = os.getenv("NAMENODE_IP_1")
namenode_port = int(os.getenv("NAMENODE_PORT_1"))  # Asegúrate de que sea int si es necesario

class Grpc_client:
    def __init__(self, name_node_url=None, data_node_url=None):
        """
        Constructor principal que inicializa el cliente para conectar ya sea con NameNode o DataNode.
        Solo puede establecerse una conexión a la vez.
        """
        if name_node_url and data_node_url:
            raise ValueError("No se puede conectar a NameNode y DataNode al mismo tiempo. Elija uno.")
        
        self.name_node_stub = None
        self.data_node_stub = None
        
        if name_node_url:
            # Establecer una conexión insegura gRPC con el NameNode
            self.name_node_channel = grpc.insecure_channel(name_node_url)
            # Crear el stub para comunicarse con el NameNode
            self.name_node_stub = hdfs_pb2_grpc.FullServicesStub(self.name_node_channel)
        
        elif data_node_url:
            # Establecer una conexión insegura gRPC con el DataNode
            self.data_node_channel = grpc.insecure_channel(data_node_url)
            # Crear el stub para comunicarse con el DataNode
            self.data_node_stub = hdfs_pb2_grpc.FullServicesStub(self.data_node_channel)
        
        else:
            raise ValueError("Debe proporcionar al menos una URL para NameNode o DataNode.")

    @classmethod
    def from_name_node(cls, name_node_ip, name_node_port):
        """
        Constructor auxiliar para inicializar el cliente y conectar con el NameNode
        """
        name_node_url = f'{name_node_ip}:{name_node_port}'
        return cls(name_node_url=name_node_url)

    @classmethod
    def from_data_node(cls, data_node_url):
        """
        Constructor auxiliar para inicializar el cliente y conectar con el DataNode
        """
        data_node_url = f'{data_node_url}'
        return cls(data_node_url=data_node_url)


    def DataNodeDesignationNameNodeClient(self, nombre_archivo, tamano_archivo, nombre_usuario, url_cliente, replicas):
        """
        Método para solicitar la designación de DataNodes desde el NameNode.
        """
        # Crear la solicitud con los parámetros proporcionados
        request = hdfs_pb2.DataNodeDesignationClientRequest(
            nombre_archivo=nombre_archivo,
            tamano_archivo=tamano_archivo,
            nombre_usuario=nombre_usuario,
            url_cliente=url_cliente,
            numero_de_replicas_por_bloque=replicas
        )
        # Enviar la solicitud al NameNode y recibir la respuesta
        response_inicial = self.name_node_stub.DataNodeDesignationNameNodeClient(request)
        
        # Devolver las listas directamente como Python
        id_data_node_lider = list(response_inicial.lista_id_data_node_lider)
        id_data_node_seguidor = list(response_inicial.lista_id_data_node_seguidor)
        url_data_node_lider = list(response_inicial.lista_url_data_node_lider)
        url_data_node_seguidor = list(response_inicial.lista_url_data_node_seguidor)

        response = {
            "id_data_node_lider": id_data_node_lider,
            "id_data_node_seguidor": id_data_node_seguidor,
            "url_data_node_lider": url_data_node_lider,
            "url_data_node_seguidor": url_data_node_seguidor
        }
        return response  # Devuelve la respuesta con los DataNodes designados

    def FileLocationNameNodeClient(self, nombre_archivo, nombre_usuario, url_cliente):
        """
        Método para obtener la ubicación de un archivo desde el NameNode.
        """
        request = hdfs_pb2.FileLocationClientRequest(
            nombre_archivo=nombre_archivo,
            nombre_usuario=nombre_usuario,
            url_cliente=url_cliente
        )
        # Enviar la solicitud al NameNode
        response_inicial = self.name_node_stub.FileLocationNameNodeClient(request)
        
        lista_id_data_node_lider= list(response_inicial.lista_id_data_node_lider)
        lista_id_data_node_seguidor= list(response_inicial.lista_id_data_node_seguidor)
        lista_url_data_node_lider= list(response_inicial.lista_url_data_node_lider)
        lista_url_data_node_seguidor= list(response_inicial.lista_url_data_node_seguidor)
        lista_rutas_bloques_lider= list(response_inicial.lista_rutas_bloques_lider)
        lista_rutas_bloques_seguidor= list(response_inicial.lista_rutas_bloques_seguidor)
        
        response = {
            "id_data_node_lider": lista_id_data_node_lider,
            "id_data_node_seguidor": lista_id_data_node_seguidor,
            "url_data_node_lider": lista_url_data_node_lider,
            "url_data_node_seguidor": lista_url_data_node_seguidor,
            "rutas_bloques_lider": lista_rutas_bloques_lider,
            "rutas_bloques_seguidor": lista_rutas_bloques_seguidor
        }     
        return response  # Devuelve la respuesta completa con la ubicación del archivo

    def UploadFileDataNodeClient(self, nombre_archivo, nombre_usuario, url_cliente, lista_contenido_bloques_lider, lista_id_data_node_lider, lista_id_data_node_seguidor, lista_url_data_node_lider, lista_url_data_node_seguidor):
        # Creación del request
        request = hdfs_pb2.UploadFileClientRequest(
            nombre_archivo=nombre_archivo,
            nombre_usuario=nombre_usuario,
            url_cliente=url_cliente,
            lista_contenido_bloques_lider=lista_contenido_bloques_lider,
            lista_id_data_node_lider=lista_id_data_node_lider,
            lista_id_data_node_seguidor=lista_id_data_node_seguidor,
            lista_url_data_node_lider=lista_url_data_node_lider,
            lista_url_data_node_seguidor=lista_url_data_node_seguidor
        )

        # Llamar al stub con el request
        response_update = self.data_node_stub.UploadFileDataNodeClient(request)
        
        # Manejar la respuesta
        if response_update.estado_exitoso:
            response = f"La carga del archivo \'{nombre_archivo}\' fue exitosa."
        else:
            response = f"La carga del archivo \'{nombre_archivo}\' falló."
        
        return response  # Devuelve si la subida fue exitosa


        
    def DownloadFileDataNodeClient(self, nombre_archivo, nombre_usuario, url_cliente, lista_id_data_node_seguidor, rutas_bloques_seguidor):
        """
        Método para descargar un archivo desde los DataNodes.
        """
        request = hdfs_pb2.DownloadFileClientRequest(
            nombre_archivo=nombre_archivo,
            nombre_usuario=nombre_usuario,
            url_cliente=url_cliente,
            lista_id_data_node_seguidor=lista_id_data_node_seguidor,
            rutas_bloques_seguidor=rutas_bloques_seguidor
        )
        # Enviar la solicitud al DataNode para la descarga
        response_inicial = self.data_node_stub.DownloadFileDataNodeClient(request)
        
        response = {
            "contenido_bloques_seguidor": list(response_inicial.lista_contenido_bloques_seguidor),
            "estado_exitoso": response_inicial.estado_exitoso
        }
        
        return response  # Devuelve la respuesta con la información de la descarga

    def ReadFileDataNodeClient(self, nombre_archivo, nombre_usuario, url_cliente, id_lideres, id_seguidores, url_lideres, url_seguidores, rutas_lideres, rutas_seguidores):
        """
        Método para leer un archivo desde los DataNodes.
        """
        request = hdfs_pb2.ReadFileClientRequest(
            nombre_archivo=nombre_archivo,
            nombre_usuario=nombre_usuario,
            url_cliente=url_cliente,
            lista_id_bloque_lider=id_lideres,
            lista_id_bloque_seguidor=id_seguidores,
            lista_url_data_node_lider=url_lideres,
            lista_url_data_node_seguidor=url_seguidores,
            lista_rutas_bloques_lider=rutas_lideres,
            lista_rutas_bloques_seguidor=rutas_seguidores
        )
        # Enviar la solicitud al DataNode
        response_inicial = self.data_node_stub.ReadFileDataNodeClient(request)
        
        response = {
            "contenido_bloques_seguidor": list(response_inicial.lista_contenido_bloques_seguidor),
            "estado_exitoso": response_inicial.estado_exitoso
        }
        return response  # Devuelve la respuesta con el contenido leído

    def DeleteFileDataNodeClient(self, nombre_archivo, nombre_usuario, url_cliente, id_lideres, id_seguidores, url_lideres, url_seguidores, rutas_lideres, rutas_seguidores):
        """
        Método para eliminar un archivo en los DataNodes.
        """
        request = hdfs_pb2.DeleteFileClientRequest(
            nombre_archivo=nombre_archivo,
            nombre_usuario=nombre_usuario,
            url_cliente=url_cliente,
            lista_id_bloque_lider=id_lideres,
            lista_id_bloque_seguidor=id_seguidores,
            lista_url_data_node_lider=url_lideres,
            lista_url_data_node_seguidor=url_seguidores,
            lista_rutas_bloques_lider=rutas_lideres,
            lista_rutas_bloques_seguidor=rutas_seguidores
        )
        # Enviar la solicitud para eliminar el archivo
        response = self.data_node_stub.DeleteFileDataNodeClient(request)
        return response.estado_exitoso  # Devuelve si la eliminación fue exitosa

# ------------------------------------------------------------------------------------------------------------------------------------
# ------------------------------------------------------------------------------------------------------------------------------------
# ------------------------------------------------------------------------------------------------------------------------------------
# ------------------------------------------------------------------------------------------------------------------------------------
# ------------------------------------------------------------------------------------------------------------------------------------
# ------------------------------------------------------------------------------------------------------------------------------------

class FileSystemClient:
    def __init__(self, namenode_ip, namenode_port, client_ip, client_port):
        self.namenode_ip = namenode_ip
        self.namenode_port = namenode_port
        self.client_ip = client_ip
        self.client_port = client_port
    # --------------------------------------------------------------------------------------------------------------------------------
    def designate_data_nodes(self, nombre_archivo, tamano_archivo, nombre_usuario, replicas):
        # Conectar al NameNode y designar DataNodes
        client = Grpc_client.from_name_node(self.namenode_ip, self.namenode_port)
        response = client.DataNodeDesignationNameNodeClient(
            nombre_archivo=nombre_archivo,
            tamano_archivo=tamano_archivo,
            nombre_usuario=nombre_usuario,
            url_cliente=f"{self.client_ip}:{self.client_port}",
            replicas=replicas
        )
        return response
    # --------------------------------------------------------------------------------------------------------------------------------
    def locate_file(self, nombre_archivo, nombre_usuario):
        # Conectar al NameNode y obtener la ubicación del archivo
        client = Grpc_client.from_name_node(self.namenode_ip, self.namenode_port)
        response = client.FileLocationNameNodeClient(
            nombre_archivo=nombre_archivo,
            nombre_usuario=nombre_usuario,
            url_cliente=f"{self.client_ip}:{self.client_port}"
        )
        return response
    # --------------------------------------------------------------------------------------------------------------------------------
    def upload_file(self, file_name, number_replications, first_data_node_url, nombre_usuario, response_data_node_designation):
        # Dividir archivo y subir a DataNodes
        
        upload_dir = os.getenv("UPLOAD_DIR")
        DIRECTORIO_ARCHIVOS =  os.path.abspath(upload_dir)
        file_path = os.path.join(DIRECTORIO_ARCHIVOS, file_name)

        resultado = split_file(file_path, number_replications)
        
        client = Grpc_client.from_data_node(first_data_node_url)
        response = client.UploadFileDataNodeClient(
            nombre_archivo=file_name,
            nombre_usuario=nombre_usuario,
            url_cliente=f"{self.client_ip}:{self.client_port}",
            lista_contenido_bloques_lider=resultado['lista_contenido_bloques'],
            lista_id_data_node_lider=response_data_node_designation['id_data_node_lider'],
            lista_id_data_node_seguidor=response_data_node_designation['id_data_node_seguidor'],
            lista_url_data_node_lider=response_data_node_designation['url_data_node_lider'],
            lista_url_data_node_seguidor=response_data_node_designation['url_data_node_seguidor']
        )
        return response
    # --------------------------------------------------------------------------------------------------------------------------------
    def download_file(self, first_data_node_url, nombre_archivo, nombre_usuario, response_file_location):
        # Descargar el archivo desde los DataNodes
        client = Grpc_client.from_data_node(first_data_node_url)
        response = client.DownloadFileDataNodeClient(
            nombre_archivo=nombre_archivo,
            nombre_usuario=nombre_usuario,
            url_cliente=f"{self.client_ip}:{self.client_port}",
            lista_id_data_node_seguidor = response_file_location['id_data_node_seguidor'],
            rutas_bloques_seguidor=response_file_location['rutas_bloques_seguidor']
        )
        print(response)
        merge_file(nombre_archivo, response["contenido_bloques_seguidor"])
        return response
    # --------------------------------------------------------------------------------------------------------------------------------
    def read_file(self, first_data_node_url, nombre_archivo, nombre_usuario, response_file_location):
        # Leer archivo desde los DataNodes
        client = Grpc_client.from_data_node(first_data_node_url)
        response = client.ReadFileDataNodeClient(
            nombre_archivo=nombre_archivo,
            nombre_usuario=nombre_usuario,
            url_cliente=f"{self.client_ip}:{self.client_port}",
            id_lideres=response_file_location['id_data_node_lider'],
            id_seguidores=response_file_location['id_data_node_seguidor'],
            url_lideres=response_file_location['url_data_node_lider'],
            url_seguidores=response_file_location['url_data_node_seguidor'],
            rutas_lideres=response_file_location['rutas_bloques_lider'],
            rutas_seguidores=response_file_location['rutas_bloques_seguidor']
        )
        nombre_archivo_para_lectura = f"lectura_{nombre_archivo}"
        merge_file(nombre_archivo_para_lectura, response["contenido_bloques_seguidor"])
        return response
    # --------------------------------------------------------------------------------------------------------------------------------
    def delete_file(self, first_data_node_url, nombre_usuario, nombre_archivo, response_file_location):
        # Eliminar archivo desde los DataNodes
        client = Grpc_client.from_data_node(first_data_node_url)
        response = client.DeleteFileDataNodeClient(
            nombre_archivo=nombre_archivo,
            nombre_usuario=nombre_usuario,
            url_cliente=f"{self.client_ip}:{self.client_port}",
            id_lideres=response_file_location['id_data_node_lider'],
            id_seguidores=response_file_location['id_data_node_seguidor'],
            url_lideres=response_file_location['url_data_node_lider'],
            url_seguidores=response_file_location['url_data_node_seguidor'],
            rutas_lideres=response_file_location['rutas_bloques_lider'],
            rutas_seguidores=response_file_location['rutas_bloques_seguidor']
        )
        return response
