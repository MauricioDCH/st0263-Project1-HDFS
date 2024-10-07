import grpc
import sys
import os
import shutil
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from concurrent import futures
from dotenv import load_dotenv

# Cargar las configuraciones desde los archivos .env
load_dotenv(dotenv_path="./configs/.env.client")  # Para configuración del cliente
load_dotenv(dotenv_path="./configs/.env.datanode")  # Para configuración de DataNode
load_dotenv(dotenv_path="./configs/.env.namenode")  # Para configuración de NameNode

# Obtener las variables de entorno
client_ip = os.getenv("CLIENT_IP")
client_port = int(os.getenv("CLIENT_PORT"))

datanode_ip = os.getenv("DATANODE_IP")
datanode_port = int(os.getenv("DATANODE_PORT"))

namenode_ip = os.getenv("NAMENODE_IP")
namenode_port = int(os.getenv("NAMENODE_PORT"))  # Asegúrate de que sea int si es necesario



import protos.hdfs_pb2 as hdfs_pb2
import protos.hdfs_pb2_grpc as hdfs_pb2_grpc


class DataNodeService(hdfs_pb2_grpc.FullServicesServicer):

    def __init__(self):
        self.files = []  # Lista que almacena los nombres de los archivos

class DataNodeService(hdfs_pb2_grpc.FullServicesServicer):

    def __init__(self):
        self.files = []  # Lista que almacena los nombres de los archivos

    def UploadFileDataNodeClient(self, request, context):
        # Maneja la subida del archivo
        leader_resources = os.getenv("LEADER_RESOURCES")
        if not leader_resources:
            context.set_details("Error: LEADER_RESOURCES no está configurado.")
            context.set_code(grpc.StatusCode.INTERNAL)
            return hdfs_pb2.UploadFileDataNodeResponse(estado_exitoso=False)
        
        file_path = os.path.join(leader_resources, request.nombre_archivo)  # Especifica la ruta donde deseas guardar el archivo

        # Verifica si la carpeta destino existe, si no, la crea
        os.makedirs(os.path.dirname(file_path), exist_ok=True)

        # Abre el archivo en modo de escritura binaria
        try:
            with open(file_path, 'wb') as f:
                for bloque in request.lista_contenido_bloques_lider:
                    f.write(bloque)  # Escribe el bloque en el archivo

            # Solo se almacena el nombre del archivo en la lista
            if request.nombre_archivo not in self.files:
                self.files.append(request.nombre_archivo)
            
            response = hdfs_pb2.UploadFileDataNodeResponse(estado_exitoso=True)
        except Exception as e:
            context.set_details(f"Error al guardar el archivo: {str(e)}")
            context.set_code(grpc.StatusCode.INTERNAL)
            response = hdfs_pb2.UploadFileDataNodeResponse(estado_exitoso=False)

        return response


    def DownloadFileDataNodeClient(self, request, context):
        # Maneja la descarga del archivo
        contenido_bloques = [b"xd", b"xd", b"\nxd"]
        response = hdfs_pb2.DownloadFileDataNodeResponse(
            lista_contenido_bloques_seguidor=contenido_bloques,
            estado_exitoso=True
        )
        return response

    def ReadFileDataNodeClient(self, request, context):
        # Lee un archivo almacenado en el DataNode
        contenido_bloques = [b"xd", b"xd", b"\nxd"]
        response = hdfs_pb2.ReadFileDataNodeResponse(
            lista_contenido_bloques_seguidor=contenido_bloques,
            estado_exitoso=True
        )
        return response

    def DeleteFileDataNodeClient(self, request, context):
        leader_resources = os.getenv("LEADER_RESOURCES")
        if not leader_resources:
            context.set_details("Error: LEADER_RESOURCES no está configurado.")
            context.set_code(grpc.StatusCode.INTERNAL)
            return hdfs_pb2.DeleteFileDataNodeResponse(estado_exitoso=False)

        file_path = os.path.join(leader_resources, request.nombre_archivo)
        print(f"Intentando eliminar: {request.nombre_archivo}")  # Para depuración
        print(f"Intentando eliminar: {file_path}")  # Para depuración

        try:
            if os.path.exists(file_path):
                if os.path.isfile(file_path):  # Verifica si es un archivo
                    os.remove(file_path)  # Elimina el archivo
                    self.files.remove(request.nombre_archivo)  # Elimina el nombre del archivo de la lista
                    response = hdfs_pb2.DeleteFileDataNodeResponse(estado_exitoso=True)
                elif os.path.isdir(file_path):  # Si es un directorio
                    shutil.rmtree(file_path)  # Elimina el directorio y su contenido
                    self.files.remove(request.nombre_archivo)  # Elimina el nombre del directorio de la lista
                    response = hdfs_pb2.DeleteFileDataNodeResponse(estado_exitoso=True)
                else:
                    context.set_details("Error: El tipo de recurso no es válido.")
                    context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
                    response = hdfs_pb2.DeleteFileDataNodeResponse(estado_exitoso=False)
            else:
                context.set_details("Error: El archivo o directorio no existe.")
                context.set_code(grpc.StatusCode.NOT_FOUND)
                response = hdfs_pb2.DeleteFileDataNodeResponse(estado_exitoso=False)

        except Exception as e:
            context.set_details(f"Error al eliminar: {str(e)}")
            context.set_code(grpc.StatusCode.INTERNAL)
            response = hdfs_pb2.DeleteFileDataNodeResponse(estado_exitoso=False)

        return response

# Inicia el servidor DataNode
def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    hdfs_pb2_grpc.add_FullServicesServicer_to_server(DataNodeService(), server)
    server.add_insecure_port(f'[::]:{datanode_port}')
    server.start()
    print(f"DataNode de HDFS escuchando en el puerto {datanode_port}...")    
    server.wait_for_termination()

if __name__ == '__main__':
    serve()
