import grpc
import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
import protos.hdfs_pb2 as hdfs_pb2
import protos.hdfs_pb2_grpc as hdfs_pb2_grpc
from dotenv import load_dotenv
from split_merge_methods import split_file

load_dotenv(dotenv_path="./configs/.env.client")  # Para cliente

# Acceder a las variables
client_ip = os.getenv("CLIENT_IP")
client_port = os.getenv("CLIENT_PORT")
upload_dir = os.getenv("UPLOAD_DIR")
download_dir = os.getenv("DOWNLOAD_DIR")

print(f"Client IP: {client_ip}")
print(f"Upload Directory: {upload_dir}")


DIRECTORIO_ARCHIVOS =  os.path.abspath(upload_dir)
# Ejemplo de uso
file_name = 'datos.txt'  # Cambia esto por el nombre del archivo que deseas procesar
file_path = os.path.join(DIRECTORIO_ARCHIVOS, file_name)  # Ruta al archivo que contiene los datos
n = 10  # Número de partes en las que deseas dividir el archivo

resultado = split_file(file_path, n)

print(f"Nombre del archivo original: {resultado['nombre_archivo']}")
print(f"Contenido de bloques creados: {resultado['lista_contenido_bloques']}")
print(f"Tamaño de bloques creados: {len(resultado['lista_contenido_bloques'])}")
