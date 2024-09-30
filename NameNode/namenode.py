import grpc
import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
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



class FullServicesServicer(hdfs_pb2_grpc.FullServicesServicer):
    def HandShakeNameNodeDataNode(self, request, context):
        # Procesa la solicitud de handshake
        print(f"Recibido HandShake de DataNode: IP {request.data_node_ip}, Puerto {request.data_node_port}")
        
        # Respuesta ficticia
        response = hdfs_pb2.HandShakeNameNodeResponse()
        response.id_data_node = 1  # ID ficticio del DataNode
        response.estado_exitoso = True  # Simula que la conexión fue exitosa
        
        return response

def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    hdfs_pb2_grpc.add_FullServicesServicer_to_server(FullServicesServicer(), server)
    
    server.add_insecure_port(f'[::]:{namenode_port}')  # Puerto del servidor
    server.start()
    print(f"Servidor de HDFS escuchando en el puerto {namenode_port}...")
    server.wait_for_termination()

if __name__ == '__main__':
    serve()
