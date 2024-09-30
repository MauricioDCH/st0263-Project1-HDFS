import grpc

import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import protos.hdfs_pb2 as hdfs_pb2
import protos.hdfs_pb2_grpc as hdfs_pb2_grpc
from dotenv import load_dotenv
load_dotenv(dotenv_path="./configs/.env.datanode")  # Para DataNode
load_dotenv(dotenv_path="./configs/.env.namenode")  # Para NameNode


datanode_ip = os.getenv("DATANODE_IP")
datanode_port = int(os.getenv("DATANODE_PORT")) 
leader_resources = os.getenv("LEADER_RESOURCES")
follower_resources = os.getenv("FOLLOWER_RESOURCES")
database_path = os.getenv("DATABASE_PATH")

namenode_ip = os.getenv("NAMENODE_IP")
namenode_port = os.getenv("NAMENODE_PORT")


def run():
    # Crea un canal de comunicación
    with grpc.insecure_channel(f'{namenode_ip}:{namenode_port}') as channel:
        stub = hdfs_pb2_grpc.FullServicesStub(channel)
        
        # Crea una solicitud HandShake
        request = hdfs_pb2.HandShakeDataNodeRequest()
        request.data_node_ip = datanode_ip  # IP del DataNode
        request.data_node_port = datanode_port  # Puerto del DataNode

        # Llama al servicio HandShake
        response = stub.HandShakeNameNodeDataNode(request)
        
        # Procesa la respuesta
        print(f"ID del DataNode: {response.id_data_node}, Estado exitoso: {response.estado_exitoso}")

if __name__ == '__main__':
    run()
