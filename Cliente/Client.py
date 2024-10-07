import getpass
import os
import sys
import signal  # Para capturar señales del sistema (como Ctrl + C)

#import grpc
#import protos.hdfs_pb2 as hdfs_pb2
#import protos.hdfs_pb2_grpc as hdfs_pb2_grpc

from CLI import CLI, cli_commands
from Autentication import Autenticacion, Menues, cerrar_programa, signal_handler
from flask import Flask
from dotenv import load_dotenv

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
load_dotenv(dotenv_path="./configs/.env.client")  # Para cliente
load_dotenv(dotenv_path="./configs/.env.namenode")  # Para NameNode

# Acceder a las variables
client_ip = os.getenv("CLIENT_IP")
client_port = os.getenv("CLIENT_PORT")
upload_dir = os.getenv("UPLOAD_DIR")

app = Flask(__name__)

if __name__ == '__main__':
    
    autenticacion = Autenticacion()
    menues = Menues()
    cli = CLI()
    
    name_node_ip = os.getenv("NAMENODE_IP")
    #name_node_port = os.getenv("NAMENODE_PORT")
    name_node_port = 5000

    # Configurar el manejador de señales para Ctrl + C
    signal.signal(signal.SIGINT, lambda sig, frame: signal_handler(sig, frame, autenticacion, name_node_ip, name_node_port))

    while True:
        opcion = menues.menu_autenticacion()
        if opcion == '1':
            # Registro de cliente
            username = input("Ingrese el nombre de usuario: ")
            password = getpass.getpass("Ingrese la contraseña: ")
            confirmation_password = getpass.getpass("Confirme la contraseña: ")
                        
            response_text, status_code = autenticacion.register_cliente(username, password, confirmation_password, name_node_ip, name_node_port)
            print(f"Response: {response_text}")

        elif opcion == '2':
            # Login de cliente
            username = input("Ingrese el nombre de usuario: ")
            password = getpass.getpass("Ingrese la contraseña: ")
            
            response_text, status_code = autenticacion.login_cliente(username, password, name_node_ip, name_node_port)
            print(f"Response: {response_text}")

            if autenticacion.is_logged_in:
                cli_commands(username)

        elif opcion == '3':
            # Logout de cliente
            response_text, status_code = autenticacion.logout_cliente(name_node_ip, name_node_port)
            print(f"Response: {response_text}")

        elif opcion == '4':
            # Eliminar cuenta
            password = getpass.getpass("Ingrese la contraseña: ")
            response_text, status_code = autenticacion.unregister_cliente(password, name_node_ip, name_node_port)
            print(f"Response: {response_text}")

        elif opcion == '0':
            cerrar_programa(autenticacion, name_node_ip, name_node_port)
        else:
            print("Opción inválida.")
































"""
DIRECTORIO_ARCHIVOS =  os.path.abspath(upload_dir)
# Ejemplo de uso
file_name = 'datos.txt'  # Cambia esto por el nombre del archivo que deseas procesar
file_path = os.path.join(DIRECTORIO_ARCHIVOS, file_name)  # Ruta al archivo que contiene los datos
n = 10  # Número de partes en las que deseas dividir el archivo

resultado = split_file(file_path, n)

print(f"Nombre del archivo original: {resultado['nombre_archivo']}")
print(f"Contenido de bloques creados: {resultado['lista_contenido_bloques']}")
print(f"Tamaño de bloques creados: {len(resultado['lista_contenido_bloques'])}")

merge_file(resultado['nombre_archivo'], resultado['lista_contenido_bloques'])

"""