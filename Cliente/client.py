import getpass
import os
import sys
import signal  # Para capturar señales del sistema (como Ctrl + C)
import time
import threading
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from CLI import CLI, cli_commands
from autentication import Autenticacion, Menu, cerrar_programa, signal_handler
from gestor_archivos import GestorArchivos
from flask import Flask
from dotenv import load_dotenv

load_dotenv(dotenv_path="./configs/.env.client")  # Para cliente
load_dotenv(dotenv_path="./configs/.env.namenode")  # Para NameNode

# Acceder a las variables
client_ip = os.getenv("CLIENT_IP")
client_port = os.getenv("CLIENT_PORT")
upload_dir = os.getenv("UPLOAD_DIR")
resources_dir = os.getenv("RESOURCES_DIR_CLI")
catalog_file = os.getenv("CATALOG_FILE")
autentication_server_port = os.getenv("AUTHENTICATION_SERVER_PORT_1")

app = Flask(__name__)

# Función para guardar los metadatos en un archivo JSON cada 30 segundos
def guardar_metadata_en_json_repetictivamente():
    while True:
        gestor_archivos.guardar_metadata_en_json()
        time.sleep(30)

if __name__ == '__main__':
    
    autenticacion = Autenticacion()
    menu = Menu()
    cli = CLI()
    gestor_archivos = GestorArchivos(resources_dir, catalog_file)
    
    # Crear un hilo para guardar los metadatos en un archivo JSON cada 30 segundos
    thread_guardar_metadata = threading.Thread(target=guardar_metadata_en_json_repetictivamente)
    thread_guardar_metadata.daemon = True  # El hilo se cerrará cuando el programa principal termine
    thread_guardar_metadata.start()
    
    name_node_ip = os.getenv("NAMENODE_IP_1")
    name_node_port = autentication_server_port

    # Configurar el manejador de señales para Ctrl + C
    signal.signal(signal.SIGINT, lambda sig, frame: signal_handler(sig, frame, autenticacion, name_node_ip, name_node_port))

    while True:
        opcion = menu.menu_autenticacion()
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
