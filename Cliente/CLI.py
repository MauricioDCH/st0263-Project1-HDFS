import os
import requests
import shutil
import grpc
from grpc_client import FileSystemClient
from Cliente.split_merge_methods import merge_file
from dotenv import load_dotenv
load_dotenv(dotenv_path="./configs/.env.client")  # Para configuración del cliente
load_dotenv(dotenv_path="./configs/.env.datanode")  # Para configuración de DataNode
load_dotenv(dotenv_path="./configs/.env.namenode")  # Para configuración de NameNode

# Obtener las variables de entorno
client_ip = os.getenv("CLIENT_IP")
client_port = int(os.getenv("CLIENT_PORT"))
uploadable_files = os.getenv("UPLOAD_DIR")
downloadable_files = os.getenv("DOWNLOAD_DIR")

datanode_ip = os.getenv("DATANODE_IP_1")
datanode_port = int(os.getenv("DATANODE_PORT_1"))

namenode_ip = os.getenv("NAMENODE_IP_1")
namenode_port = int(os.getenv("NAMENODE_PORT_1"))  # Asegúrate de que sea int si es necesario

class CLI:
    def __init__(self):
        pass     

    def obtener_ip_global(self):
        try:
            response = requests.get('https://api.ipify.org?format=json')
            response.raise_for_status()
            ip_address = response.json()['ip']
            return ip_address
        except requests.RequestException as e:
            print(f"Error al obtener la IP global: {e}")
            return "IP no disponible"
    
    def listar_archivos_y_carpetas(self, ruta_base, ruta_relativa):
        """Función que lista los archivos y carpetas desde 'uploadable_files'."""
        ruta_completa = os.path.join(ruta_base, ruta_relativa)  # Ruta absoluta

        try:
            elementos = os.listdir(ruta_completa)  # Listar archivos y carpetas
            #print(f"\nContenido de la carpeta: {ruta_relativa or '.'}\n")  # Mostrar solo la ruta relativa
            for elemento in elementos:
                ruta_elemento = os.path.join(ruta_completa, elemento)
                if os.path.isdir(ruta_elemento):
                    print(f"\t{elemento}/")
                else:
                    print(f"\t{elemento}")
        except FileNotFoundError:
            print(f"Error: La ruta '{ruta_relativa}' no existe.")
        except PermissionError:
            print(f"Error: No tienes permiso para acceder a la carpeta '{ruta_relativa}'.")

    def validar_ruta(self, ruta_base, ruta_actual, nueva_ruta):
        """Función que valida que la nueva ruta no sobrepase la carpeta base 'uploadable_files'."""
        ruta_completa = os.path.abspath(os.path.join(ruta_base, ruta_actual, nueva_ruta))
        if ruta_completa.startswith(ruta_base):
            return os.path.normpath(os.path.join(ruta_actual, nueva_ruta))
        else:
            print(f"Error: No puedes navegar más allá de la carpeta base 'uploadable_files'")
            return ruta_actual

    def cargar_configuraciones(self):
        """Carga las configuraciones desde los archivos .env."""
        load_dotenv(dotenv_path="./configs/.env.client")  # Para configuración del cliente
        load_dotenv(dotenv_path="./configs/.env.namenode")  # Para configuración de NameNode

        client_ip = os.getenv("CLIENT_IP")
        client_port = int(os.getenv("CLIENT_PORT"))
        namenode_ip = os.getenv("NAMENODE_IP_1")
        namenode_port = int(os.getenv("NAMENODE_PORT_1"))  # Asegúrate de que sea int si es necesario

        return namenode_ip, namenode_port, client_ip, client_port

    def crear_cliente(self, namenode_ip, namenode_port, client_ip, client_port):
        """Crea una instancia del cliente del sistema de archivos."""
        return FileSystemClient(namenode_ip, namenode_port, client_ip, client_port)

    def comando_invalido(self):
        """Función que muestra un mensaje de error por comando inválido."""
        print("Comando inválido. Intenta de nuevo.")

    def comando_help(self):
        """Función que muestra los comandos disponibles."""
        print("\nComandos disponibles:")
        print(" - ls [nombre_carpeta]: Listar el contenido de una carpeta de manera local.")
        print(" - cd [nombre_carpeta]: Cambiar a una carpeta dentro de la ruta actual de manera local.")
        print(" - cd ..: Subir hasta al directorio base de manera local.")
        print(" - mkdir [nombre_carpeta]: Crear una carpeta vacía de manera local.")
        print(" - touch [nombre_archivo]: Crear un archivo vacío de manera local.")
        print(" - nano [nombre_archivo]: Editar un archivo de manera local.")
        print(" - cp [nombre_archivo_origen] [nombre_archivo_destino]: Copiar un archivo de manera local.")
        print(" - mv [nombre_archivo_origen] [nombre_archivo_destino]: Mover un archivo de manera local.")
        print(" - get [nombre_archivo]: Descargar un archivo en el sistema HDFS.")
        print(" - put [nombre_archivo]: Subir un archivo en el sistema HDFS.")
        print(" - read [nombre_archivo]: Leer un archivo en el sistema HDFS.")
        print(" - delete [nombre_archivo]: Eliminar un archivo en el sistema HDFS.")
        print(" - rm [nombre_archivo]: Eliminar un archivo de manera local.")
        print(" - rm -r [nombre_carpeta]: Eliminar una carpeta y su contenido de manera local.")
        print(" - clear: Limpiar la consola de manera local.")
        print(" - exit: Salir del programa de manera local.\n")

    def comando_clear(self):
        """Función que limpia la consola."""
        os.system('cls' if os.name == 'nt' else 'clear')

    def comando_ls(self, comando, ruta_base, ruta_actual):
        if len(comando) > 1:
            nueva_ruta = self.validar_ruta(ruta_base, ruta_actual, comando[1])
            self.listar_archivos_y_carpetas(ruta_base, nueva_ruta)
        else:
            self.listar_archivos_y_carpetas(ruta_base, ruta_actual)

    def comandos_cd(self, comando, ruta_base, ruta_actual):
        # Verificar si se pasó una ruta después del comando cd
        if len(comando) > 1:
            # Manejar comandos para subir múltiples niveles (e.g., cd ../../..)
            if comando[1].startswith(".."):
                partes = comando[1].split("/")  # Dividir el comando para manejar múltiples niveles
                niveles_subir = sum(1 for parte in partes if parte == "..")  # Contar cuántos niveles se quiere subir

                # Subir solo hasta el directorio base 'uploadable_files'
                nueva_ruta = ruta_actual
                for _ in range(niveles_subir):
                    if nueva_ruta != "":  # Solo cambiar si no estamos ya en la carpeta base
                        nueva_ruta = os.path.dirname(nueva_ruta)  # Subir un nivel

                # Validar si no se pasó del directorio base
                ruta_completa = os.path.abspath(os.path.join(ruta_base, nueva_ruta))
                if not ruta_completa.startswith(ruta_base):  # No puede retroceder más allá de 'uploadable_files'
                    print("Error: No puedes navegar más allá de la carpeta base 'uploadable_files'.")
                else:
                    ruta_actual = nueva_ruta  # Actualizar la ruta actual si es válida
            else:
                # Intentar cambiar a un directorio específico hacia adelante (cd nombre_carpeta)
                nueva_ruta = os.path.normpath(os.path.join(ruta_actual, comando[1]))
                ruta_completa = os.path.abspath(os.path.join(ruta_base, nueva_ruta))

                if os.path.isdir(ruta_completa):
                    ruta_actual = nueva_ruta  # Actualizar la ruta solo si la carpeta es válida
                else:
                    print(f"Error: '{comando[1]}' no es una carpeta válida.")
        else:
            print("Error: Debes proporcionar el nombre de una carpeta.")

        return ruta_actual  # Devolver la nueva ruta actualizada

    def comandos_rm(self, comando, ruta_actual):
        if len(comando) > 1:
            ruta_actual = os.path.abspath(uploadable_files)  # La ruta base que no se puede sobrepasar
            # Comprobar si el comando incluye la opción recursiva '-r'
            if comando[1] == '-r':
                # Se espera que la siguiente parte del comando sea la carpeta a eliminar
                if len(comando) > 2:
                    target = os.path.join(ruta_actual, comando[2])
                    # Verificar si el directorio existe
                    if os.path.exists(target) and os.path.isdir(target):
                        try:
                            shutil.rmtree(target)
                            print(f"Directorio '{comando[2]}' eliminado recursivamente.")
                        except Exception as e:
                            print(f"Error al eliminar directorio: {e}")
                    else:
                        print(f"Error: '{comando[2]}' no es un directorio válido o no existe.")
                else:
                    print("Error: Debes proporcionar un directorio para eliminar recursivamente.")
            else:
                # Si no hay bandera '-r', tratar como archivo o carpeta normal
                target = os.path.join(ruta_actual, comando[1])
                
                # Verificar si es un archivo o directorio
                if os.path.exists(target):
                    if os.path.isdir(target):
                        print(f"Error: '{comando[1]}' es un directorio. Usa '-r' para eliminar recursivamente.")
                    else:
                        # Eliminar archivo
                        try:
                            os.remove(target)
                            print(f"Archivo '{comando[1]}' eliminado.")
                        except Exception as e:
                            print(f"Error al eliminar archivo: {e}")
                else:
                    print(f"Error: El archivo o directorio '{comando[1]}' no existe.")
        else:
            print("Error: Debes proporcionar el nombre del archivo o carpeta a eliminar.")

    def comandos_touch(self, comando, ruta_actual):
        ruta_actual = os.path.abspath(uploadable_files)  # La ruta base que no se puede sobrepasar
        if len(comando) > 1:
            nuevo_archivo = os.path.join(ruta_actual, comando[1])
            try:
                with open(nuevo_archivo, 'a'):
                    os.utime(nuevo_archivo, None)  # Actualiza la fecha de acceso/modificación
                print(f"Archivo '{comando[1]}' creado o actualizado.")
            except Exception as e:
                print(f"Error al crear archivo: {e}")
        else:
            print("Error: Debes proporcionar el nombre del archivo a crear.")

    def comandos_mkdir(self, comando, ruta_actual):
        ruta_base = os.path.abspath(uploadable_files)  # La ruta base que no se puede sobrepasar
        ruta_actual = ruta_base  # Ruta actual que cambia con los comandos
        if len(comando) > 1:
            nueva_carpeta = os.path.join(ruta_actual, comando[1])
            try:
                os.makedirs(nueva_carpeta, exist_ok=True)
                print(f"Carpeta '{comando[1]}' creada con éxito.")
            except Exception as e:
                print(f"Error al crear carpeta: {e}")
        else:
            print("Error: Debes proporcionar el nombre de la carpeta a crear.")

    def comandos_nano(self, comando, ruta_actual):
        ruta_actual = os.path.abspath(uploadable_files)  # La ruta base que no se puede sobrepasar
        if len(comando) > 1:
            archivo = os.path.join(ruta_actual, comando[1])
            
            # Verificar si el archivo existe
            if os.path.exists(archivo):
                with open(archivo, 'r') as f:
                    contenido_actual = f.readlines()  # Leer todas las líneas en una lista
                    print(f"Contenido actual de '{comando[1]}':")
                    for linea in contenido_actual:
                        print(linea, end='')  # Imprimir sin añadir nuevas líneas
            else:
                print(f"Archivo '{comando[1]}' no existe. Creándolo...")

            # Mostrar el contenido actual y permitir que el usuario lo edite
            print("\nEscribe más líneas en el archivo.\nY para terminar, presiona Enter y teclea Ctrl+D para guardar los cambios.")

            # Comenzar con el contenido actual si el archivo existe
            contenido_nuevo = contenido_actual.copy() if os.path.exists(archivo) else []

            # Permitir la edición
            try:
                while True:
                    linea = input()  # Leer línea por línea
                    if linea.strip() == "":  # Permitir que el usuario presione una línea vacía para dejar de agregar
                        break
                    contenido_nuevo.append(linea)  # Agregar la línea ingresada
            except EOFError:
                pass  # Detectar Ctrl+D para terminar la edición

            # Revisar si el archivo actual tiene un salto de línea al final
            if contenido_actual and not contenido_actual[-1].endswith('\n'):
                # Guardar el nuevo contenido en el archivo, sobrescribiendo el anterior
                with open(archivo, 'w') as f:
                    f.write('\n'.join(contenido_nuevo)+'\n')
            else:
                with open(archivo, 'w') as f:
                        f.write(''.join(contenido_nuevo)+'\n')
            print(f"Archivo '{comando[1]}' actualizado.")
        else:
            print("Error: Debes proporcionar el nombre del archivo a editar.")


    def comandos_cp(self, comando, ruta_actual):
        if len(comando) < 3:
            print("Error: Debes proporcionar el origen y el destino.")
            return

        recursivo = False
        if comando[1] == "-r":
            recursivo = True
            comando.pop(1)  # Eliminar la flag -r para manejar el resto normalmente

        origen = os.path.join(ruta_actual, comando[1])
        destino = os.path.join(ruta_actual, comando[2])

        if not os.path.exists(origen):
            print(f"Error: El archivo o directorio '{comando[1]}' no existe.")
            return

        # Si es un archivo o directorio
        if os.path.isfile(origen):
            # Si es un archivo, copiarlo directamente
            shutil.copy2(origen, destino)
            print(f"Archivo '{comando[1]}' copiado a '{comando[2]}'.")
        elif os.path.isdir(origen):
            if recursivo:
                # Copiar recursivamente
                shutil.copytree(origen, destino)
                print(f"Directorio '{comando[1]}' copiado recursivamente a '{comando[2]}'.")
            else:
                print(f"Error: '{comando[1]}' es un directorio. Usa '-r' para copiar recursivamente.")
        else:
            print(f"Error: '{comando[1]}' no es un archivo ni un directorio válido.")


    def comandos_mv(self, comando, ruta_actual):
        if len(comando) < 3:
            print("Error: Debes proporcionar el origen y el destino.")
            return

        recursivo = False
        if comando[1] == "-r":
            recursivo = True
            comando.pop(1)  # Eliminar la flag -r

        origen = os.path.join(ruta_actual, comando[1])
        destino = os.path.join(ruta_actual, comando[2])

        if not os.path.exists(origen):
            print(f"Error: El archivo o directorio '{comando[1]}' no existe.")
            return

        # Si es un archivo o directorio
        if os.path.isfile(origen):
            # Si es un archivo, moverlo directamente
            shutil.move(origen, destino)
            print(f"Archivo '{comando[1]}' movido a '{comando[2]}'.")
        elif os.path.isdir(origen):
            if recursivo:
                # Mover el directorio recursivamente
                shutil.move(origen, destino)
                print(f"Directorio '{comando[1]}' movido recursivamente a '{comando[2]}'.")
            else:
                print(f"Error: '{comando[1]}' es un directorio. Usa '-r' para mover recursivamente.")
        else:
            print(f"Error: '{comando[1]}' no es un archivo ni un directorio válido.")


    
    def comando_get(self, username, comando):
        ruta_actual = os.path.abspath(downloadable_files)  # La ruta base que no se puede sobrepasar
        ruta_archivo = os.path.join(ruta_actual, comando[1])
        if len(comando) > 1:
            nombre_archivo = comando[1]
            try:
                configs = self.cargar_configuraciones()
                client = self.crear_cliente(configs[0], configs[1], configs[2], configs[3])

                nombre_usuario = username
                respuesta_localizacion = client.locate_file(nombre_archivo, nombre_usuario)
                if 'url_data_node_seguidor' in respuesta_localizacion and respuesta_localizacion['url_data_node_seguidor']:
                    first_data_node_url_seguidor = respuesta_localizacion['url_data_node_seguidor'][0]
                    respuesta_descarga = client.download_file(first_data_node_url_seguidor, nombre_archivo, nombre_usuario, respuesta_localizacion)
                    print(f"\n{respuesta_descarga}\n")
                    if respuesta_descarga['estado_exitoso']:
                        # Abrir el archivo en modo escritura (sobrescribiendo si ya existe)
                        contenido_archivo = b""
                        for i in range(len(respuesta_descarga["contenido_bloques_seguidor"]) - 1, -1, -1):
                            print(f'Contenido del bloque {i}: {respuesta_descarga["contenido_bloques_seguidor"][i]}')
                            contenido_archivo += respuesta_descarga["contenido_bloques_seguidor"][i]
                        print(f'Contenido del archivo: {contenido_archivo}')
                        with open(ruta_archivo, 'wb') as file:
                            file.write(contenido_archivo)
                        #print(f"\nArchivo '{nombre_archivo}' descargado y sobrescrito con éxito.")
                    else:
                        print(f"\nError al descargar el archivo '{nombre_archivo}'.")
                else:
                    print("Error: No se encontró la URL del DataNode seguidor.")
            
            except grpc.RpcError as e:
                print(f"Error RPC: {e.code()} - {e.details()}")
            except Exception as e:
                print(f"Error al descargar el archivo: {e}")
        else:
            print("Error: Debes proporcionar el nombre del archivo a descargar.")

    
    
    
    
    def comando_put(self, username, comando):
        ruta_actual = os.path.abspath(uploadable_files)  # La ruta base que no se puede sobrepasar
        if len(comando) > 1:
            nombre_archivo = comando[1]
            file_path = os.path.join(ruta_actual, nombre_archivo)
            try:
                configs = self.cargar_configuraciones()
                client = self.crear_cliente(configs[0], configs[1], configs[2], configs[3])
                
                # Aquí inserto el código de 'put'
                upload_dir = os.getenv("UPLOAD_DIR")
                DIRECTORIO_ARCHIVOS = os.path.abspath(upload_dir)
                file_path = os.path.join(DIRECTORIO_ARCHIVOS, nombre_archivo)  # Ruta al archivo que contiene los datos
                tamano_archivo = os.path.getsize(file_path)  # Obtener el tamaño del archivo
                nombre_usuario = username  # Nombre del usuario
                replicas = 3  # Número de réplicas a crear

                # Designar DataNodes
                print("Antes de designación de DataNodes")
                respuesta_designacion = client.designate_data_nodes(nombre_archivo, tamano_archivo, nombre_usuario, replicas)
                print(f"Después de designación de DataNodes")
                # Subir el archivo
                
                first_data_node_url_lider = respuesta_designacion['url_data_node_lider'][0]  # Obtener la URL del DataNode líder
                
                # Si el número de urls de DataNodes es menor a 3, se sube el archivo a los DataNodes disponibles
                # si no, se sube a los 3 DataNodes designados
                
                if len(respuesta_designacion['url_data_node_lider']) < 3:
                    replicas_data_node = len(respuesta_designacion['url_data_node_lider'])
                elif len(respuesta_designacion['url_data_node_lider']) >= 3:
                    replicas_data_node = 3
                
                print(f"first_data_node_url_lider: {first_data_node_url_lider}")
                respuesta_subida = client.upload_file(nombre_archivo, replicas_data_node, first_data_node_url_lider, nombre_usuario, respuesta_designacion)
                print(f"\n{respuesta_subida}\n")

            except Exception as e:
                print(f"Error al subir el archivo: {e}")
        else:
            print("Error: Debes proporcionar el nombre del archivo a subir.")
        
    
    
    
    def comando_read(self, username, comando):
        if len(comando) > 1:
            nombre_archivo = comando[1]
            try:
                configs = self.cargar_configuraciones()
                client = self.crear_cliente(configs[0], configs[1], configs[2], configs[3])

                # Aquí inserto el código de 'read'
                nombre_usuario = username
                respuesta_localizacion = client.locate_file(nombre_archivo, nombre_usuario)

                first_data_node_url_seguidor = respuesta_localizacion['url_data_node_seguidor'][0]  # Obtener la URL del DataNode seguidor
                respuesta_lectura = client.read_file(first_data_node_url_seguidor, nombre_archivo, nombre_usuario, respuesta_localizacion)

                download_dir = os.getenv("DOWNLOAD_DIR")
                nombre_archivo_para_lectura = f"lectura_{nombre_archivo}"
                ruta_archivo_descargado = os.path.join(download_dir, nombre_archivo_para_lectura)

                print(f"\n{respuesta_lectura}\n")
                if respuesta_lectura['estado_exitoso']:
                    # Abrir el archivo en modo escritura (sobrescribiendo si ya existe)
                    contenido_archivo = b""
                    for i in range(len(respuesta_lectura["contenido_bloques_seguidor"]) - 1, -1, -1):
                        print(f'Contenido del bloque {i}: {respuesta_lectura["contenido_bloques_seguidor"][i]}')
                        contenido_archivo += respuesta_lectura["contenido_bloques_seguidor"][i]
                    print(f'Contenido del archivo: {contenido_archivo}')
                    with open(ruta_archivo_descargado, 'wb') as file:
                        file.write(contenido_archivo)
                            
                

                # Ver el contenido del archivo en consola
                print("Contenido del archivo:\n---------------------\n")
                # Mantener la consola abierta hasta que el usuario escriba "salir"
                while True:
                    with open(ruta_archivo_descargado, 'r') as file:
                        print(file.read())
                    print("\n---------------------\n")
                    user_input = input("Escribe 'q' o 'Q' para cerrar la lectura: ")
                    if user_input.lower() in ['q', 'Q']:
                        break  # Salir del bucle si el usuario escribe "salir"
                os.remove(ruta_archivo_descargado)
                print("Cerrando lectura correctamente...")

            except Exception as e:
                print(f"Error al leer el archivo: {e}")
        else:
            print("Error: Debes proporcionar el nombre del archivo a leer.")
    
    
    
    
    
    
    def comando_delete(self, username, comando):
        if len(comando) > 1:
            nombre_archivo = comando[1]
            try:
                configs = self.cargar_configuraciones()
                client = self.crear_cliente(configs[0], configs[1], configs[2], configs[3])

                # Aquí inserto el código de 'delete'
                nombre_usuario = username
                respuesta_localizacion = client.locate_file(nombre_archivo, nombre_usuario)

                first_data_node_url_lider = respuesta_localizacion['url_data_node_lider'][0]  # Obtener la URL del DataNode líder
                respuesta_eliminacion = client.delete_file(first_data_node_url_lider, nombre_usuario, nombre_archivo, respuesta_localizacion)
                
                if respuesta_eliminacion:
                    print(f"\nArchivo '{nombre_archivo}' eliminado con éxito.\n")
                else:
                    print(f"\nError al eliminar el archivo '{nombre_archivo}'.")

            except Exception as e:
                print(f"Error al eliminar el archivo: {e}")
        else:
            print("Error: Debes proporcionar el nombre del archivo a eliminar.")









def cli_commands(username):
    """Función principal que ejecuta el CLI para explorar archivos y carpetas."""
    cli = CLI()
    
    ruta_base = os.path.abspath(uploadable_files)  # Carpeta base
    if not os.path.exists(ruta_base):
        os.makedirs(ruta_base)  # Crear la carpeta uploadable_files si no existe

    ruta_actual = ""  # Iniciar en la carpeta base sin subcarpetas

    while True:
        usuario = username
        comando = input(f'{usuario}-{cli.obtener_ip_global()}@hdfs-service:~/{ruta_actual}$ ').strip().split()

        if len(comando) == 0:
            cli.comando_invalido()
            continue
        elif comando[0].lower() == "help":
            cli.comando_help()
        elif comando[0].lower() == "ls":
            cli.comando_ls(comando, ruta_base, ruta_actual)
        elif comando[0].lower() == "cd":
            ruta_actual = cli.comandos_cd(comando, ruta_base, ruta_actual)
        elif comando[0].lower() == "mkdir":
            cli.comandos_mkdir(comando, ruta_actual)
        elif comando[0].lower() == "touch":
            cli.comandos_touch(comando, ruta_actual)
        elif comando[0].lower() == "nano":
            cli.comandos_nano(comando, ruta_actual)
        elif comando[0].lower() == "cp":
            cli.comandos_cp(comando, ruta_actual)
        elif comando[0].lower() == "mv":
            cli.comandos_mv(comando, ruta_actual)
            
            
        elif comando[0].lower() == "get":
            cli.comando_get(username, comando)
        elif comando[0].lower() == "put":
            cli.comando_put(username, comando)
        elif comando[0].lower() == "read":
            cli.comando_read(username, comando)
        elif comando[0].lower() == "delete":
            cli.comando_delete(username, comando)
            
            
        elif comando[0].lower() == "rm":
            cli.comandos_rm(comando, ruta_actual)
        elif comando[0].lower() == "clear":
            cli.comando_clear()
        elif comando[0].lower() == "exit":
            print("Saliendo de la consola...")
            break
        else:
            cli.comando_invalido()
            continue