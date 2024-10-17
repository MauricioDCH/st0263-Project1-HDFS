import os
from dotenv import load_dotenv
load_dotenv(dotenv_path="./configs/.env.client")  # Para cliente

upload_dir = os.getenv("UPLOAD_DIR")
download_dir = os.getenv("DOWNLOAD_DIR")

# Directorio donde se encuentra la carpeta 'resources'
def split_file(file_path, n):
    # Obtener el tamaño total del archivo
    total_size = os.path.getsize(file_path)
    
    # Calcular el tamaño de cada parte
    chunk_size = total_size // n
    remainder = total_size % n  # Resto para ajustar el tamaño en caso de que no se divida perfectamente

    # Leer el contenido del archivo en modo binario
    lista_contenido_bloques = []  # Diccionario para almacenar el contenido de cada bloque
    with open(file_path, 'rb') as file:
        for i in range(n):
            # Leer un bloque de tamaño 'chunk_size'
            if i == n - 1:  # Si es la última parte, añadir el resto
                chunk = file.read(chunk_size + remainder)
            else:
                chunk = file.read(chunk_size)
            
            if not chunk:  # Si no hay más contenido, salir del bucle
                break
            
            # Agregar el contenido del bloque al diccionario
            lista_contenido_bloques.append(chunk)
            #print(f"Archivo creado: {chunk_path}")

    return {
        "nombre_archivo": os.path.basename(file_path),  # Nombre del archivo original
        "lista_contenido_bloques": lista_contenido_bloques  # Diccionario de bloques creados
    }

def merge_file(output_file_name, blocks_content):
    output_file_path = os.path.join(download_dir, output_file_name)
    try:
        # Verificar que todos los elementos de blocks_content son de tipo bytes
        if not all(isinstance(block, bytes) for block in blocks_content):
            raise ValueError("Todos los elementos de blocks_content deben ser de tipo bytes.")

        # Abrir el archivo de salida en modo binario
        with open(output_file_path, 'wb') as output_file:
            for block in blocks_content:
                output_file.write(block)  # Escribir el contenido del bloque en formato binario

        print(f"Archivo '{output_file_name}' fusionado exitosamente en formato binario.")
    except Exception as e:
        print(f"Ocurrió un error al fusionar el archivo: {e}")