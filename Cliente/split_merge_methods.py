import os
import sys

# Directorio donde se encuentra la carpeta 'local_files'
DIRECTORIO_ARCHIVOS = os.path.abspath(os.path.join(os.path.dirname(__file__), 'resources', 'uploadable_files'))
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
            
            
            ##### Esto se puede borrar cuando sea necesario
            
            # Crear un nombre para el archivo de salida con formato block_####
            chunk_filename = f"block_{i + 1:04d}"  # Formato block_0001, block_0002, etc.
            chunk_path = os.path.join(DIRECTORIO_ARCHIVOS, chunk_filename)

            # Escribir el bloque en un nuevo archivo
            with open(chunk_path, 'wb') as chunk_file:
                chunk_file.write(chunk)
                
            ##### hasta acá
            
            
            # Agregar el contenido del bloque al diccionario
            lista_contenido_bloques.append(chunk)
            #print(f"Archivo creado: {chunk_path}")

    return {
        "nombre_archivo": os.path.basename(file_path),  # Nombre del archivo original
        "lista_contenido_bloques": lista_contenido_bloques  # Diccionario de bloques creados
    }