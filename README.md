# Proyecto 1

**Curso:** ST0263 - Tópicos Especiales en Telemática
<br>**Profesor:** Edwin Montoya - emontoya@eafit.edu.co
<br>**Estudiantes:**
- Miguel Ángel Calvache Giraldo
- Mauricio David Correa Hernandez
- Miguel Angel Martinez Garcia
- Salomon Velez Perez
- Simon Botero
  
<br>**Título:** Proyecto 1 - Almacenamiento de datos distribuidos
<br>**Objetivo:** El objetivo del proyecto es diseñar e implementar un sistema de archivos distribuidos minimalista con diferentes servicios

<br>**Marco teorico:** https://docs.google.com/document/d/1XhELDe5ejoQrJkqlhBVplVIEhJko8Ut9RD6CYbSD4R4/edit?usp=sharing

## 1. Descripción general de la actividad

Este proyecto consiste en la implementación de un sistema de archivos distribuido basado en el modelo de HDFS (Hadoop Distributed File System) para manejar grandes volúmenes de datos con alta disponibilidad y tolerancia a fallos. La arquitectura requiere del NameNode para gestionar los metadatos y coordinar la replicación de los bloques de archivos en múltiples DataNodes.

### 1.1 Aspectos cumplidos
* Comunicación con API REST
* Comunicación con gRPC
* Creación de cuenta, incio de sesión, cerrar sesión, borrar cuenta de clientes.
* Carga de archivos por clientes.
* Descarga de archivos por clientes.
* Lectura de archivos por clientes.
* Eliminación de archivos por clientespara localización.
* Block report de cada datanode al namenode.
* Heartbeat entre NameNode y DataNode.
* Handshake entre NameNode y DataNode
* Almacenamiento de bloques con particionamiento y replicación en DataNodes tanto como leader y follower
* Balanceo de cargas entre los 3 DataNode.
* Manejo de archivos y directorios en NameNode para datanodes, usuarios y simulación como base de datos para localización que apunta a los bloques. 
* Manejo de directorios en NameNode.
* Pipeline de sincronización entre DataNodes

### 1.2 Aspectos no desarrollados
* Replicación en NameNode follower (backup).
* Visualización de archivos en el sistema.
* Despliegue en AWS.

## 2. Arquitectura del sistema

A continuación se observa el diagrama de la arquitectura usada para nuestro proyecto.

![telematica-arquitectura-proyecto-1](Arquitectura_HDFS.png)

### 2.1 Descripción de la arquitectura

<br>**Especificaciones finales del proyecto y arquitectura:** https://docs.google.com/document/d/1BWjEFo9onN4zYj9Sk8SyBW9M5ToxV9vimVzr1-dyPo8/edit#heading=h.h3ye4bivbzs

### 

## 3. Descripción del ambiente de desarrollo

Ejecución del nameNode

```
python NameNode1/nameNode.py
```

Ejecución de los dataNode

```
python DataNode1/dataNode.py
```
```
python DataNode2/dataNode.py
```
```
python DataNode3/dataNode.py
```

Ejecución dle cliente

```
python Cliente/client.py
```

Desde el cliente se pueden realizar las siguientes acciones:

0. Salir
1. Registrar
2. Iniciar sesión
3. Cerrar sesión
4. Eliminar cuenta (requiere iniciar sesión y mantener la sesión activa)

Al haber iniciado sesión, se pueden realizar las siguientes acciones:

- help: Muestra los comandos disponibles.
 - ls [nombre_carpeta]: Listar el contenido de una carpeta de manera local.
 - cd [nombre_carpeta]: Cambiar a una carpeta dentro de la ruta actual de manera local.
 - cd ..: Subir hasta al directorio base de manera local.
 - mkdir [nombre_carpeta]: Crear una carpeta vacía de manera local.
 - touch [nombre_archivo]: Crear un archivo vacío de manera local.
 - nano [nombre_archivo]: Editar un archivo de manera local.
 - cp [nombre_archivo_origen] [nombre_archivo_destino]: Copiar un archivo de manera local.
 - mv [nombre_archivo_origen] [nombre_archivo_destino]: Mover un archivo de manera local.
 - get [nombre_archivo]: Descargar un archivo en el sistema HDFS.
 - put [nombre_archivo]: Subir un archivo en el sistema HDFS.
 - read [nombre_archivo]: Leer un archivo en el sistema HDFS.
 - delete [nombre_archivo]: Eliminar un archivo en el sistema HDFS.
 - rm [nombre_archivo]: Eliminar un archivo de manera local.
 - rm -r [nombre_carpeta]: Eliminar una carpeta y su contenido de manera local.
 - clear: Limpiar la consola de manera local.
 - exit: Salir del programa de manera local.

 ## 4. Descripción del ambiente de ejecución

<br> Lenguaje de programación: Python 3.8.5
<br>Requirements: 

```
grpcio
grpcio-tools
pip
protobuf==5.27.2
setuptools
python-dotenv
Flask 
bcrypt
requests
```