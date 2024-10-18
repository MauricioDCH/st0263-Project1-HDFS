from google.protobuf.internal import containers as _containers
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from typing import ClassVar as _ClassVar, Iterable as _Iterable, Optional as _Optional

DESCRIPTOR: _descriptor.FileDescriptor

class DataNodeDesignationClientRequest(_message.Message):
    __slots__ = ("nombre_archivo", "tamano_archivo", "nombre_usuario", "url_cliente", "numero_de_replicas_por_bloque")
    NOMBRE_ARCHIVO_FIELD_NUMBER: _ClassVar[int]
    TAMANO_ARCHIVO_FIELD_NUMBER: _ClassVar[int]
    NOMBRE_USUARIO_FIELD_NUMBER: _ClassVar[int]
    URL_CLIENTE_FIELD_NUMBER: _ClassVar[int]
    NUMERO_DE_REPLICAS_POR_BLOQUE_FIELD_NUMBER: _ClassVar[int]
    nombre_archivo: str
    tamano_archivo: float
    nombre_usuario: str
    url_cliente: str
    numero_de_replicas_por_bloque: int
    def __init__(self, nombre_archivo: _Optional[str] = ..., tamano_archivo: _Optional[float] = ..., nombre_usuario: _Optional[str] = ..., url_cliente: _Optional[str] = ..., numero_de_replicas_por_bloque: _Optional[int] = ...) -> None: ...

class DataNodeDesignationNameNodeResponse(_message.Message):
    __slots__ = ("lista_id_data_node_lider", "lista_id_data_node_seguidor", "lista_url_data_node_lider", "lista_url_data_node_seguidor")
    LISTA_ID_DATA_NODE_LIDER_FIELD_NUMBER: _ClassVar[int]
    LISTA_ID_DATA_NODE_SEGUIDOR_FIELD_NUMBER: _ClassVar[int]
    LISTA_URL_DATA_NODE_LIDER_FIELD_NUMBER: _ClassVar[int]
    LISTA_URL_DATA_NODE_SEGUIDOR_FIELD_NUMBER: _ClassVar[int]
    lista_id_data_node_lider: _containers.RepeatedScalarFieldContainer[int]
    lista_id_data_node_seguidor: _containers.RepeatedScalarFieldContainer[int]
    lista_url_data_node_lider: _containers.RepeatedScalarFieldContainer[str]
    lista_url_data_node_seguidor: _containers.RepeatedScalarFieldContainer[str]
    def __init__(self, lista_id_data_node_lider: _Optional[_Iterable[int]] = ..., lista_id_data_node_seguidor: _Optional[_Iterable[int]] = ..., lista_url_data_node_lider: _Optional[_Iterable[str]] = ..., lista_url_data_node_seguidor: _Optional[_Iterable[str]] = ...) -> None: ...

class BlockReportDataNodeRequest(_message.Message):
    __slots__ = ("id_data_node", "lista_rutas_bloques_seguidor", "lista_rutas_bloques_lider", "json_diccionario_metadatos_bloques_seguidor", "json_diccionario_metadatos_bloques_lider", "id_bloque", "ruta_lider", "id_lider", "url_lider", "ids_seguidores", "urls_seguidores", "rutas_seguidores")
    ID_DATA_NODE_FIELD_NUMBER: _ClassVar[int]
    LISTA_RUTAS_BLOQUES_SEGUIDOR_FIELD_NUMBER: _ClassVar[int]
    LISTA_RUTAS_BLOQUES_LIDER_FIELD_NUMBER: _ClassVar[int]
    JSON_DICCIONARIO_METADATOS_BLOQUES_SEGUIDOR_FIELD_NUMBER: _ClassVar[int]
    JSON_DICCIONARIO_METADATOS_BLOQUES_LIDER_FIELD_NUMBER: _ClassVar[int]
    ID_BLOQUE_FIELD_NUMBER: _ClassVar[int]
    RUTA_LIDER_FIELD_NUMBER: _ClassVar[int]
    ID_LIDER_FIELD_NUMBER: _ClassVar[int]
    URL_LIDER_FIELD_NUMBER: _ClassVar[int]
    IDS_SEGUIDORES_FIELD_NUMBER: _ClassVar[int]
    URLS_SEGUIDORES_FIELD_NUMBER: _ClassVar[int]
    RUTAS_SEGUIDORES_FIELD_NUMBER: _ClassVar[int]
    id_data_node: int
    lista_rutas_bloques_seguidor: _containers.RepeatedScalarFieldContainer[str]
    lista_rutas_bloques_lider: _containers.RepeatedScalarFieldContainer[str]
    json_diccionario_metadatos_bloques_seguidor: str
    json_diccionario_metadatos_bloques_lider: str
    id_bloque: str
    ruta_lider: str
    id_lider: str
    url_lider: str
    ids_seguidores: _containers.RepeatedScalarFieldContainer[str]
    urls_seguidores: _containers.RepeatedScalarFieldContainer[str]
    rutas_seguidores: _containers.RepeatedScalarFieldContainer[str]
    def __init__(self, id_data_node: _Optional[int] = ..., lista_rutas_bloques_seguidor: _Optional[_Iterable[str]] = ..., lista_rutas_bloques_lider: _Optional[_Iterable[str]] = ..., json_diccionario_metadatos_bloques_seguidor: _Optional[str] = ..., json_diccionario_metadatos_bloques_lider: _Optional[str] = ..., id_bloque: _Optional[str] = ..., ruta_lider: _Optional[str] = ..., id_lider: _Optional[str] = ..., url_lider: _Optional[str] = ..., ids_seguidores: _Optional[_Iterable[str]] = ..., urls_seguidores: _Optional[_Iterable[str]] = ..., rutas_seguidores: _Optional[_Iterable[str]] = ...) -> None: ...

class BlockReportNameNodeResponse(_message.Message):
    __slots__ = ("estado_exitoso",)
    ESTADO_EXITOSO_FIELD_NUMBER: _ClassVar[int]
    estado_exitoso: bool
    def __init__(self, estado_exitoso: bool = ...) -> None: ...

class FileLocationClientRequest(_message.Message):
    __slots__ = ("nombre_archivo", "nombre_usuario", "url_cliente")
    NOMBRE_ARCHIVO_FIELD_NUMBER: _ClassVar[int]
    NOMBRE_USUARIO_FIELD_NUMBER: _ClassVar[int]
    URL_CLIENTE_FIELD_NUMBER: _ClassVar[int]
    nombre_archivo: str
    nombre_usuario: str
    url_cliente: str
    def __init__(self, nombre_archivo: _Optional[str] = ..., nombre_usuario: _Optional[str] = ..., url_cliente: _Optional[str] = ...) -> None: ...

class FileLocationNameNodeResponse(_message.Message):
    __slots__ = ("lista_id_data_node_seguidor", "lista_id_data_node_lider", "lista_url_data_node_seguidor", "lista_url_data_node_lider", "lista_rutas_bloques_seguidor", "lista_rutas_bloques_lider")
    LISTA_ID_DATA_NODE_SEGUIDOR_FIELD_NUMBER: _ClassVar[int]
    LISTA_ID_DATA_NODE_LIDER_FIELD_NUMBER: _ClassVar[int]
    LISTA_URL_DATA_NODE_SEGUIDOR_FIELD_NUMBER: _ClassVar[int]
    LISTA_URL_DATA_NODE_LIDER_FIELD_NUMBER: _ClassVar[int]
    LISTA_RUTAS_BLOQUES_SEGUIDOR_FIELD_NUMBER: _ClassVar[int]
    LISTA_RUTAS_BLOQUES_LIDER_FIELD_NUMBER: _ClassVar[int]
    lista_id_data_node_seguidor: _containers.RepeatedScalarFieldContainer[int]
    lista_id_data_node_lider: _containers.RepeatedScalarFieldContainer[int]
    lista_url_data_node_seguidor: _containers.RepeatedScalarFieldContainer[str]
    lista_url_data_node_lider: _containers.RepeatedScalarFieldContainer[str]
    lista_rutas_bloques_seguidor: _containers.RepeatedScalarFieldContainer[str]
    lista_rutas_bloques_lider: _containers.RepeatedScalarFieldContainer[str]
    def __init__(self, lista_id_data_node_seguidor: _Optional[_Iterable[int]] = ..., lista_id_data_node_lider: _Optional[_Iterable[int]] = ..., lista_url_data_node_seguidor: _Optional[_Iterable[str]] = ..., lista_url_data_node_lider: _Optional[_Iterable[str]] = ..., lista_rutas_bloques_seguidor: _Optional[_Iterable[str]] = ..., lista_rutas_bloques_lider: _Optional[_Iterable[str]] = ...) -> None: ...

class BackUpNameNodeLeaderRequest(_message.Message):
    __slots__ = ("lista_todos_los_archivos_en_namenodeleader", "lista_todos_contenidos_los_archivos_en_namenodeleader", "lista_diccionario_metadatos_archivos")
    LISTA_TODOS_LOS_ARCHIVOS_EN_NAMENODELEADER_FIELD_NUMBER: _ClassVar[int]
    LISTA_TODOS_CONTENIDOS_LOS_ARCHIVOS_EN_NAMENODELEADER_FIELD_NUMBER: _ClassVar[int]
    LISTA_DICCIONARIO_METADATOS_ARCHIVOS_FIELD_NUMBER: _ClassVar[int]
    lista_todos_los_archivos_en_namenodeleader: _containers.RepeatedScalarFieldContainer[str]
    lista_todos_contenidos_los_archivos_en_namenodeleader: _containers.RepeatedScalarFieldContainer[str]
    lista_diccionario_metadatos_archivos: _containers.RepeatedScalarFieldContainer[str]
    def __init__(self, lista_todos_los_archivos_en_namenodeleader: _Optional[_Iterable[str]] = ..., lista_todos_contenidos_los_archivos_en_namenodeleader: _Optional[_Iterable[str]] = ..., lista_diccionario_metadatos_archivos: _Optional[_Iterable[str]] = ...) -> None: ...

class BackUpNameNodeFollowerResponse(_message.Message):
    __slots__ = ("estado_exitoso",)
    ESTADO_EXITOSO_FIELD_NUMBER: _ClassVar[int]
    estado_exitoso: bool
    def __init__(self, estado_exitoso: bool = ...) -> None: ...

class HandShakeDataNodeRequest(_message.Message):
    __slots__ = ("data_node_ip", "data_node_port")
    DATA_NODE_IP_FIELD_NUMBER: _ClassVar[int]
    DATA_NODE_PORT_FIELD_NUMBER: _ClassVar[int]
    data_node_ip: str
    data_node_port: int
    def __init__(self, data_node_ip: _Optional[str] = ..., data_node_port: _Optional[int] = ...) -> None: ...

class HandShakeNameNodeResponse(_message.Message):
    __slots__ = ("id_data_node", "estado_exitoso")
    ID_DATA_NODE_FIELD_NUMBER: _ClassVar[int]
    ESTADO_EXITOSO_FIELD_NUMBER: _ClassVar[int]
    id_data_node: int
    estado_exitoso: bool
    def __init__(self, id_data_node: _Optional[int] = ..., estado_exitoso: bool = ...) -> None: ...

class HeartBeatDataNodeRequest(_message.Message):
    __slots__ = ("id_data_node",)
    ID_DATA_NODE_FIELD_NUMBER: _ClassVar[int]
    id_data_node: int
    def __init__(self, id_data_node: _Optional[int] = ...) -> None: ...

class HeartBeatNameNodeResponse(_message.Message):
    __slots__ = ("estado_exitoso", "timestrap")
    ESTADO_EXITOSO_FIELD_NUMBER: _ClassVar[int]
    TIMESTRAP_FIELD_NUMBER: _ClassVar[int]
    estado_exitoso: bool
    timestrap: str
    def __init__(self, estado_exitoso: bool = ..., timestrap: _Optional[str] = ...) -> None: ...

class DownloadFileClientRequest(_message.Message):
    __slots__ = ("nombre_archivo", "nombre_usuario", "url_cliente", "lista_id_data_node_seguidor", "rutas_bloques_seguidor")
    NOMBRE_ARCHIVO_FIELD_NUMBER: _ClassVar[int]
    NOMBRE_USUARIO_FIELD_NUMBER: _ClassVar[int]
    URL_CLIENTE_FIELD_NUMBER: _ClassVar[int]
    LISTA_ID_DATA_NODE_SEGUIDOR_FIELD_NUMBER: _ClassVar[int]
    RUTAS_BLOQUES_SEGUIDOR_FIELD_NUMBER: _ClassVar[int]
    nombre_archivo: str
    nombre_usuario: str
    url_cliente: str
    lista_id_data_node_seguidor: _containers.RepeatedScalarFieldContainer[int]
    rutas_bloques_seguidor: _containers.RepeatedScalarFieldContainer[str]
    def __init__(self, nombre_archivo: _Optional[str] = ..., nombre_usuario: _Optional[str] = ..., url_cliente: _Optional[str] = ..., lista_id_data_node_seguidor: _Optional[_Iterable[int]] = ..., rutas_bloques_seguidor: _Optional[_Iterable[str]] = ...) -> None: ...

class DownloadFileDataNodeResponse(_message.Message):
    __slots__ = ("lista_contenido_bloques_seguidor", "estado_exitoso")
    LISTA_CONTENIDO_BLOQUES_SEGUIDOR_FIELD_NUMBER: _ClassVar[int]
    ESTADO_EXITOSO_FIELD_NUMBER: _ClassVar[int]
    lista_contenido_bloques_seguidor: _containers.RepeatedScalarFieldContainer[bytes]
    estado_exitoso: bool
    def __init__(self, lista_contenido_bloques_seguidor: _Optional[_Iterable[bytes]] = ..., estado_exitoso: bool = ...) -> None: ...

class UploadFileClientRequest(_message.Message):
    __slots__ = ("nombre_archivo", "nombre_usuario", "url_cliente", "lista_contenido_bloques_lider", "lista_id_data_node_lider", "lista_id_data_node_seguidor", "lista_url_data_node_lider", "lista_url_data_node_seguidor")
    NOMBRE_ARCHIVO_FIELD_NUMBER: _ClassVar[int]
    NOMBRE_USUARIO_FIELD_NUMBER: _ClassVar[int]
    URL_CLIENTE_FIELD_NUMBER: _ClassVar[int]
    LISTA_CONTENIDO_BLOQUES_LIDER_FIELD_NUMBER: _ClassVar[int]
    LISTA_ID_DATA_NODE_LIDER_FIELD_NUMBER: _ClassVar[int]
    LISTA_ID_DATA_NODE_SEGUIDOR_FIELD_NUMBER: _ClassVar[int]
    LISTA_URL_DATA_NODE_LIDER_FIELD_NUMBER: _ClassVar[int]
    LISTA_URL_DATA_NODE_SEGUIDOR_FIELD_NUMBER: _ClassVar[int]
    nombre_archivo: str
    nombre_usuario: str
    url_cliente: str
    lista_contenido_bloques_lider: _containers.RepeatedScalarFieldContainer[bytes]
    lista_id_data_node_lider: _containers.RepeatedScalarFieldContainer[int]
    lista_id_data_node_seguidor: _containers.RepeatedScalarFieldContainer[int]
    lista_url_data_node_lider: _containers.RepeatedScalarFieldContainer[str]
    lista_url_data_node_seguidor: _containers.RepeatedScalarFieldContainer[str]
    def __init__(self, nombre_archivo: _Optional[str] = ..., nombre_usuario: _Optional[str] = ..., url_cliente: _Optional[str] = ..., lista_contenido_bloques_lider: _Optional[_Iterable[bytes]] = ..., lista_id_data_node_lider: _Optional[_Iterable[int]] = ..., lista_id_data_node_seguidor: _Optional[_Iterable[int]] = ..., lista_url_data_node_lider: _Optional[_Iterable[str]] = ..., lista_url_data_node_seguidor: _Optional[_Iterable[str]] = ...) -> None: ...

class UploadFileDataNodeResponse(_message.Message):
    __slots__ = ("estado_exitoso",)
    ESTADO_EXITOSO_FIELD_NUMBER: _ClassVar[int]
    estado_exitoso: bool
    def __init__(self, estado_exitoso: bool = ...) -> None: ...

class DeleteFileClientRequest(_message.Message):
    __slots__ = ("nombre_archivo", "nombre_usuario", "url_cliente", "lista_id_bloque_lider", "lista_id_bloque_seguidor", "lista_url_data_node_lider", "lista_url_data_node_seguidor", "lista_rutas_bloques_lider", "lista_rutas_bloques_seguidor")
    NOMBRE_ARCHIVO_FIELD_NUMBER: _ClassVar[int]
    NOMBRE_USUARIO_FIELD_NUMBER: _ClassVar[int]
    URL_CLIENTE_FIELD_NUMBER: _ClassVar[int]
    LISTA_ID_BLOQUE_LIDER_FIELD_NUMBER: _ClassVar[int]
    LISTA_ID_BLOQUE_SEGUIDOR_FIELD_NUMBER: _ClassVar[int]
    LISTA_URL_DATA_NODE_LIDER_FIELD_NUMBER: _ClassVar[int]
    LISTA_URL_DATA_NODE_SEGUIDOR_FIELD_NUMBER: _ClassVar[int]
    LISTA_RUTAS_BLOQUES_LIDER_FIELD_NUMBER: _ClassVar[int]
    LISTA_RUTAS_BLOQUES_SEGUIDOR_FIELD_NUMBER: _ClassVar[int]
    nombre_archivo: str
    nombre_usuario: str
    url_cliente: str
    lista_id_bloque_lider: _containers.RepeatedScalarFieldContainer[int]
    lista_id_bloque_seguidor: _containers.RepeatedScalarFieldContainer[int]
    lista_url_data_node_lider: _containers.RepeatedScalarFieldContainer[str]
    lista_url_data_node_seguidor: _containers.RepeatedScalarFieldContainer[str]
    lista_rutas_bloques_lider: _containers.RepeatedScalarFieldContainer[str]
    lista_rutas_bloques_seguidor: _containers.RepeatedScalarFieldContainer[str]
    def __init__(self, nombre_archivo: _Optional[str] = ..., nombre_usuario: _Optional[str] = ..., url_cliente: _Optional[str] = ..., lista_id_bloque_lider: _Optional[_Iterable[int]] = ..., lista_id_bloque_seguidor: _Optional[_Iterable[int]] = ..., lista_url_data_node_lider: _Optional[_Iterable[str]] = ..., lista_url_data_node_seguidor: _Optional[_Iterable[str]] = ..., lista_rutas_bloques_lider: _Optional[_Iterable[str]] = ..., lista_rutas_bloques_seguidor: _Optional[_Iterable[str]] = ...) -> None: ...

class DeleteFileDataNodeResponse(_message.Message):
    __slots__ = ("estado_exitoso",)
    ESTADO_EXITOSO_FIELD_NUMBER: _ClassVar[int]
    estado_exitoso: bool
    def __init__(self, estado_exitoso: bool = ...) -> None: ...

class ReadFileClientRequest(_message.Message):
    __slots__ = ("nombre_archivo", "nombre_usuario", "url_cliente", "lista_id_bloque_lider", "lista_id_bloque_seguidor", "lista_url_data_node_lider", "lista_url_data_node_seguidor", "lista_rutas_bloques_lider", "lista_rutas_bloques_seguidor")
    NOMBRE_ARCHIVO_FIELD_NUMBER: _ClassVar[int]
    NOMBRE_USUARIO_FIELD_NUMBER: _ClassVar[int]
    URL_CLIENTE_FIELD_NUMBER: _ClassVar[int]
    LISTA_ID_BLOQUE_LIDER_FIELD_NUMBER: _ClassVar[int]
    LISTA_ID_BLOQUE_SEGUIDOR_FIELD_NUMBER: _ClassVar[int]
    LISTA_URL_DATA_NODE_LIDER_FIELD_NUMBER: _ClassVar[int]
    LISTA_URL_DATA_NODE_SEGUIDOR_FIELD_NUMBER: _ClassVar[int]
    LISTA_RUTAS_BLOQUES_LIDER_FIELD_NUMBER: _ClassVar[int]
    LISTA_RUTAS_BLOQUES_SEGUIDOR_FIELD_NUMBER: _ClassVar[int]
    nombre_archivo: str
    nombre_usuario: str
    url_cliente: str
    lista_id_bloque_lider: _containers.RepeatedScalarFieldContainer[int]
    lista_id_bloque_seguidor: _containers.RepeatedScalarFieldContainer[int]
    lista_url_data_node_lider: _containers.RepeatedScalarFieldContainer[str]
    lista_url_data_node_seguidor: _containers.RepeatedScalarFieldContainer[str]
    lista_rutas_bloques_lider: _containers.RepeatedScalarFieldContainer[str]
    lista_rutas_bloques_seguidor: _containers.RepeatedScalarFieldContainer[str]
    def __init__(self, nombre_archivo: _Optional[str] = ..., nombre_usuario: _Optional[str] = ..., url_cliente: _Optional[str] = ..., lista_id_bloque_lider: _Optional[_Iterable[int]] = ..., lista_id_bloque_seguidor: _Optional[_Iterable[int]] = ..., lista_url_data_node_lider: _Optional[_Iterable[str]] = ..., lista_url_data_node_seguidor: _Optional[_Iterable[str]] = ..., lista_rutas_bloques_lider: _Optional[_Iterable[str]] = ..., lista_rutas_bloques_seguidor: _Optional[_Iterable[str]] = ...) -> None: ...

class ReadFileDataNodeResponse(_message.Message):
    __slots__ = ("lista_contenido_bloques_seguidor", "estado_exitoso")
    LISTA_CONTENIDO_BLOQUES_SEGUIDOR_FIELD_NUMBER: _ClassVar[int]
    ESTADO_EXITOSO_FIELD_NUMBER: _ClassVar[int]
    lista_contenido_bloques_seguidor: _containers.RepeatedScalarFieldContainer[bytes]
    estado_exitoso: bool
    def __init__(self, lista_contenido_bloques_seguidor: _Optional[_Iterable[bytes]] = ..., estado_exitoso: bool = ...) -> None: ...

class PipeLineDataNodeRequest(_message.Message):
    __slots__ = ("nombre_archivo", "id_data_node_lider", "id_data_node_seguidor", "contenido_bloques_lider", "contenido_bloques_seguidor")
    NOMBRE_ARCHIVO_FIELD_NUMBER: _ClassVar[int]
    ID_DATA_NODE_LIDER_FIELD_NUMBER: _ClassVar[int]
    ID_DATA_NODE_SEGUIDOR_FIELD_NUMBER: _ClassVar[int]
    CONTENIDO_BLOQUES_LIDER_FIELD_NUMBER: _ClassVar[int]
    CONTENIDO_BLOQUES_SEGUIDOR_FIELD_NUMBER: _ClassVar[int]
    nombre_archivo: str
    id_data_node_lider: _containers.RepeatedScalarFieldContainer[int]
    id_data_node_seguidor: _containers.RepeatedScalarFieldContainer[int]
    contenido_bloques_lider: _containers.RepeatedScalarFieldContainer[bytes]
    contenido_bloques_seguidor: _containers.RepeatedScalarFieldContainer[bytes]
    def __init__(self, nombre_archivo: _Optional[str] = ..., id_data_node_lider: _Optional[_Iterable[int]] = ..., id_data_node_seguidor: _Optional[_Iterable[int]] = ..., contenido_bloques_lider: _Optional[_Iterable[bytes]] = ..., contenido_bloques_seguidor: _Optional[_Iterable[bytes]] = ...) -> None: ...

class PipeLineDataNodeResponse(_message.Message):
    __slots__ = ("estado_exitoso", "contenido_bloques_seguidor")
    ESTADO_EXITOSO_FIELD_NUMBER: _ClassVar[int]
    CONTENIDO_BLOQUES_SEGUIDOR_FIELD_NUMBER: _ClassVar[int]
    estado_exitoso: bool
    contenido_bloques_seguidor: _containers.RepeatedScalarFieldContainer[bytes]
    def __init__(self, estado_exitoso: bool = ..., contenido_bloques_seguidor: _Optional[_Iterable[bytes]] = ...) -> None: ...

class DeleteFileDataNodeRequest(_message.Message):
    __slots__ = ("nombre_archivo",)
    NOMBRE_ARCHIVO_FIELD_NUMBER: _ClassVar[int]
    nombre_archivo: str
    def __init__(self, nombre_archivo: _Optional[str] = ...) -> None: ...

class DeleteFileNameNodeResponse(_message.Message):
    __slots__ = ("estado_exitoso",)
    ESTADO_EXITOSO_FIELD_NUMBER: _ClassVar[int]
    estado_exitoso: bool
    def __init__(self, estado_exitoso: bool = ...) -> None: ...

class PipeLineForGetDataNodeRequest(_message.Message):
    __slots__ = ("nombre_archivo", "lista_id_data_node_seguidor", "rutas_bloques_seguidor")
    NOMBRE_ARCHIVO_FIELD_NUMBER: _ClassVar[int]
    LISTA_ID_DATA_NODE_SEGUIDOR_FIELD_NUMBER: _ClassVar[int]
    RUTAS_BLOQUES_SEGUIDOR_FIELD_NUMBER: _ClassVar[int]
    nombre_archivo: str
    lista_id_data_node_seguidor: _containers.RepeatedScalarFieldContainer[int]
    rutas_bloques_seguidor: _containers.RepeatedScalarFieldContainer[str]
    def __init__(self, nombre_archivo: _Optional[str] = ..., lista_id_data_node_seguidor: _Optional[_Iterable[int]] = ..., rutas_bloques_seguidor: _Optional[_Iterable[str]] = ...) -> None: ...

class PipeLineForGetDataNodeResponse(_message.Message):
    __slots__ = ("lista_contenido_bloques_seguidor", "estado_exitoso")
    LISTA_CONTENIDO_BLOQUES_SEGUIDOR_FIELD_NUMBER: _ClassVar[int]
    ESTADO_EXITOSO_FIELD_NUMBER: _ClassVar[int]
    lista_contenido_bloques_seguidor: _containers.RepeatedScalarFieldContainer[bytes]
    estado_exitoso: bool
    def __init__(self, lista_contenido_bloques_seguidor: _Optional[_Iterable[bytes]] = ..., estado_exitoso: bool = ...) -> None: ...

class PipeLineForDeleteDataNodeRequest(_message.Message):
    __slots__ = ("nombre_archivo", "lista_id_bloque_lider", "lista_id_bloque_seguidor")
    NOMBRE_ARCHIVO_FIELD_NUMBER: _ClassVar[int]
    LISTA_ID_BLOQUE_LIDER_FIELD_NUMBER: _ClassVar[int]
    LISTA_ID_BLOQUE_SEGUIDOR_FIELD_NUMBER: _ClassVar[int]
    nombre_archivo: str
    lista_id_bloque_lider: _containers.RepeatedScalarFieldContainer[int]
    lista_id_bloque_seguidor: _containers.RepeatedScalarFieldContainer[int]
    def __init__(self, nombre_archivo: _Optional[str] = ..., lista_id_bloque_lider: _Optional[_Iterable[int]] = ..., lista_id_bloque_seguidor: _Optional[_Iterable[int]] = ...) -> None: ...

class PipeLineForDeleteDataNodeResponse(_message.Message):
    __slots__ = ("estado_exitoso",)
    ESTADO_EXITOSO_FIELD_NUMBER: _ClassVar[int]
    estado_exitoso: bool
    def __init__(self, estado_exitoso: bool = ...) -> None: ...
