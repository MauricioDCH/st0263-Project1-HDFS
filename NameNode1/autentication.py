import os
import json
import bcrypt
import uuid  # Para generar tokens únicos
import re  # Para validación de IPs
from flask import Flask, request, jsonify

class AuthServer:
    def __init__(self):
        self.app = Flask(__name__)
        script_dir = os.path.dirname(os.path.abspath(__file__))
        # Rutas de archivos JSON
        self.USERS_FILE = os.path.join(script_dir, 'users', 'registered_users.json')
        self.LOGGED_USERS_FILE = os.path.join(script_dir, 'users', 'logged_users.json')

        # Definir las rutas de Flask
        self.app.add_url_rule('/register', view_func=self.register_user, methods=['POST'])
        self.app.add_url_rule('/login', view_func=self.login_user, methods=['POST'])
        self.app.add_url_rule('/logout', view_func=self.logout_user, methods=['POST'])
        self.app.add_url_rule('/unregister', view_func=self.unregister_user, methods=['POST'])

    # Función para cargar usuarios registrados desde el archivo JSON
    def cargar_usuarios(self):
        if os.path.exists(self.USERS_FILE):
            with open(self.USERS_FILE, 'r') as file:
                return json.load(file)
        return {}

    # Función para guardar usuarios registrados en el archivo JSON
    def guardar_usuarios(self, usuarios):
        os.makedirs(os.path.dirname(self.USERS_FILE), exist_ok=True)
        with open(self.USERS_FILE, 'w') as file:
            json.dump(usuarios, file, indent=4)

    # Función para cargar usuarios logueados desde el archivo JSON
    def cargar_usuarios_logueados(self):
        if os.path.exists(self.LOGGED_USERS_FILE):
            with open(self.LOGGED_USERS_FILE, 'r') as file:
                return json.load(file)
        return {}

    # Función para guardar usuarios logueados en el archivo JSON
    def guardar_usuarios_logueados(self, usuarios_logueados):
        os.makedirs(os.path.dirname(self.LOGGED_USERS_FILE), exist_ok=True)
        with open(self.LOGGED_USERS_FILE, 'w') as file:
            json.dump(usuarios_logueados, file, indent=4)

    # Función para validar IP
    def validar_ip(self, ip):
        regex = r'^(?:[0-9]{1,3}\.){3}[0-9]{1,3}$'
        return re.match(regex, ip) is not None

    def register_user(self):
        usuarios_registrados = self.cargar_usuarios()
        
        data = request.get_json()
        username = data.get('username')
        password = data.get('password')
        confirmation_password = data.get('confirmation_password')
        client_ip = data.get('client_ip')
        
        # Validación de datos
        if not all([username, password, confirmation_password, client_ip]):
            return "Faltan datos", 400  # Mensaje simple en caso de error
        
        if not self.validar_ip(client_ip):
            return "IP inválida", 400  # Mensaje simple en caso de error
        
        if password != confirmation_password:
            return "Las contraseñas no coinciden", 400  # Mensaje simple en caso de error
        
        if username in usuarios_registrados:
            return "Usuario ya registrado", 409  # Mensaje simple en caso de error
        
        hashed_password = bcrypt.hashpw(password.encode('utf-8'), bcrypt.gensalt()).decode('utf-8')
        
        usuarios_registrados[username] = {
            'password': hashed_password,
            'client_ip': client_ip
        }
        
        self.guardar_usuarios(usuarios_registrados)
        
        return f"Usuario {username} registrado exitosamente.", 201  # Mensaje simple al registrarse exitosamente

    def login_user(self):
        usuarios_registrados = self.cargar_usuarios()
        usuarios_logueados = self.cargar_usuarios_logueados()
        
        data = request.get_json()
        username = data.get('username')
        password = data.get('password')
        
        # Validación de datos
        if not all([username, password]):
            return jsonify({"message": "Faltan datos"}), 400
        
        if username not in usuarios_registrados:
            return jsonify({"message": "Usuario no registrado"}), 404
        
        # Verificar si el usuario ya está logueado
        if username in usuarios_logueados:
            return jsonify({"message": f"El usuario {username} ya está logueado"}), 409
        
        # Verificar la contraseña
        hashed_password = usuarios_registrados[username]['password']
        if bcrypt.checkpw(password.encode('utf-8'), hashed_password.encode('utf-8')):
            # Generar token único para el usuario
            token = str(uuid.uuid4())
            
            # Guardar el usuario logueado y su token
            usuarios_logueados[username] = {
                'token': token,
                'client_ip': usuarios_registrados[username]['client_ip']
            }
            self.guardar_usuarios_logueados(usuarios_logueados)
            
            return jsonify({"message": f"Inicio de sesión exitoso para {username}", "token": token}), 200
        else:
            return jsonify({"message": "Contraseña incorrecta"}), 401

    def logout_user(self):
        usuarios_logueados = self.cargar_usuarios_logueados()    
        
        data = request.get_json()
        username = data.get('username')
        token = data.get('token')
            
        # Verificar si el usuario está logueado
        if username in usuarios_logueados:
            if usuarios_logueados[username]['token'] == token:
                # Eliminar al usuario del archivo de usuarios logueados
                del usuarios_logueados[username]
                self.guardar_usuarios_logueados(usuarios_logueados)
                return jsonify({"message": f"Usuario {username} ha cerrado sesión correctamente"}), 200
            else:
                return jsonify({"message": "Token incorrecto"}), 400
        else:
            return jsonify({"message": "Usuario no encontrado"}), 400

    def unregister_user(self):
        data = request.get_json()
        username = data.get('username')
        password = data.get('password')
        token = data.get('token')

        # Validación de datos
        if not username or not password or not token:
            return jsonify({"message": "Faltan datos"}), 400

        # Verificar que el token sea válido
        logged_users = self.cargar_usuarios_logueados()
        if username not in logged_users or logged_users[username]['token'] != token:
            return jsonify({"message": "Token inválido o sesión no activa"}), 401

        # Cargar usuarios registrados
        usuarios_registrados = self.cargar_usuarios()

        # Verificar si el usuario está registrado
        if username not in usuarios_registrados:
            return jsonify({"message": "Usuario no registrado"}), 404

        # Obtener el hash de la contraseña almacenada
        hash_password_almacenada = usuarios_registrados[username]['password']

        # Verificar la contraseña proporcionada
        if not bcrypt.checkpw(password.encode('utf-8'), hash_password_almacenada.encode('utf-8')):
            return jsonify({"message": "Contraseña incorrecta"}), 401

        # Eliminar al usuario de la lista de usuarios registrados
        del usuarios_registrados[username]
        self.guardar_usuarios(usuarios_registrados)

        # También eliminar al usuario de los usuarios logueados
        del logged_users[username]
        self.guardar_usuarios_logueados(logged_users)

        return jsonify({"message": f"Usuario {username} desregistrado con éxito"}), 200

    def run(self, port):
        self.app.run(host='0.0.0.0', port=port)
