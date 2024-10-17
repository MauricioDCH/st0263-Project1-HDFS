import requests

class Autenticacion:
    def __init__(self):
        self.is_logged_in = False
        self.token = None
        self.username = None

    def obtener_ip_global(self):
        try:
            response = requests.get('https://api.ipify.org?format=json')
            response.raise_for_status()
            ip_address = response.json()['ip']
            return ip_address
        except requests.RequestException as e:
            print(f"Error al obtener la IP global: {e}")
            return "IP no disponible"

    def register_cliente(self, username, password, confirmation_password, name_node_ip, name_node_port):
        client_ip = self.obtener_ip_global()
        endpoint = f'http://{name_node_ip}:{name_node_port}/register'
        data_dictionary = {
            "username": username,
            "password": password,
            "confirmation_password": confirmation_password,
            "client_ip": client_ip
        }
        try:
            response = requests.post(endpoint, json=data_dictionary)
            return response.text, response.status_code
        except requests.RequestException as e:
            print(f"Error al registrar cliente: {e}")
            return "Error de conexión", 500

    def login_cliente(self, username, password, name_node_ip, name_node_port):
        endpoint = f'http://{name_node_ip}:{name_node_port}/login'  # endpoint del servidor Flask
        headers = {'Content-Type': 'application/json'}
        data = {
            'username': username,
            'password': password
        }

        try:
            response = requests.post(endpoint, json=data, headers=headers)
            if response.status_code == 200:
                self.is_logged_in = True
                self.token = response.json()['token']
                self.username = username
                return response.json()["message"], 200
            else:
                return response.json()["message"], response.status_code
        except requests.exceptions.RequestException as e:
            return f"Error en la conexión al servidor: {e}", 500

    def logout_cliente(self, name_node_ip, name_node_port):
        if self.is_logged_in:
            endpoint = f'http://{name_node_ip}:{name_node_port}/logout'  # endpoint del servidor Flask
            headers = {'Content-Type': 'application/json'}
            data = {
                'username': self.username,  # Usar el username guardado
                'token': self.token          # Usar el token guardado
            }

            try:
                response = requests.post(endpoint, json=data, headers=headers)
                if response.status_code == 200:
                    self.is_logged_in = False  # Cambiar estado a no loggeado
                    return response.json()["message"], 200  # Mensaje del servidor
                else:
                    return response.json()["message"], response.status_code  # Mensaje de error
            except requests.exceptions.RequestException as e:
                return f"Error en la conexión al servidor: {e}", 500
        else:
            return "No hay ninguna sesión activa", 400  # Retornar error si no está loggeado
        
    def unregister_cliente(self, password, name_node_ip, name_node_port):
        if self.is_logged_in:
            url = f'http://{name_node_ip}:{name_node_port}/unregister'  # URL del servidor Flask
            headers = {'Content-Type': 'application/json'}
            data = {
                'username': self.username,
                'password': password,
                'token': self.token
            }

            try:
                response = requests.post(url, json=data, headers=headers)
                if response.status_code == 200:
                    self.is_logged_in = False  # Cambiar estado a no loggeado
                    self.token = None  # Limpiar el token
                    self.username = None  # Limpiar el nombre de usuario
                    return response.json()["message"], 200
                else:
                    return response.json()["message"], response.status_code
            except requests.exceptions.RequestException as e:
                return f"Error en la conexión al servidor: {e}", 500
        else:
            return "No hay ninguna sesión activa", 400  # Retornar error si no está loggeado


class Menu:
    def __init__(self) -> None:
        pass
    
    def menu_autenticacion(self):
        print("\nMenú autenticación")
        print("0. Salir")
        print("1. Registrar")
        print("2. Iniciar Sesión")
        print("3. Cerrar Sesión")
        print("4. Eliminar Cuenta")
        option = input("Seleccione una opción: ")
        return option

def cerrar_programa(autenticacion, name_node_ip, name_node_port):
    """Función para cerrar el programa de forma segura."""
    if autenticacion.is_logged_in:
        response_text, status_code = autenticacion.logout_cliente(name_node_ip, name_node_port)
        print(f"Respuesta al cerrar sesión: {response_text}, Código de estado: {status_code}")
    print("Saliendo del programa...")
    exit(0)

def signal_handler(sig, frame, autenticacion, name_node_ip, name_node_port):
    """Manejador para la señal de Ctrl + C."""
    print("\nInterrupción detectada (Ctrl + C).")
    cerrar_programa(autenticacion, name_node_ip, name_node_port)