import osmnx as ox
import networkx as nx
import random
from datetime import datetime
import time
import threading
import requests

# URL de tu API local
API_URL = "http://192.168.1.44:8080"

class SafetyMovementGenerator:
    def __init__(self, place_name="Valencia, Spain"):
        print(f"Descargando mapa de {place_name}...")
        self.graph = ox.graph_from_place(place_name, network_type='drive')
        self.nodes = list(self.graph.nodes())
        print(f"✓ Mapa cargado: {len(self.nodes)} nodos.")
        
    def send_to_api(self, data, role):
        """
        Diferencia el endpoint según el rol, pero no envía el rol en el JSON.
        """
        endpoint = "/victimas" if role == "VICTIMA" else "/agresores"
        
        # Doble seguridad para que no viaje el campo 'role'
        data.pop('role', None) 
        
        try:
            requests.post(f"{API_URL}{endpoint}", json=data, timeout=5)
        except Exception as e:
            print(f"Error de conexión: {e}")

    def user_thread(self, s):
        """Bucle de movimiento independiente para cada sujeto."""
        curr_node = random.choice(self.nodes)
        battery = random.uniform(85, 100)
        
        while True:
            try:
                target_node = random.choice(self.nodes)
                route = nx.shortest_path(self.graph, curr_node, target_node, weight='length')

                for node_id in route:
                    node_data = self.graph.nodes[node_id]
                    battery -= random.uniform(0.01, 0.05)
                    
                    # CAMBIO REALIZADO: Coordenadas en formato lista [lat, lon]
                    # según la captura de WhatsApp adjunta
                    payload = {
                        "user_id": s['user_id'],
                        "coordinates": [node_data['y'], node_data['x']],
                        "kmh": s['kmh'],
                        "battery": round(battery, 1),
                        "timestamp": datetime.now().isoformat()
                    }

                    # Enrutamiento basado en rol (sin enviarlo en el payload)
                    self.send_to_api(payload, s['role'])

                    curr_node = node_id
                    time.sleep(5)

            except Exception:
                time.sleep(1)
                continue

if __name__ == "__main__":
    generator = SafetyMovementGenerator()

    sujetos = [
        ("VIC_01", "VICTIMA"), ("VIC_02", "VICTIMA"), 
        ("VIC_03", "VICTIMA"), ("VIC_04", "VICTIMA"),
        ("AGR_01", "AGRESOR"), ("AGR_02", "AGRESOR"), 
        ("AGR_03", "AGRESOR"), ("AGR_04", "AGRESOR"), 
        ("AGR_05", "AGRESOR")
    ]

    for uid, role in sujetos:
        r = random.random()
        if r < 0.80:
            kmh = random.uniform(3, 6)   
        elif r < 0.95:
            kmh = random.uniform(30, 50) 
        else:
            kmh = random.uniform(12, 18) 

        config = {
            'user_id': uid,
            'role': role,
            'kmh': round(kmh, 2)
        }

        threading.Thread(target=generator.user_thread, args=(config,), daemon=True).start()

    try:
        while True: time.sleep(1)
    except KeyboardInterrupt:
        print("\nSimulación finalizada.")
        