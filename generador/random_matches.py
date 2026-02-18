import os
from dotenv import load_dotenv

load_dotenv()

import osmnx as ox
import networkx as nx
import random
from datetime import datetime
import time
import threading
import requests

# --- IMPORTACIONES PARA CLOUD SQL ---
from google.cloud.sql.connector import Connector, IPTypes

# URL de tu API local
API_URL = "http://127.0.0.1:8080"

# --- FUNCIONES DE BASE DE DATOS ---
def obtener_ids_desde_db():
    print("[DB] Consultando Cloud SQL por nuevos IDs...")
    try:
        connector = Connector()
        conn = connector.connect(
            os.getenv("INSTANCE_CONNECTION_NAME"), "pg8000",
            user=os.getenv("DB_USER"), password=os.getenv("DB_PASS"),
            db=os.getenv("DB_NAME"), ip_type=IPTypes.PUBLIC
        )
        cursor = conn.cursor()
        
        cursor.execute("SELECT id_victima FROM victimas")
        victimas = [str(row[0]) for row in cursor.fetchall()] 
        
        cursor.execute("SELECT id_agresor FROM agresores")
        agresores = [str(row[0]) for row in cursor.fetchall()]
        
        conn.close()
        return victimas, agresores
    except Exception as e:
        print(f"[DB ERROR] Error al conectar o consultar la BBDD: {e}")
        return [], []


class SafetyMovementGenerator:
    def __init__(self, place_name="Valencia, Spain"):
        print(f"Descargando mapa de {place_name}...")
        self.graph = ox.graph_from_place(place_name, network_type='drive')
        self.nodes = list(self.graph.nodes())
        print(f"✓ Mapa cargado: {len(self.nodes)} nodos.")
        
        self.active_threads = {} 
        
    def send_to_api(self, data, role):
        endpoint = "/victimas" if role == "VICTIMA" else "/agresores"
        payload = data.copy()
        payload.pop('role', None) 
        
        try:
            requests.post(f"{API_URL}{endpoint}", json=payload, timeout=5)
        except Exception as e:
            print(f"⚠️ [ERROR API] No se pudo enviar a {endpoint}: {e}")

    def user_thread(self, config):
        user_id = config['user_id']
        role = config['role']
        battery = random.uniform(85, 100)
        
        print(f"[THREAD START] Iniciado seguimiento para {role} -> ID: {user_id}")
        
        # ACTORES EXTRAS: Caminan por Valencia libremente (incluida vic_003)
        if user_id not in ["vic_001", "agr_001", "vic_002", "agr_002"]:
            curr_node = random.choice(self.nodes)

        while True:
            try:
                # ==========================================
                # 🔥 HACK DEMO: FORZAR ESCENARIOS FIJOS 🔥
                # ==========================================
                
                # --- ESCENARIO 1: MATCH FÍSICO (Cercanía) ---
                if user_id == "vic_001":
                    # Usamos coordenadas cerca de Tatooine (plc_004) para que sea realista
                    payload = {"user_id": user_id, "coordinates": [39.469900, -0.376000], "kmh": config['kmh'], "battery": round(battery, 1), "timestamp": datetime.now().isoformat()}
                    self.send_to_api(payload, role)
                    time.sleep(5)
                    continue
                    
                elif user_id == "agr_001":
                    # Agresor 1 pegado a Víctima 1 (Diferencia de 0.00005)
                    payload = {"user_id": user_id, "coordinates": [39.469950, -0.376050], "kmh": config['kmh'], "battery": round(battery, 1), "timestamp": datetime.now().isoformat()}
                    self.send_to_api(payload, role)
                    time.sleep(5)
                    continue

                # --- ESCENARIO 2: INVASIÓN DE ZONA SEGURA ---
                elif user_id == "vic_002":
                    # Víctima 2 está en 'Estrella de la Muerte' (Coordenadas exactas de tu BBDD)
                    payload = {"user_id": user_id, "coordinates": [39.455000, -0.350500], "kmh": 0, "battery": round(battery, 1), "timestamp": datetime.now().isoformat()}
                    self.send_to_api(payload, role)
                    time.sleep(5)
                    continue

                elif user_id == "agr_002":
                    # Agresor 2 irrumpe en la Estrella de la Muerte (A pocos metros de distancia)
                    payload = {"user_id": user_id, "coordinates": [39.455020, -0.350520], "kmh": config['kmh'], "battery": round(battery, 1), "timestamp": datetime.now().isoformat()}
                    self.send_to_api(payload, role)
                    time.sleep(5)
                    continue
                # ==========================================

                # ==========================================
                # 🚶‍♂️ LÓGICA NORMAL (Movimiento al azar para los extras) 🚶‍♂️
                # ==========================================
                target_node = random.choice(self.nodes)
                route = nx.shortest_path(self.graph, curr_node, target_node, weight='length')

                for node_id in route:
                    node_data = self.graph.nodes[node_id]
                    battery -= random.uniform(0.01, 0.05)
                    if battery <= 0: battery = 100 
                    
                    payload = {
                        "user_id": user_id,
                        "coordinates": [node_data['y'], node_data['x']],
                        "kmh": config['kmh'],
                        "battery": round(battery, 1),
                        "timestamp": datetime.now().isoformat()
                    }

                    self.send_to_api(payload, role)

                    curr_node = node_id
                    time.sleep(5)

            except Exception:
                time.sleep(1)
                continue

    def arrancar_hilo_si_no_existe(self, user_id, role):
        if user_id not in self.active_threads:
            if user_id in ["vic_001", "agr_001", "vic_002", "agr_002"]:
                kmh = 0
            else:
                r = random.random()
                if r < 0.80: kmh = random.uniform(3, 6)   
                elif r < 0.95: kmh = random.uniform(30, 50) 
                else: kmh = random.uniform(12, 18) 

            config = {
                'user_id': user_id,
                'role': role,
                'kmh': round(kmh, 2)
            }
            
            t = threading.Thread(target=self.user_thread, args=(config,), daemon=True)
            t.start()
            self.active_threads[user_id] = t


if __name__ == "__main__":
    generator = SafetyMovementGenerator()

    try:
        while True:
            v_reales, a_reales = obtener_ids_desde_db()
            
            if "vic_001" not in v_reales: v_reales.append("vic_001")
            if "agr_001" not in a_reales: a_reales.append("agr_001")
            if "vic_002" not in v_reales: v_reales.append("vic_002")
            if "agr_002" not in a_reales: a_reales.append("agr_002")

            for v_id in v_reales:
                generator.arrancar_hilo_si_no_existe(v_id, "VICTIMA")
                
            for a_id in a_reales:
                generator.arrancar_hilo_si_no_existe(a_id, "AGRESOR")
            
            print(f"[INFO] Hilos activos actualmente: {len(generator.active_threads)}")
            time.sleep(60) 
            
    except KeyboardInterrupt:
        print("\n[FIN] Simulación detenida manualmente.")