import time
import json
import datetime
import os
import random
from google.cloud import pubsub_v1
from dotenv import load_dotenv

# 1. Cargar configuración del .env
load_dotenv()
PROJECT_ID = os.getenv("PROJECT_ID")

# DEFINIMOS LOS TÓPICOS MANUALMENTE (según tus indicaciones)
TOPIC_VICTIMAS = "victimas-datos"
TOPIC_AGRESORES = "agresores-datos"

# Configurar el cliente de PubSub
publisher = pubsub_v1.PublisherClient()
topic_path_v = publisher.topic_path(PROJECT_ID, TOPIC_VICTIMAS)
topic_path_a = publisher.topic_path(PROJECT_ID, TOPIC_AGRESORES)

print(f"🚀 INICIANDO GENERADOR DE MATCHES AUTOMÁTICO")
print(f"🎯 Proyecto: {PROJECT_ID}")
print(f"📡 Publicando en: {TOPIC_VICTIMAS} y {TOPIC_AGRESORES}")
print("------------------------------------------------")

def generar_mensaje(user_id, lat, lon):
    now = datetime.datetime.now().isoformat()
    msg = {
        "user_id": user_id,
        "coordinates": [lat, lon], # Lista [lat, lon]
        "kmh": round(random.uniform(0, 5), 2),
        "battery": random.randint(20, 100),
        "timestamp": now
    }
    return json.dumps(msg).encode("utf-8")

def publicar_ciclo():
    try:
        # COORDENADAS BASE: Plaza del Ayuntamiento, Valencia
        lat_base = 39.4699
        lon_base = -0.3763
        
        # 1. ENVIAR VÍCTIMA (vi_001)
        # ------------------------------------------------
        # Le sumamos casi nada para que esté en el mismo sitio
        lat_v = lat_base
        lon_v = lon_base
        
        datos_v = generar_mensaje("vi_001", lat_v, lon_v)
        publisher.publish(topic_path_v, datos_v).result()
        print(f"Running... 📤 VÍCTIMA (vi_001) enviada.")
        
        # 2. ESPERA DE SEGURIDAD (1 segundo)
        # ------------------------------------------------
        time.sleep(1)
        
        # 3. ENVIAR AGRESOR (ag_001)
        # ------------------------------------------------
        # A solo unos metros de diferencia
        lat_a = lat_base + 0.00005 
        lon_a = lon_base + 0.00005
        
        datos_a = generar_mensaje("ag_001", lat_a, lon_a)
        publisher.publish(topic_path_a, datos_a).result()
        print(f"Running... 📤 AGRESOR (ag_001) enviado.")
        
        print("✅ Pareja enviada. Esperando a Dataflow...")
        print("------------------------------------------------")

    except Exception as e:
        print(f"❌ Error publicando: {e}")

if __name__ == "__main__":
    while True:
        publicar_ciclo()
        print("⏳ Durmiendo 20 segundos para cerrar la ventana de Dataflow...")
        # Esperamos 20s para dar tiempo a que la ventana de 15s se cierre y emita resultado
        time.sleep(5)