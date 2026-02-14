import time
import json
import datetime
import os
import random
from google.cloud import pubsub_v1
from dotenv import load_dotenv

# 1. Cargar configuración
load_dotenv()
PROJECT_ID = os.getenv("PROJECT_ID") # Asegúrate de que esto esté en tu .env
TOPIC_VICTIMAS = "victimas-datos"
TOPIC_AGRESORES = "agresores-datos"

# Configurar cliente PubSub
publisher = pubsub_v1.PublisherClient()
topic_path_v = publisher.topic_path(PROJECT_ID, TOPIC_VICTIMAS)
topic_path_a = publisher.topic_path(PROJECT_ID, TOPIC_AGRESORES)

print(f"🚀 GENERADOR DE ESCENARIOS: FÍSICOS Y SAFE PLACES")
print(f"🎯 Proyecto: {PROJECT_ID}")
print("------------------------------------------------")

def generar_mensaje(user_id, lat, lon):
    now = datetime.datetime.now().isoformat()
    msg = {
        "user_id": user_id,
        "coordinates": [lat, lon], 
        "kmh": round(random.uniform(0, 5), 2),
        "battery": random.randint(20, 100),
        "timestamp": now
    }
    data = json.dumps(msg).encode("utf-8")
    return data

def simular_match_fisico():
    """
    ESCENARIO A: Harry y Voldemort se encuentran en el Ayuntamiento.
    """
    print("\n--- ⚔️  SIMULANDO MATCH FÍSICO (Cercanía) ---")
    # Coordenadas: Plaza del Ayuntamiento
    lat = 39.4699
    lon = -0.3763
    
    # 1. Enviar Harry (vic_001)
    # Nota: Uso los IDs exactos de tu captura de pantalla
    msg_v = generar_mensaje("vic_001", lat, lon)
    publisher.publish(topic_path_v, msg_v).result()
    print(f"📍 Víctima enviada: Harry (vic_001) en Ayuntamiento.")

    time.sleep(1) # Pequeña pausa para realismo

    # 2. Enviar Voldemort (agr_001) muy cerca
    msg_a = generar_mensaje("agr_001", lat + 0.00005, lon + 0.00005)
    publisher.publish(topic_path_a, msg_a).result()
    print(f"😈 Agresor enviado: Voldemort (agr_001) en Ayuntamiento.")

def simular_safe_place_intrusion():
    """
    ESCENARIO B: Vader entra en Safe Places (Hemisfèric y Mestalla).
    No necesitamos enviar a la víctima, el sistema debe detectar que 
    el agresor está en una zona prohibida de la BBDD.
    """
    print("\n--- 🚨 SIMULANDO INTRUSIÓN EN SAFE PLACE ---")
    
    # CASO 1: Vader en la "Estrella de la Muerte" (Hemisfèric - plc_003)
    # Coordenadas exactas del SQL
    lat_hemisferic = 39.4550
    lon_hemisferic = -0.3505
    
    msg_a1 = generar_mensaje("agr_002", lat_hemisferic, lon_hemisferic)
    publisher.publish(topic_path_a, msg_a1).result()
    print(f"😈 Agresor enviado: Vader (agr_002) en SAFE PLACE (Hemisfèric/Estrella Muerte).")
    
    time.sleep(2)

    # CASO 2: Vader en "Alderaan" (Mestalla - plc_005)
    # Coordenadas exactas del SQL
    lat_mestalla = 39.4800
    lon_mestalla = -0.3400
    
    msg_a2 = generar_mensaje("agr_002", lat_mestalla, lon_mestalla)
    publisher.publish(topic_path_a, msg_a2).result()
    print(f"😈 Agresor enviado: Vader (agr_002) en SAFE PLACE (Mestalla/Alderaan).")


if __name__ == "__main__":
    while True:
        # Ciclo de pruebas
        simular_match_fisico()
        time.sleep(5) # Espera breve entre escenarios
        
        simular_safe_place_intrusion()
        
        print("\n⏳ Ciclo terminado. Esperando 15s para nueva ronda...")
        print("   (Esto permite ver los resultados en Dataflow/Consola)")
        time.sleep(5)