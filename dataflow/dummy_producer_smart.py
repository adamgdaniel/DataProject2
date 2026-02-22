import time
import json
import datetime
import os
import random
from google.cloud import pubsub_v1
from dotenv import load_dotenv

# 1. Cargar configuración
load_dotenv()
PROJECT_ID = os.getenv("PROJECT_ID") 
TOPIC_VICTIMAS = "victimas-datos"
TOPIC_AGRESORES = "agresores-datos"

# Configurar cliente PubSub
publisher = pubsub_v1.PublisherClient()
topic_path_v = publisher.topic_path(PROJECT_ID, TOPIC_VICTIMAS)
topic_path_a = publisher.topic_path(PROJECT_ID, TOPIC_AGRESORES)

print(f"🚀 GENERADOR DE ESCENARIOS DINÁMICOS")
print(f"🎯 Proyecto: {PROJECT_ID}")
print("------------------------------------------------")

def generar_mensaje(user_id, lat, lon):
    now = datetime.datetime.now().isoformat()
    msg = {
        "user_id": user_id,
        # CAMBIO IMPORTANTE: Formato string "lat, lon" para que no rompa Dataflow
        "coordinates": f"{lat:.6f}, {lon:.6f}", 
        "kmh": round(random.uniform(0, 5), 2),
        "battery": random.randint(20, 100),
        "timestamp": now
    }
    data = json.dumps(msg).encode("utf-8")
    return data

def simular_match_fisico():
    print("\n--- ⚔️  SIMULANDO MATCH FÍSICO EN MOVIMIENTO ---")
    
    # 1. Generamos una ubicación aleatoria por el centro de Valencia
    # Sumamos un offset aleatorio a las coordenadas del Ayuntamiento
    base_lat = 39.4699 + random.uniform(-0.02, 0.02)
    base_lon = -0.3763 + random.uniform(-0.02, 0.02)
    
    # Enviar Víctima (vic_001)
    msg_v = generar_mensaje("vic_001", base_lat, base_lon)
    publisher.publish(topic_path_v, msg_v).result()
    print(f"📍 Víctima (vic_001) caminando por: {base_lat:.5f}, {base_lon:.5f}")

    time.sleep(1) # Pequeña pausa para realismo

    # 2. El Agresor (agr_001) acecha cerca
    # Generamos un offset entre -0.004 y 0.004 grados (aprox. entre 50m y 400m de distancia)
    # Esto garantiza que salte la alerta de < 500m, pero siempre desde un ángulo distinto
    lat_agresor = base_lat + random.uniform(-0.004, 0.004)
    lon_agresor = base_lon + random.uniform(-0.004, 0.004)
    
    msg_a = generar_mensaje("agr_001", lat_agresor, lon_agresor)
    publisher.publish(topic_path_a, msg_a).result()
    print(f"😈 Agresor (agr_001) detectado en:  {lat_agresor:.5f}, {lon_agresor:.5f}")

def simular_safe_place_intrusion():
    print("\n--- 🚨 SIMULANDO INTRUSIÓN DINÁMICA EN SAFE PLACE ---")
    
    # Coordenadas exactas del Safe Place 'Callejón Diagon' en tu BD
    lat_zona = 39.470200
    lon_zona = -0.376800
    
    # El agresor no pisa el centro exacto, merodea por el borde de la zona
    # (offset de ~100 metros)
    lat_agresor = lat_zona + random.uniform(-0.001, 0.001)
    lon_agresor = lon_zona + random.uniform(-0.001, 0.001)
    
    msg_a = generar_mensaje("agr_001", lat_agresor, lon_agresor)
    publisher.publish(topic_path_a, msg_a).result()
    print(f"🏰 Agresor (agr_001) merodeando zona segura en: {lat_agresor:.5f}, {lon_agresor:.5f}")


if __name__ == "__main__":
    while True:
        # Hacemos que el script decida aleatoriamente qué tipo de alerta generar
        tipo_simulacion = random.choice(["fisico", "zona_segura"])
        
        if tipo_simulacion == "fisico":
            simular_match_fisico()
        else:
            simular_safe_place_intrusion()
            
        print("\n⏳ Ciclo terminado. Esperando 15s para nueva ronda...")
        print("   (Verás cómo la dirección de escape en BigQuery va cambiando)")
        time.sleep(15)