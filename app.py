import os
import json
from flask import Flask, jsonify, request
from datetime import datetime
from google.cloud import pubsub_v1

app = Flask(__name__)

PROJECT_ID = "data-project-streaming-486719" 

publisher = pubsub_v1.PublisherClient()

TOPIC_AGRESORES = publisher.topic_path(PROJECT_ID, "agresores-datos")
TOPIC_VICTIMAS = publisher.topic_path(PROJECT_ID, "victimas-datos")

# --- FUNCIÓN CORREGIDA ---
def publicar_en_nube(data, role, topic_path): # Añadido 'role' para coincidir con la llamada
    # Enviamos los datos tal cual vienen (que ya no traen el role dentro)
    message_json = json.dumps(data)
    message_bytes = message_json.encode("utf-8")
    
    future = publisher.publish(topic_path, message_bytes)
    return future.result()

@app.route("/", methods=["GET"])
def home():
    return jsonify({
        "app": os.getenv("APP_NAME", "API Ingestora Pub/Sub"),
        "status": "online",
        "endpoints": ["/agresores", "/victimas"],
        "timestamp": datetime.utcnow().isoformat() + "Z"
    })

@app.route("/agresores", methods=["POST"])
def ingest_agresor():
    data = request.json
    if not data:
        return jsonify({"error": "No data provided"}), 400
    
    try:
        # Aquí pasas 3 argumentos, ahora la función los acepta
        msg_id = publicar_en_nube(data, "AGRESOR", TOPIC_AGRESORES)
        return jsonify({
            "status": "success",
            "message_id": msg_id,
            "received_at": datetime.utcnow().isoformat() + "Z"
        }), 201
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/victimas", methods=["POST"])
def ingest_victima():
    data = request.json
    if not data:
        return jsonify({"error": "No data provided"}), 400
    
    try:
        msg_id = publicar_en_nube(data, "VICTIMA", TOPIC_VICTIMAS)
        return jsonify({
            "status": "success",
            "message_id": msg_id,
            "received_at": datetime.utcnow().isoformat() + "Z"
        }), 201
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/health", methods=["GET"])
def health():
    return jsonify({"status": "ok"}), 200

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8080))
    app.run(host="0.0.0.0", port=port)
    