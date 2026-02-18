import os
import json
from flask import Flask, jsonify, request
from datetime import datetime
from google.cloud import pubsub_v1
from google.cloud.sql.connector import Connector, IPTypes
import pg8000

app = Flask(__name__)
PROJECT_ID = os.getenv("PROJECT_ID", "data-project-streaming-487217")

# --- CONEXIONES ---
publisher = pubsub_v1.PublisherClient()
TOPIC_AGRESORES = publisher.topic_path(PROJECT_ID, "agresores-datos")
TOPIC_VICTIMAS = publisher.topic_path(PROJECT_ID, "victimas-datos")

def get_db_connection():
    """Crea la conexión segura a Cloud SQL."""
    connector = Connector()
    conn = connector.connect(
        os.getenv("INSTANCE_CONNECTION_NAME"), "pg8000",
        user=os.getenv("DB_USER"), password=os.getenv("DB_PASS"),
        db=os.getenv("DB_NAME"), ip_type=IPTypes.PUBLIC
    )
    return conn

# =========================================================
# 📡 1. MÓDULO DE INGESTA DE DISPOSITIVOS GPS (Hacia Pub/Sub)
# =========================================================
@app.route("/agresores", methods=["POST"])
def ingest_agresor():
    data = request.json
    publisher.publish(TOPIC_AGRESORES, json.dumps(data).encode("utf-8")).result()
    return jsonify({"status": "success"}), 201

@app.route("/victimas", methods=["POST"])
def ingest_victima():
    data = request.json
    publisher.publish(TOPIC_VICTIMAS, json.dumps(data).encode("utf-8")).result()
    return jsonify({"status": "success"}), 201


# =========================================================
# 👮‍♂️ 2. MÓDULO DASHBOARD POLICÍA (Hacia Cloud SQL)
# =========================================================

# ----------------- VÍCTIMAS -----------------
@app.route("/api/policia/victimas", methods=["GET"])
def obtener_victimas():
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT id_victima, nombre_victima, apellido_victima, url_foto_victima FROM victimas")
        rows = cursor.fetchall()
        conn.close()
        
        victimas = [{"id_victima": r[0], "nombre_victima": r[1], "apellido_victima": r[2], "url_foto_victima": r[3]} for r in rows]
        return jsonify({"victimas": victimas}), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/api/policia/nueva_victima", methods=["POST"])
def crear_victima():
    data = request.json
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        foto = data.get('url_foto_victima', 'vacio')
        
        cursor.execute(
            "INSERT INTO victimas (id_victima, nombre_victima, apellido_victima, url_foto_victima) VALUES (%s, %s, %s, %s)",
            (data['id_victima'], data['nombre_victima'], data['apellido_victima'], foto)
        )
        conn.commit()
        conn.close()
        return jsonify({"status": "Víctima creada con éxito"}), 201
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# ----------------- AGRESORES -----------------
@app.route("/api/policia/agresores", methods=["GET"])
def obtener_agresores():
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT id_agresor, nombre_agresor, apellido_agresor, url_foto_agresor FROM agresores")
        rows = cursor.fetchall()
        conn.close()
        
        agresores = [{"id_agresor": r[0], "nombre_agresor": r[1], "apellido_agresor": r[2], "url_foto_agresor": r[3]} for r in rows]
        return jsonify({"agresores": agresores}), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/api/policia/nuevo_agresor", methods=["POST"])
def crear_agresor():
    data = request.json 
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        foto = data.get('url_foto_agresor', 'vacio')
        
        cursor.execute(
            "INSERT INTO agresores (id_agresor, nombre_agresor, apellido_agresor, url_foto_agresor) VALUES (%s, %s, %s, %s)",
            (data['id_agresor'], data['nombre_agresor'], data['apellido_agresor'], foto)
        )
        conn.commit()
        conn.close()
        return jsonify({"status": "Agresor creado con éxito"}), 201
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# ----------------- SAFE PLACES -----------------
@app.route("/api/policia/safe_places", methods=["GET"])
def obtener_safe_places():
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT id_place, place_name, place_coordinates, radius FROM safe_places")
        rows = cursor.fetchall()
        conn.close()
        
        lugares = [{"id_place": r[0], "place_name": r[1], "place_coordinates": r[2], "radius": r[3]} for r in rows]
        return jsonify({"safe_places": lugares}), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/api/policia/nuevo_safe_place", methods=["POST"])
def crear_zona_segura():
    data = request.json 
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute(
            "INSERT INTO safe_places (id_place, place_name, place_coordinates, radius) VALUES (%s, %s, %s, %s)",
            (data['id_place'], data['place_name'], data['place_coordinates'], data['radius'])
        )
        conn.commit()
        conn.close()
        return jsonify({"status": "Safe Place registrado"}), 201
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# ----------------- RELACIÓN VÍCTIMA-AGRESOR -----------------
@app.route("/api/policia/relaciones_agresores", methods=["GET"])
def obtener_relaciones_agresores():
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT id_agresor, id_victima, dist_seguridad FROM rel_victimas_agresores")
        rows = cursor.fetchall()
        conn.close()
        
        relaciones = [{"id_agresor": r[0], "id_victima": r[1], "dist_seguridad": r[2]} for r in rows]
        return jsonify({"relaciones": relaciones}), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/api/policia/relacion_victima_agresor", methods=["POST"])
def crear_relacion_victima_agresor():
    data = request.json 
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute(
            "INSERT INTO rel_victimas_agresores (id_agresor, id_victima, dist_seguridad) VALUES (%s, %s, %s)",
            (data['id_agresor'], data['id_victima'], data['dist_seguridad'])
        )
        conn.commit()
        conn.close()
        return jsonify({"status": "Relación Víctima-Agresor establecida"}), 201
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# ----------------- RELACIÓN VÍCTIMA-SAFE PLACE -----------------
@app.route("/api/policia/relaciones_safe_places", methods=["GET"])
def obtener_relaciones_safe_places():
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        # ¡Corregido el nombre de la tabla!
        cursor.execute("SELECT id_victima, id_place FROM rel_places_victimas")
        rows = cursor.fetchall()
        conn.close()
        
        relaciones = [{"id_victima": r[0], "id_place": r[1]} for r in rows]
        return jsonify({"relaciones": relaciones}), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/api/policia/relacion_victima_safe_place", methods=["POST"])
def relacion_victima_safe_places():
    data = request.json 
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        # ¡Corregido el nombre de la tabla!
        cursor.execute(
            "INSERT INTO rel_places_victimas (id_victima, id_place) VALUES (%s, %s)",
            (data['id_victima'], data['id_place'])
        )
        conn.commit()
        conn.close()
        return jsonify({"status": "Safe Place asignado a la víctima"}), 201
    except Exception as e:
        return jsonify({"error": str(e)}), 500


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8080))
    app.run(host="0.0.0.0", port=port)
