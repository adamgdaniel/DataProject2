import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions, SetupOptions
from apache_beam.transforms.window import FixedWindows
from apache_beam.transforms.periodicsequence import PeriodicImpulse
from apache_beam.transforms import trigger
from apache_beam.transforms import window
import logging
import json
import os
import argparse
from geopy.distance import geodesic
import psycopg2 
from google.cloud import firestore
import math
from dotenv import load_dotenv

load_dotenv()

### FUNCIONES
def parsePubSubMessage(message):
    try:
        message_str = message.decode('utf-8')
        message_dict = json.loads(message_str)

        logging.info(f"Parsed message: {message_dict}")

        return message_dict
    except Exception as e:
        logging.error(f"Error parsing message: {e}")
        return None

def jsonEncode(elemento):
    mensaje_str = json.dumps(elemento)

    return mensaje_str.encode('utf-8')

def normalizeVictimas(event):
   
    return {
        "user_id": event["user_id"],
        "coordinates": event["coordinates"],
        "timestamp": event["timestamp"],
        "tipo": "victima",
    }

def normalizeAgresores(event):
  
    return {
        "user_id": event["user_id"],
        "coordinates": event["coordinates"],
        "timestamp": event["timestamp"],
        "tipo": "agresor",
    }

def cruzar_datos_en_memoria(datos_victima, datos_maestros):
    if not datos_victima: return
    
    uid = str(datos_victima['user_id'])
    info_victima_db = datos_maestros.get(uid)
    
    if info_victima_db:
        datos_victima['safe_zones'] = info_victima_db['zonas']
        
        for id_agresor, dist in info_victima_db['agresores'].items():
            # Ahora datos_victima tiene: user_id, coordinates, timestamp, tipo, safe_zones Y dist_seguridad
            datos_victima['dist_seguridad'] = dist            
            yield (id_agresor, datos_victima)

def calcular_direccion_escape(coords_victima, coords_agresor):
    lat_v, lon_v = coords_victima
    lat_a, lon_a = coords_agresor

    d_lat = lat_a - lat_v
    d_lon = lon_a - lon_v

    # Calculamos el ángulo en grados
    angulo = math.degrees(math.atan2(d_lat, d_lon))
    if angulo < 0:
        angulo += 360


    if 22.5 <= angulo < 67.5: return "Noreste"
    elif 67.5 <= angulo < 112.5: return "Norte"
    elif 112.5 <= angulo < 157.5: return "Noroeste"
    elif 157.5 <= angulo < 202.5: return "Oeste"
    elif 202.5 <= angulo < 247.5: return "Suroeste"
    elif 247.5 <= angulo < 292.5: return "Sur"
    elif 292.5 <= angulo < 337.5: return "Sureste"
    else: return "Este"

def detectar_match(elemento):
    """
    Recibe: ('ag_001', [lista_de_usuaros_mezclados])
    Devuelve: [alerta1, alerta2, ...] 
    """
    id_agresor, usuarios = elemento

    # Separar al agresor de las víctimas
    datos_agresor = None
    victimas = []

    for e in usuarios:
        if e['tipo'] == 'agresor':
            datos_agresor = e
        elif e['tipo'] == 'victima':
            victimas.append(e)

    if not datos_agresor or not victimas:
        return []

    # Calcular distancias
    alertas_json = []
    for vic in victimas:
        distancia_configurada = vic.get('dist_seguridad', 500)
        dist_fisica = round(geodesic(datos_agresor['coordinates'], vic['coordinates']).meters,2)
        
        if dist_fisica < distancia_configurada:
                direccion_escape = calcular_direccion_escape(vic['coordinates'], datos_agresor['coordinates'])
                alerta = {
                    "alerta": "fisica",
                    "activa": True,
                    "nivel": "CRITICO",
                    "id_victima": vic['user_id'],
                    "id_agresor": id_agresor,
                    "distancia_metros": dist_fisica,
                    "direccion_escape": direccion_escape,
                    "coordenadas_agresor": datos_agresor['coordinates'], 
                    "coordenadas_victima": vic['coordinates'],
                    "timestamp": datos_agresor['timestamp'],
                    "dist_seguridad": distancia_configurada,
                }

                alertas_json.append(alerta)
                print(f"🚨 JSON ALERTA GENERADO (FISICA): {alerta}")
            
    #matches de zona
        for zona in vic.get('safe_zones', []):
                    
                    dist_zona = geodesic(datos_agresor['coordinates'], zona['place_coordinates']).meters
                    
                    if dist_zona < (distancia_configurada + zona['radius']):
                        direccion_escape = calcular_direccion_escape(zona['place_coordinates'], datos_agresor['coordinates'])
                        alerta = {
                            "alerta": "place",
                            "activa": True,
                            "id_victima": vic['user_id'],
                            "id_agresor": id_agresor,
                            "id_place": zona['id_place'],
                            "nombre_place": zona['place_name'], 
                            "distancia_metros": round(dist_zona, 2),
                            "direccion_escape": direccion_escape,
                            "radio_zona": zona['radius'],
                            "coordenadas_agresor": datos_agresor['coordinates'],
                            "coordenadas_place": zona['place_coordinates'],
                            "timestamp": datos_agresor['timestamp'],
                            "distancia_limite": distancia_configurada,
                        }
                        alertas_json.append(alerta)
                        print(f"🏰 JSON ALERTA GENERADO (Place): {alerta}") 
            
    return alertas_json


### CLASES

class CargarDatosMaestros(beam.DoFn):

    def __init__(self, db_host, db_user, db_pass, db_name):
            self.db_host = db_host
            self.db_user = db_user
            self.db_pass = db_pass
            self.db_name = db_name

    def start_bundle(self):
        import psycopg2
        self.conn = psycopg2.connect(
            host=self.db_host, 
            user=self.db_user,
            password=self.db_pass,
            dbname=self.db_name
        )

    def process(self, element):
        cursor = self.conn.cursor()
        datos_maestros = {}

        #Cargar Agrersores-victimas
        try:
                    # --- 1. Cargar Agresores-Víctimas ---
            cursor.execute("SELECT id_victima, id_agresor, dist_seguridad FROM rel_victimas_agresores")
            for vic, agr, dist in cursor.fetchall():
                vic, agr = str(vic), str(agr)
                dist_seguridad = float(dist) if dist is not None else 500.0
                if vic not in datos_maestros:
                    datos_maestros[vic] = {'agresores': {}, 'zonas': []}
                datos_maestros[vic]['agresores'][agr] = dist_seguridad

        #Cargar Safe Places
            query_zonas = """
                        SELECT r.id_victima, s.place_name, s.place_coordinates, s.radius, s.id_place 
                        FROM safe_places s
                        JOIN rel_places_victimas r ON s.id_place = r.id_place
                    """
            cursor.execute(query_zonas)

            for row in cursor.fetchall():
                        vic = row[0]
                        
                        if vic in datos_maestros:

                            datos_maestros[vic]['zonas'].append({
                                "place_name": row[1],
                                "id_place": row[4],
                                "place_coordinates": row[2],
                                "radius": float(row[3])
                            })
            yield datos_maestros

        except Exception as e:
            logging.error(f"❌ Error cargando DB: {e}")
            yield {}
    
        # Estructira devuelta de datos_maestros:
        # { "vic_001": {
        #       "agresores": ["ag_001", "ag_002"],  
        #       "zonas": [
        #           {"place_name": "Comisaría Centro", "id_place": "place_001", "place_coordinates": (39.4700, -0.3765), "radius": 300}


    def finish_bundle(self):
        if self.conn:
            self.conn.close()


class FormatFirestoreDocument(beam.DoFn):
    def __init__(self, project_id, firestore_collection, firestore_database):
        self.project_id = project_id
        self.collection = firestore_collection
        self.database = firestore_database

    def setup(self):
        # Se ejecuta 1 vez al arrancar el worker
        from google.cloud import firestore
        self.db = firestore.Client(project=self.project_id, database=self.database)

    def process(self, element):
        # element ya es el diccionario de la alerta. Lo guardamos directo.
        doc_id = f"{element['id_victima']}_{element['id_agresor']}"
        try:
            self.db.collection(self.collection).document(doc_id).set(element, merge=True)
        except Exception as e:
            import logging
            logging.error(f"Error guardando en Firestore: {e}")
        yield element  

    def teardown(self):
        # Cierra la conexión al apagar
        if hasattr(self, 'db'):
            self.db.close()



### PIPELINE

def run():

    parser = argparse.ArgumentParser(description=('Input arguments for the Dataflow Streaming Pipeline.'),allow_abbrev=False)

    parser.add_argument(
                '--project_id',
                required=True,
                default=os.getenv("PROJECT_ID"),
                help='GCP cloud project name.')
    
    parser.add_argument(
                '--victimas_pubsub_subscription_name',
                required=True,
                default= "victimas-datos-sub",
                help='Pub/Sub subscription for victim events.')
    
    parser.add_argument(
                '--agresores_pubsub_subscription_name',
                required=True,
                default= "agresores-datos-sub",
                help='Pub/Sub subscription for engagement events.')
    
    parser.add_argument(
            '--firestore_db',
            required=True,
            help='Firestore database name.')

    parser.add_argument(
            '--firestore_collection',
            required=True,
            help='Firestore collection name.')
    
    parser.add_argument(
                '--alertas_policia_topic',
                required=False,
                default="policia-alertas",
                help='Pub/Sub topic for police notifications.')
    parser.add_argument(
                '--alertas_agresores_topic',
                required=False,
                default= "agresores-alertas",
                help='Pub/Sub topic for agressor notifications.')
    parser.add_argument(
                '--alertas_victimas_topic',
                required=False,
                default= "victimas-alertas",
                help='Pub/Sub topic for victim notifications.')

    

    
    parser.add_argument(
                '--bigquery_dataset',
                required=False,
                help='BigQuery dataset name.')
    
    parser.add_argument(
                '--user_bigquery_table',
                required=False,
                help='User BigQuery table name.')
    
    parser.add_argument(
                '--episode_bigquery_table',
                required=False,
                help='Episode BigQuery table name.')
    
    #info de la db
    parser.add_argument('--db_host', required=True)
    parser.add_argument('--db_user', required=True)
    parser.add_argument('--db_pass', required=True)
    parser.add_argument('--db_name', required=True)
    
    # Parseamos los argumentos
    known_args, beam_args = parser.parse_known_args()
    # Creamos las opciones pasándole los argumentos de Beam
    options = PipelineOptions(beam_args)
    options.view_as(StandardOptions).streaming = True
    # Configuramos save_main_session para que las funciones globales viajen a los workers
    options.view_as(SetupOptions).save_main_session = True
    
    # Nombres de suscripciones
    sub_v = f"projects/{known_args.project_id}/subscriptions/{known_args.victimas_pubsub_subscription_name}"
    sub_a = f"projects/{known_args.project_id}/subscriptions/{known_args.agresores_pubsub_subscription_name}"
    topic_policia = f"projects/{known_args.project_id}/topics/{known_args.alertas_policia_topic}"
    firestore_database = known_args.firestore_db
   
    with beam.Pipeline(options=options) as p:
        datos_side_input = (
            p
            | "Reloj" >> PeriodicImpulse(fire_interval=900, apply_windowing=True)
            | "VentanaGlobal" >> beam.WindowInto(window.GlobalWindows(), trigger=trigger.Repeatedly(trigger.AfterCount(1)), accumulation_mode=trigger.AccumulationMode.DISCARDING)
            | "CargarDB" >> beam.ParDo(CargarDatosMaestros(db_host=known_args.db_host, db_user=known_args.db_user, db_pass=known_args.db_pass, db_name=known_args.db_name))
            )

        vista_datos_maestros = beam.pvalue.AsSingleton(datos_side_input, default_value={})

        victimas = (
            p 
            | "LeerVictimas" >> beam.io.ReadFromPubSub(subscription=sub_v)
            | "ParsearV" >> beam.Map(parsePubSubMessage)
            | "FormatearV" >> beam.Map(normalizeVictimas)
            | "CruzarDB" >> beam.FlatMap(cruzar_datos_en_memoria, datos_maestros=vista_datos_maestros)
            # Sale: ('agr_001', {datos_victima})
        )
        agresores = (
            p 
            | "LeerAgresores" >> beam.io.ReadFromPubSub(subscription=sub_a)
            | "ParsearA" >> beam.Map(parsePubSubMessage)
            | "FormatearA" >> beam.Map(normalizeAgresores)
            | "ClaveAgresor" >> beam.Map(lambda x: (x['user_id'], x))
            # Sale: ('agr_001', {datos_agresor})
        )

        # Match
        (
            (victimas, agresores)
            | "UnirTodo" >> beam.Flatten()
            | "Ventana15s" >> beam.WindowInto(FixedWindows(15))
            | "Agrupar" >> beam.GroupByKey() #juntamos en base a key que es el agresor id
            | "Calcular" >> beam.FlatMap(detectar_match)
            | "EnviarFirestore" >> beam.ParDo(FormatFirestoreDocument(firestore_collection=known_args.firestore_collection, project_id=known_args.project_id, firestore_database=firestore_database))
            | "Serializar" >> beam.Map(jsonEncode)
            | "EnviarPolicia" >> beam.io.WriteToPubSub(topic=topic_policia)
        )

if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    print("🚀 Arrancando Pipeline...")
    run()