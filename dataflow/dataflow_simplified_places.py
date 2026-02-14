import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions, SetupOptions
from apache_beam.transforms.window import FixedWindows
from apache_beam.transforms.periodicsequence import PeriodicImpulse
from apache_beam.io import GenerateSequence
import logging
import json
import os
import argparse
from geopy.distance import geodesic
import psycopg2 
from dotenv import load_dotenv

load_dotenv()

### FUNCIONES
def parsePubSubMessage(message):

    message_str = message.decode('utf-8')
    message_dict = json.loads(message_str)

    logging.info(f"Parsed message: {message_dict}")

    return message_dict

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
    
    uid = datos_victima['user_id']

    if uid in datos_maestros:
        print(f"✅ DEBUG CRUCE: ¡{uid} encontrado! Agresores asociados: {datos_maestros[uid]['agresores']}")
    else:
        # Esto nos dirá si llega ' vi_001' en vez de 'vi_001'
        print(f"❌ DEBUG CRUCE: {uid} NO está en datos_maestros. (Claves disponibles: {list(datos_maestros.keys())[:5]})")
    
    info = datos_maestros.get(uid)
    
    if info:
        datos_victima['safe_zones'] = info['zonas']
        
        #Generamos un evento por cada agresor
        for id_agresor in info['agresores']:
            yield (id_agresor, datos_victima)

def detectar_match(elemento):
    """
    Recibe: ('ag_001', [lista_de_eventos_mezclados])
    Devuelve: [alerta1, alerta2, ...] 
    """
    id_agresor, eventos = elemento

    print(f"⚖️ DEBUG MATCH: Analizando ventana para Agresor {id_agresor}. Total eventos: {len(eventos)}") #########
    
    # Separar al agresor de las víctimas
    datos_agresor = None
    victimas = []

    for e in eventos:
        if e['tipo'] == 'agresor':
            datos_agresor = e
        elif e['tipo'] == 'victima':
            victimas.append(e)

    #Si no hay datos del agresor o de las víctimas, no devuelve nada
    if not datos_agresor or not victimas:
        return []

    # Calcular distancias
    alertas_json = []
    for vic in victimas:
        dist_fisica = round(geodesic(datos_agresor['coordinates'], vic['coordinates']).meters,2)
        
        if dist_fisica < 500:
                alerta = {
                    "alerta": "fisica",
                    "nivel": "CRITICO",
                    "id_victima": vic['user_id'],
                    "id_agresor": id_agresor,
                    "distancia_metros": dist_fisica,
                    "coordenadas_agresor": datos_agresor['coordinates'], 
                    "coordenadas_victima": vic['coordinates'],
                    "timestamp": datos_agresor['timestamp']
                }
                # No incluimos campos de 'place' porque no aplican aquí
                alertas_json.append(alerta)
                print(f"🚨 JSON ALERTA GENERADO (FISICA): {alerta}")
            
    #matches de zona
        for zona in vic.get('safe_zones', []):
                    
                    dist_zona = geodesic(datos_agresor['coordinates'], zona['place_coordinates']).meters
                    
                    if dist_zona < zona['radius']:
                        alerta = {
                            "alerta": "place",
                            "nivel": "ALTO",
                            "id_victima": vic['user_id'],
                            "id_agresor": id_agresor,
                            "id_place": zona['id_place'],
                            "nombre_place": zona['place_name'], 
                            "distancia_metros": round(dist_zona, 2),
                            "radio_zona": zona['radius'],
                            "coordenadas_agresor": datos_agresor['coordinates'],
                            "coordenadas_place": zona['place_coordinates'],
                            "timestamp": datos_agresor['timestamp']
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
        cursor.execute("SELECT id_victima, id_agresor FROM rel_victimas_agresores")
        for vic, agr in cursor.fetchall():
            if vic not in datos_maestros:
                datos_maestros[vic] = {'agresores': [], 'zonas': []}
            datos_maestros[vic]['agresores'].append(agr)

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
        print(f"📊 DEBUG DB: Cargados {len(datos_maestros)} usuarios maestros.")
        if len(datos_maestros) > 0:
            ejemplo_id = list(datos_maestros.keys())[0]
            print(f"🔑 DEBUG CLAVE: Ejemplo de ID en memoria: '{ejemplo_id}' (Ojo a los espacios)")
            print(f"🔗 DEBUG RELACIÓN: {datos_maestros[ejemplo_id]}")
        
        yield datos_maestros
        # Estructira devuelta de datos_maestros:
        # { "vic_001": {
        #       "agresores": ["ag_001", "ag_002"],  
        #       "zonas": [
        #           {"place_name": "Comisaría Centro", "id_place": "place_001", "place_coordinates": (39.4700, -0.3765), "radius": 300}

    def finish_bundle(self):
        if self.conn:
            self.conn.close()

### PIPELINE

def run():

    parser = argparse.ArgumentParser(description=('Input arguments for the Dataflow Streaming Pipeline.'),allow_abbrev=False)

    parser.add_argument(
                '--project_id',
                required=False,
                default=os.getenv("PROJECT_ID"),
                help='GCP cloud project name.')
    
    parser.add_argument(
                '--victimas_pubsub_subscription_name',
                required=False,
                default= "victimas-datos-sub",
                help='Pub/Sub subscription for victim events.')
    
    parser.add_argument(
                '--agresores_pubsub_subscription_name',
                required=False,
                default= "agresores-datos-sub",
                help='Pub/Sub subscription for engagement events.')
    
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
                '--firestore_collection',
                required=False,
                help='Firestore collection name.')
    
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
    parser.add_argument('--db_host', required=False, default=os.getenv("DB_HOST"))
    parser.add_argument('--db_user', required=False, default=os.getenv("DB_USER"))
    parser.add_argument('--db_pass', required=False, default=os.getenv("DB_PASS"))
    parser.add_argument('--db_name', required=False, default=os.getenv("DB_NAME"))
    
    # Parseamos los argumentos
    known_args, beam_args = parser.parse_known_args()
    # Creamos las opciones pasándole los argumentos de Beam
    options = PipelineOptions(beam_args)
    options.view_as(StandardOptions).streaming = True
    # Configuramos save_main_session para que las funciones globales viajen a los workers
    options.view_as(SetupOptions).save_main_session = True
    
    # Nombres de suscripciones
    sub_v = f"projects/{os.getenv('PROJECT_ID')}/subscriptions/{os.getenv('SUBSCRIPTION_VICTIMAS')}"
    sub_a = f"projects/{os.getenv('PROJECT_ID')}/subscriptions/{os.getenv('SUBSCRIPTION_AGRESORES')}"
    topic_policia = f"projects/{known_args.project_id}/topics/{known_args.alertas_policia_topic}"

    with beam.Pipeline(options=options) as p:
        db_pcoll = (
                    p
                    | "TriggerUnico" >> beam.Create([None])
                    | "CargarDB" >> beam.ParDo(CargarDatosMaestros(db_host=known_args.db_host, db_user=known_args.db_user, db_pass=known_args.db_pass, db_name=known_args.db_name))
        )

        side_db = beam.pvalue.AsSingleton(db_pcoll, default_value={})



        victimas = (
            p 
            | "LeerVictimas" >> beam.io.ReadFromPubSub(subscription=sub_v)
            | "ParsearV" >> beam.Map(parsePubSubMessage)
            | "FormatearV" >> beam.Map(normalizeVictimas)
            | "CruzarDB" >> beam.FlatMap(cruzar_datos_en_memoria, datos_maestros=side_db)
            # Salida: ('ag_001', {datos_victima})
        )
        agresores = (
            p 
            | "LeerAgresores" >> beam.io.ReadFromPubSub(subscription=sub_a)
            | "ParsearA" >> beam.Map(parsePubSubMessage)
            | "FormatearA" >> beam.Map(normalizeAgresores)
            | "ClaveAgresor" >> beam.Map(lambda x: (x['user_id'], x))
            # Salida: ('ag_001', {datos_agresor})
        )

        # Match
        (
            (victimas, agresores)
            | "UnirTodo" >> beam.Flatten()
            | "Ventana15s" >> beam.WindowInto(FixedWindows(15))
            | "Agrupar" >> beam.GroupByKey() #juntamos en base a key que es el agresor id
            | "Calcular" >> beam.FlatMap(detectar_match)
            | "Serializar" >> beam.Map(jsonEncode)
            | "EnviarPolicia" >> beam.io.WriteToPubSub(topic=topic_policia)
        )

if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    print("🚀 Arrancando Pipeline Simplificado...")
    run()