import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions
from apache_beam.transforms.window import FixedWindows
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

class EtiquetarVictimasConAgresores(beam.DoFn):

    def start_bundle(self):
        self.conn = psycopg2.connect(
            host=os.getenv("DB_HOST"), 
            user=os.getenv("DB_USER"),
            password=os.getenv("DB_PASS"),
            dbname=os.getenv("DB_NAME")
        )

    def process(self, datos_victima):
        if not datos_victima: return

        # COnsulta a la BD quiénes son los agresores de esta víctima
        cursor = self.conn.cursor()
        vic_id = datos_victima['user_id']
        query = f"SELECT id_agresor FROM rel_victimas_agresores WHERE id_victima = '{vic_id}'"
        cursor.execute(query)
        resultados = cursor.fetchall()
        
        #Etiqueta a la víctima con el ID de sus agresores
        
        if not resultados:
            logging.info(f"La victima {datos_victima['user_id']} no tiene agresores en la BD")

        for row in resultados:
            id_agresor = row[0] #para pillar el id del agresor
            yield (id_agresor, datos_victima)

    def finish_bundle(self):
        # Cerrar conexión al terminar
        self.conn.close()

class CargarDatosMaestros(beam.DoFn):

    def start_bundle(self):
        self.conn = psycopg2.connect(
            host=os.getenv("DB_HOST"), 
            user=os.getenv("DB_USER"),
            password=os.getenv("DB_PASS"),
            dbname=os.getenv("DB_NAME")
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
                    JOIN rel_victimas_places r ON s.id_place = r.id_place
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

    parser = argparse.ArgumentParser(description=('Input arguments for the Dataflow Streaming Pipeline.'))

    parser.add_argument(
                '--project_id',
                required=False,
                default=os.getenv("PROJECT_ID"),
                help='GCP cloud project name.')
    
    parser.add_argument(
                '--victimas_pubsub_subscription_name',
                required=False,
                default=os.getenv("SUBSCRIPTION_VICTIMAS"),
                help='Pub/Sub subscription for victim events.')
    
    parser.add_argument(
                '--agresores_pubsub_subscription_name',
                required=False,
                default=os.getenv("SUBSCRIPTION_AGRESORES"),
                help='Pub/Sub subscription for engagement events.')
    
    parser.add_argument(
                '--alertas_policia_topic',
                required=False,
                default=os.getenv("TOPIC_POLICIA"),
                help='Pub/Sub topic for police notifications.')
    parser.add_argument(
                '--alertas_agresores_topic',
                required=False,
                default=os.getenv("TOPIC_AGRESORES"),
                help='Pub/Sub topic for agressor notifications.')
    parser.add_argument(
                '--alertas_victimas_topic',
                required=False,
                default=os.getenv("TOPIC_VICTIMAS"),
                help='Pub/Sub topic for victim notifications.')

    
    parser.add_argument(
                '--firestore_collection',
                required=True,
                help='Firestore collection name.')
    
    parser.add_argument(
                '--bigquery_dataset',
                required=True,
                help='BigQuery dataset name.')
    
    parser.add_argument(
                '--user_bigquery_table',
                required=True,
                help='User BigQuery table name.')
    
    parser.add_argument(
                '--episode_bigquery_table',
                required=True,
                help='Episode BigQuery table name.')
    
    args, pipeline_opts = parser.parse_known_args()


    # Configuración estándar
    options = PipelineOptions(streaming=True, project=os.getenv("PROJECT_ID"))
    options.view_as(StandardOptions).runner = 'DirectRunner'
    
    # Nombres de suscripciones
    sub_v = f"projects/{os.getenv('PROJECT_ID')}/subscriptions/{os.getenv('SUBSCRIPTION_VICTIMAS')}"
    sub_a = f"projects/{os.getenv('PROJECT_ID')}/subscriptions/{os.getenv('SUBSCRIPTION_AGRESORES')}"

    with beam.Pipeline(options=options) as p:

        side_db = (
                    p
                    | "Reloj" >> GenerateSequence(start=0, stop=None, period=60)
                    | "VentanaReloj" >> beam.WindowInto(FixedWindows(60))
                    | "CargarDB" >> beam.ParDo(CargarDatosMaestros())
                    | "VistaSingleton" >> beam.View.AsSingleton(element_default={}) 
                )



        victimas = (
            p 
            | "LeerVictimas" >> beam.io.ReadFromPubSub(subscription=sub_v)
            | "ParsearV" >> beam.Map(parsePubSubMessage)
            | "FormatearV" >> beam.FlatMap(normalizeVictimas)
            | "CruzarDB" >> beam.FlatMap(cruzar_datos_en_memoria, datos_maestros=side_db)
            # Salida: ('ag_001', {datos_victima})
        )
        agresores = (
            p 
            | "LeerAgresores" >> beam.io.ReadFromPubSub(subscription=sub_a)
            | "ParsearA" >> beam.Map(parsePubSubMessage)
            | "FormatearA" >> beam.FlatMap(normalizeAgresores)
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
        )

if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    print("🚀 Arrancando Pipeline Simplificado...")
    run()