import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions
from apache_beam.transforms.window import FixedWindows
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

def detectar_match(elemento):
    """
    Recibe: ('ag_001', [lista_de_eventos_mezclados])
    Calcula distancias entre el agresor y las víctimas de ese grupo.
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
    alertas = []
    for vic in victimas:
        #Pasamos de coordenadas a metros
        dist = geodesic(datos_agresor['coords'], vic['coords']).meters
        
        if dist < 500: 
            mensaje = f"🚨 MATCH CONFIRMADO: {id_agresor} está a {dist}m de {vic['user_id']}"
            print(mensaje) 
            alertas.append(mensaje)
            
    return alertas


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
                '--policia_pubsub_subscription_name',
                required=True,

                help='Pub/Sub subscription for quality events.')

    parser.add_argument(
                '--notifications_pubsub_topic_name',
                required=True,
                help='Pub/Sub topic for push notifications.')
    
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

        victimas = (
            p 
            | "LeerVictimas" >> beam.io.ReadFromPubSub(subscription=sub_v)
            | "ParsearV" >> beam.Map(parsePubSubMessage)
            | "FormatearV" >> beam.FlatMap(normalizeVictimas)
            | "BuscarEnDB" >> beam.ParDo(EnriquecerConDatosDB()) 
            # Salida: ('ag_001', {datos_victima})
        )
        agresores = (
            p 
            | "LeerAgresores" >> beam.io.ReadFromPubSub(subscription=sub_a)
            | "ParsearA" >> beam.Map(parsePubSubMessage)
            | "FormatearA" >> beam.FlatMap(normalizeAgresores)
            # Salida: ('ag_001', {datos_agresor})
        )

        # UNIÓN Y MATCHING
        (
            (victimas, agresores)
            | "UnirTodo" >> beam.Flatten()
            | "Ventana15s" >> beam.WindowInto(FixedWindows(15)) # Ventana Fija
            | "Agrupar" >> beam.GroupByKey() # Junta víctima y agresor por la clave 'ag_001'
            | "Calcular" >> beam.FlatMap(detectar_match) # Función simple
        )

if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    print("🚀 Arrancando Pipeline Simplificado...")
    run()