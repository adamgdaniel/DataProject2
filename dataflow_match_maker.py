# A. Apache Beam Libraries
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, SetupOptions
from apache_beam.transforms.window import Sessions, SlidingWindows
from apache_beam.utils.timestamp import Timestamp

# B. Google Cloud Libraries
from google.cloud import firestore

# C. Python Libraries
import argparse
import logging
import uuid
import json
from geopy.distance import geodesic

""" Code: Helpful functions """

def parsePubSubMessage(message):

    """
    Parse Pub/Sub message from bytes to dictionary.
    Args:
        message (bytes): Pub/Sub message in bytes.
    Returns:
        dict: Parsed message as a dictionary.
    """

    message_str = message.decode('utf-8')
    message_dict = json.loads(message_str)

    logging.info(f"Parsed message: {message_dict}")

    return message_dict


def normalizeVictimas(event):
   
    return {
        "user_id": event["user_id"],
        "coordinates": event["coordinates"],
        "timestamp": event["timestamp"],
        "pareja_id": event["pareja_id"],
        "type": "victima",
    }

def normalizeAgresores(event):
  
    return {
        "user_id": event["user_id"],
        "coordinates": event["coordinates"],
        "timestamp": event["timestamp"],
        "pareja_id": event["pareja_id"],
        "type": "agresor",
    }

def calcularDistancia(match):
    coord_victima = match["coordenadas_victima"]
    coord_agresor = match["coordenadas_agresor"]

    distancia = geodesic(coord_victima, coord_agresor).meters

    print(f"La distancia es: {distancia} metros")

    if distancia < 500:
        print("¡ALERTA! RIESGO DETECTADO")

        return {
            "id_victima": match["id_victima"],
            "id_agresor": match["id_agresor"],
            "coordenadas_victima": match["coordenadas_victima"],
            "coordenadas_agresor": match["coordenadas_agresor"],
            "distancia": distancia,
            "timestamp": match["timestamp"],
            "pareja_id": match["pareja_id"],
            "type": "alerta",
        }

# class FormatFirestoreDocument(beam.DoFn):

#     def __init__(self,firestore_collection, project_id):
#         self.firestore_collection = firestore_collection
#         self.project_id = project_id

#     def setup(self):
#         from google.cloud import firestore
#         self.db = firestore.Client(project=self.project_id)

#     def process(self, element):

#         doc_ref = self.db.collection(self.firestore_collection).document(element['user_id']).collection('notifications').document(element['notification_id'])
#         doc_ref.set(element)

#         logging.info(f"Document written to Firestore: {doc_ref.id}")

#         yield element

""" Code: Dataflow Process """

def run():

    """ Input Arguments """

    parser = argparse.ArgumentParser(description=('Input arguments for the Dataflow Streaming Pipeline.'))

    parser.add_argument(
                '--project_id',
                required=True,
                help='GCP cloud project name.')
    
    parser.add_argument(
                '--victimas_pubsub_subscription_name',
                required=True,
                help='Suscripcion Pub/Sub de Víctimas.')
    
    parser.add_argument(
                '--agresores_pubsub_subscription_name',
                required=True,
                help='Suscripcion Pub/Sub de Agresores.')
    

    parser.add_argument(
                '--policia_pubsub_topic_name',
                required=False,
                help='Pub/Sub topic para mandar notificaciones a la Policía.')
    
    parser.add_argument(
                '--firestore_collection',
                required=False,
                help='Firestore collection name.')
    
    parser.add_argument(
                '--bigquery_dataset',
                required=False,
                help='BigQuery dataset para ingestar matches.')
    
    parser.add_argument(
                '--user_bigquery_table',
                required=False,
                help='Nombre de la tabla de BQ para ingestar matches.')
    
    
    args, pipeline_opts = parser.parse_known_args()

    # Pipeline Options
    options = PipelineOptions(pipeline_opts,
        streaming=True, project=args.project_id)
    
    setup = options.view_as(SetupOptions)
    setup.save_main_session = True
    
    # Pipeline Object
    with beam.Pipeline(argv=pipeline_opts,options=options) as p:

        leer_agresores = (
            p 
                | "ReadFromAgresores" >> beam.io.ReadFromPubSub(
                    subscription=f"projects/{args.project_id}/subscriptions/{args.agresores_pubsub_subscription_name}")
                | "ParseAgresoresMessages" >> beam.Map(parsePubSubMessage)
                | "NormalizeAgresoresEvents" >> beam.Map(normalizeAgresores)
        )

        leer_victimas = (
            p
                | "ReadFromVictimas" >> beam.io.ReadFromPubSub(
                    subscription=f"projects/{args.project_id}/subscriptions/{args.victimas_pubsub_subscription_name}")
                | "ParseVictimasMessages" >> beam.Map(parsePubSubMessage)
                | "NormalizeVictimasEvents" >> beam.Map(normalizeVictimas)
        )

        # leer_safeplaces = (
        #     p
        #         | "ReadFromFirestore" >> beam.io.ReadFromFirestore(
        #         | "ParseQualityMessages" >> 
        #         | "NormalizeQualityEvents" >> 
        # )

        everyone = (leer_victimas, leer_agresores) | "MergeVictimasAgresores" >> beam.Flatten()

        # A. User real-time metrics (Fixed window)
        match = (
            everyone
                | "WindowIntoSessions" >> beam.WindowInto(FixedWindows(size=60))
                | "KeyByUserId" >> beam.Map(lambda x: (x["user_id"], x))
                | "GroupByUserId" >> beam.GroupByKey()
                | "CalcularDistancia" >> beam.ParDo(calcularDistancia())
                | "FilterAlertas" >> #TO DO: Filtrar solo las alertas que cumplen con la condición de distancia
        )



if __name__ == '__main__':

    # Set Logs
    logging.basicConfig(level=logging.INFO)

    # Disable logs from apache_beam.utils.subprocess_server
    logging.getLogger("apache_beam.utils.subprocess_server").setLevel(logging.ERROR)

    logging.info("The process started")

    # Run Process
    run()