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



def normalizeQualityEvent(event):

    """
    Normalize quality event data.
    Args:
        event (dict): Raw quality event data.
    Returns:
        dict: Normalized quality event data.
    """

    w = {
        "BUFFERING_START": -1, 
        "BUFFERING_END": -0.2, 
        "DROPOUT": -2
    }.get(event["event_type"], 0)
    
    return {
        "user_id": event["user_id"],
        "episode_id": event["episode_id"],
        "type": event["event_type"].lower(),
        "w": w,
    }


class FormatFirestoreDocument(beam.DoFn):

    def __init__(self,firestore_collection, project_id):
        self.firestore_collection = firestore_collection
        self.project_id = project_id

    def setup(self):
        from google.cloud import firestore
        self.db = firestore.Client(project=self.project_id)

    def process(self, element):

        doc_ref = self.db.collection(self.firestore_collection).document(element['user_id']).collection('notifications').document(element['notification_id'])
        doc_ref.set(element)

        logging.info(f"Document written to Firestore: {doc_ref.id}")

        yield element

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
                required=True,
                help='Pub/Sub topic para mandar notificaciones a la Policía.')
    
    parser.add_argument(
                '--firestore_collection',
                required=True,
                help='Firestore collection name.')
    
    parser.add_argument(
                '--bigquery_dataset',
                required=True,
                help='BigQuery dataset para ingestar matches.')
    
    parser.add_argument(
                '--user_bigquery_table',
                required=True,
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
        )

        leer_victimas = (
            p
                | "ReadFromVictimas" >> beam.io.ReadFromPubSub(
                    subscription=f"projects/{args.project_id}/subscriptions/{args.victimas_pubsub_subscription_name}")
                | "ParseVictimasMessages" >> beam.Map(parsePubSubMessage)
        )

        leer_safeplaces = (
            p
                | "ReadFromFirestore" >> beam.io.ReadFromFirestore(
                    subscription=f"projects/{args.project_id}/subscriptions/{args.quality_pubsub_subscription_name}")
                | "ParseQualityMessages" >> beam.Map(parsePubSubMessage)
                | "NormalizeQualityEvents" >> beam.Map(normalizeQualityEvent)
        )

        all_events = (leer_victimas, leer_agresores, leer_safeplaces) | "MergeEvents" >> beam.Flatten()

        # A. User real-time metrics (Session-based)
        user_data = (
            all_events
                | "WindowIntoSessions" >> beam.WindowInto(Sessions(gap_size=30))
                | "KeyByUserId" >> beam.Map(lambda x: (x["user_id"], x))
                | "GroupByUserId" >> beam.GroupByKey()
                | "ComputeUserMetrics" >> beam.ParDo(UserMetricsFn()).with_outputs(UserMetricsFn.METRICS, UserMetricsFn.NOTIFY)
        )

        (
            user_data.metrics
                | "WriteUserMetricsToBigQuery" >> beam.io.WriteToBigQuery(
                        project=args.project_id,
                        dataset=args.bigquery_dataset,
                        table=args.user_bigquery_table,
                        schema='user_id:STRING, window_start:TIMESTAMP, window_end:TIMESTAMP, plays:INTEGER, completes:INTEGER, score:FLOAT',
                        write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
                        create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED
                    )
        )

        (
            user_data.notify
                | "WriteToFirestore" >> beam.ParDo(FormatFirestoreDocument(firestore_collection=args.firestore_collection, project_id=args.project_id))
        )

        (
            user_data.notify
                | "EncodeUserNotifications" >> beam.Map(lambda x: json.dumps(x).encode('utf-8'))
                | "WriteUserNotificationsToPubSub" >> beam.io.WriteToPubSub(
                    topic=f"projects/{args.project_id}/topics/{args.notifications_pubsub_topic_name}"
                )
        )

        # B. Content real-time metrics (Sliding window-based)
        content_data = (
            all_events
                | "WindowIntoSliding" >> beam.WindowInto(SlidingWindows(size=60, period=10))
                | "KeyByEpisodeId" >> beam.Map(lambda x: (x["episode_id"], x))
                | "GroupByEpisodeId" >> beam.GroupByKey()
                | "ComputeContentMetrics" >> beam.ParDo(ContentMetricsFn()).with_outputs(ContentMetricsFn.METRICS, ContentMetricsFn.NOTIFY)
        )
          
        (content_data.metrics 
                | "WriteToBigQuery" >> beam.io.WriteToBigQuery(
                        project=args.project_id,
                        dataset=args.bigquery_dataset,
                        table=args.episode_bigquery_table,
                        schema='episode_id:STRING, window_start:TIMESTAMP, window_end:TIMESTAMP, plays:INTEGER, completes:INTEGER, score:FLOAT',
                        write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
                        create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED
                    )
        )

if __name__ == '__main__':

    # Set Logs
    logging.basicConfig(level=logging.INFO)

    # Disable logs from apache_beam.utils.subprocess_server
    logging.getLogger("apache_beam.utils.subprocess_server").setLevel(logging.ERROR)

    logging.info("The process started")

    # Run Process
    run()