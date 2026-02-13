from email.mime import message
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions
from apache_beam.transforms.window import FixedWindows
import logging
import json
import os
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

class EnriquecerConDatosDB(beam.DoFn):

    def start_bundle(self):
        self.conn = psycopg2.connect(
            host=os.getenv("DB_HOST"), # ¡OJO! Añade esto a tu .env (IP pública de SQL)
            user=os.getenv("DB_USER"),
            password=os.getenv("DB_PASS"),
            dbname=os.getenv("DB_NAME")
        )

    def process(self, datos_victima):
        if not datos_victima: return

        # 1. Consultar a la BD quiénes son los agresores de esta víctima
        cursor = self.conn.cursor()
        query = "SELECT id_agresor FROM rel_victimas_agresores WHERE id_victima = %s"
        cursor.execute(query, (datos_victima['user_id'],))
        resultados = cursor.fetchall()
        
        # 2. Etiquetar a la víctima con el ID de sus agresores
        datos_victima['tipo'] = 'victima'
        
        if not resultados:
            logging.info(f"Víctima {datos_victima['user_id']} no tiene agresores en BD.")

        for row in resultados:
            id_agresor = row[0].lower() # ej: 'ag_001'
            # Emitimos: (CLAVE_AGRESOR, DATOS_VICTIMA)
            yield (id_agresor, datos_victima)

    def finish_bundle(self):
        # Cerrar conexión al terminar
        self.conn.close()


### PIPELINE

def run():
    # Configuración estándar
    options = PipelineOptions(streaming=True, project=os.getenv("PROJECT_ID"))
    options.view_as(StandardOptions).runner = 'DirectRunner'
    
    # Nombres de suscripciones
    sub_v = f"projects/{os.getenv('PROJECT_ID')}/subscriptions/{os.getenv('SUBSCRIPTION_VICTIMAS')}"
    sub_a = f"projects/{os.getenv('PROJECT_ID')}/subscriptions/{os.getenv('SUBSCRIPTION_AGRESORES')}"

    with beam.Pipeline(options=options) as p:

        # RAMA 1: Procesar Víctimas
        # Leemos -> Parseamos (Función) -> Consultamos DB (Clase)
        victimas = (
            p 
            | "LeerVictimas" >> beam.io.ReadFromPubSub(subscription=sub_v)
            | "ParsearV" >> beam.Map(parsear_mensaje)
            | "BuscarEnDB" >> beam.ParDo(EnriquecerConDatosDB()) 
            # Salida: ('ag_001', {datos_victima})
        )

        # RAMA 2: Procesar Agresores
        # Leemos -> Parseamos (Función) -> Preparamos Clave (Función)
        agresores = (
            p 
            | "LeerAgresores" >> beam.io.ReadFromPubSub(subscription=sub_a)
            | "ParsearA" >> beam.Map(parsear_mensaje)
            | "FormatearA" >> beam.FlatMap(formatear_agresor)
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