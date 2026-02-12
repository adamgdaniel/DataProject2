import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, SetupOptions, StandardOptions
from apache_beam.transforms.window import FixedWindows
import logging
import json
import os
from geopy.distance import geodesic
from dotenv import load_dotenv

# Librerías de Base de Datos
from google.cloud.sql.connector import Connector, IPTypes
import sqlalchemy

# 1. Cargamos variables del .env
load_dotenv()
PROJECT_ID = os.getenv("PROJECT_ID")
SUBSCRIPTION_VICTIMAS = os.getenv("SUBSCRIPTION_VICTIMAS")
SUBSCRIPTION_AGRESORES = os.getenv("SUBSCRIPTION_AGRESORES")

# --- PASO 1: LEER LA BASE DE DATOS (Side Input) ---
# Esto se ejecuta una vez al principio para saber quién no puede acercarse a quién.

class LeerReglasDB(beam.DoFn):
    def process(self, element):
        # Nos conectamos a Cloud SQL
        connector = Connector()
        def getconn():
            return connector.connect(
                os.getenv("INSTANCE_CONNECTION_NAME"),
                "pg8000",
                user=os.getenv("DB_USER"),
                password=os.getenv("DB_PASS"),
                db=os.getenv("DB_NAME"),
                ip_type=IPTypes.PUBLIC
            )
        
        # Creamos la conexión y lanzamos la consulta
        engine = sqlalchemy.create_engine("postgresql+pg8000://", creator=getconn)
        mapa_relaciones = {}
        
        with engine.connect() as conn:
            # Traemos la tabla de relaciones
            result = conn.execute(sqlalchemy.text("SELECT id_victima, id_agresor FROM rel_victimas_agresores"))
            
            # Construimos un diccionario: { "vi_001": ["ag_001", "ag_002"] }
            for row in result:
                vic, agr = str(row[0]), str(row[1])
                if vic not in mapa_relaciones: mapa_relaciones[vic] = []
                mapa_relaciones[vic].append(agr)
        
        logging.info(f"Base de datos cargada: {len(mapa_relaciones)} víctimas protegidas")
        yield mapa_relaciones

# --- PASO 2: FUNCIONES SIMPLES (Lógica) ---

def normalizar_victima(mensaje):
    """Convierte JSON a Diccionario y Coordenadas a Tupla"""
    datos = json.loads(mensaje.decode('utf-8'))
    return {
        "id": datos["user_id"],
        "coords": tuple(datos["coordinates"]), # <--- IMPORTANTE: Tupla para geopy
        "timestamp": datos["timestamp"],
        "tipo": "victima"
    }

def normalizar_agresor(mensaje):
    """Igual, pero marcando como agresor"""
    datos = json.loads(mensaje.decode('utf-8'))
    return {
        "id": datos["user_id"],
        "coords": tuple(datos["coordinates"]),
        "timestamp": datos["timestamp"],
        "tipo": "agresor"
    }

def asignar_claves_victima(victima, mapa_relaciones):
    """
    Si entra 'Ana' (vi_001), buscamos en la BD quiénes son sus agresores.
    Si su agresor es 'Carlos' (ag_001), emitimos: ('ag_001', datos_de_ana)
    """
    lista_agresores = mapa_relaciones.get(victima['id'], [])
    for id_agresor in lista_agresores:
        yield (id_agresor, victima)

def detectar_match(elemento):
    """
    Recibe: ('ag_001', [lista_de_cosas_que_han_pasado_en_15s])
    """
    id_agresor, eventos = elemento

    # 1. Buscar dónde está el agresor ahora mismo
    datos_agresor = next((e for e in eventos if e['tipo'] == 'agresor'), None)
    if not datos_agresor: return # Si el agresor no está conectado, no hay peligro

    # 2. Buscar todas las víctimas que han aparecido en este grupo
    victimas_cerca = [e for e in eventos if e['tipo'] == 'victima']

    # 3. Calcular distancias
    for v in victimas_cerca:
        metros = geodesic(datos_agresor['coords'], v['coords']).meters
        
        # SI ESTÁ A MENOS DE 500 METROS -> ¡MATCH!
        if metros < 500:
            mensaje = f"🚨 ALERTA: {id_agresor} está a {metros:.2f}m de {v['id']}"
            logging.info(mensaje) # Imprimimos por consola
            yield mensaje

# --- PASO 3: EL PIPELINE (La Tubería) ---

def run():
    options = PipelineOptions(streaming=True, project=PROJECT_ID)
    options.view_as(SetupOptions).save_main_session = True # Para que las funciones viajen bien
    
    with beam.Pipeline(options=options) as p:
        
        # A. Cargar la Base de Datos (Side Input)
        reglas_bd = (
            p 
            | "Inicio" >> beam.Create([None])
            | "LeerDB" >> beam.ParDo(LeerReglasDB())
        )
        vista_bd = beam.pvalue.AsSingleton(reglas_bd)

        # B. Procesar Víctimas
        victimas = (
            p 
            | "LeerVictimas" >> beam.io.ReadFromPubSub(subscription=f"projects/{PROJECT_ID}/subscriptions/{SUBSCRIPTION_VICTIMAS}")
            | "NormVictimas" >> beam.Map(normalizar_victima)
            | "CruzarConBD"  >> beam.FlatMap(asignar_claves_victima, vista_bd)
        )

        # C. Procesar Agresores
        agresores = (
            p 
            | "LeerAgresores" >> beam.io.ReadFromPubSub(subscription=f"projects/{PROJECT_ID}/subscriptions/{SUBSCRIPTION_AGRESORES}")
            | "NormAgresores" >> beam.Map(normalizar_agresor)
            | "ClaveAgresor"  >> beam.Map(lambda x: (x['id'], x)) # La clave es su propio ID
        )

        # D. Juntar, Ventana y Match
        (
            (victimas, agresores)
            | "JuntarTodo"    >> beam.Flatten()
            | "Ventana15s"    >> beam.WindowInto(FixedWindows(15)) # Ventana de 15 segundos
            | "Agrupar"       >> beam.GroupByKey()
            | "BuscarPeligro" >> beam.FlatMap(detectar_match)
        )

if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    print("🚀 Arrancando Dataflow Simplificado...")
    run()