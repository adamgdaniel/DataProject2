import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, SetupOptions
import os
from dotenv import load_dotenv

# Cargar variables
load_dotenv()
PROJECT_ID = os.getenv("PROJECT_ID")
SUB_V = os.getenv("SUBSCRIPTION_VICTIMAS")
SUB_A = os.getenv("SUBSCRIPTION_AGRESORES")

print(f"🕵️‍♂️ DIAGNÓSTICO DE CONEXIÓN")
print(f"Proyecto: {PROJECT_ID}")
print(f"Escuchando Suscripción VÍCTIMAS: {SUB_V}")
print(f"Escuchando Suscripción AGRESORES: {SUB_A}")
print("------------------------------------------------")

def imprimir_mensaje(msg):
    # Imprimimos el mensaje crudo, tal cual llega
    raw = msg.decode('utf-8')
    print(f"🔔 ¡LLEGÓ ALGO! -> {raw}")
    return raw

def run():
    options = PipelineOptions(streaming=True, project=PROJECT_ID)
    # Runner Directo para ver los prints en pantalla
    options.view_as(StandardOptions).runner = 'DirectRunner'
    
    with beam.Pipeline(options=options) as p:
        # Tubería simple: Leer -> Imprimir
        (
            p 
            | "LeerV" >> beam.io.ReadFromPubSub(subscription=f"projects/{PROJECT_ID}/subscriptions/{SUB_V}")
            | "PrintV" >> beam.Map(imprimir_mensaje)
        )

        (
            p 
            | "LeerA" >> beam.io.ReadFromPubSub(subscription=f"projects/{PROJECT_ID}/subscriptions/{SUB_A}")
            | "PrintA" >> beam.Map(imprimir_mensaje)
        )

if __name__ == '__main__':
    from apache_beam.options.pipeline_options import StandardOptions
    run()