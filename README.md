
# 🛡️ Sistema de Monitorización Policial en Tiempo Real (Data Project 2)

Un sistema integral de datos en tiempo real diseñado para la monitorización, control y gestión de alertas por órdenes de alejamiento. Este proyecto combina procesamiento de eventos en *streaming* con sincronización de bases de datos relacionales para ofrecer un panel de control instantáneo y analítico a las fuerzas de seguridad.

## 🏗️ Arquitectura del Sistema

![Arquitectura del Sistema](arquitectura/diagrama.svg)

La arquitectura está desplegada íntegramente en **Google Cloud Platform (GCP)** y sigue un modelo híbrido combinando Streaming y Batch, separando el flujo de telemetría del flujo transaccional.

### 1. Ingesta y Generación de Datos
* **Simulador Python:** Un generador de coordenadas geográficas simula el movimiento de dispositivos GPS (pulseras de agresores y móviles de víctimas) usando redes de calles reales mediante `osmnx`.
* **API REST (Cloud Run):** Desarrollada en Flask. Actúa como puerta de entrada. Recibe la telemetría (POST) y expone endpoints CRUD (GET, POST, PUT) para que el Dashboard gestione los perfiles.

### 2.(Streaming de Coordenadas)
* **Pub/Sub:** Actúa como bus de mensajería para desacoplar la ingesta del procesamiento. Separa los tópicos de víctimas y agresores.
* **Dataflow:** Procesa los datos en tiempo real, calcula distancias y manda alertas si se vulnera el perímetro de seguridad. Escribe los resultados en **Firestore** (para el mapa en tiempo real) y en **BigQuery** (para procesamiento analítico).

### 3.(Gestión Relacional y CDC)
* **Cloud SQL (PostgreSQL):** Base de datos maestra que almacena las entidades (`victimas`, `agresores`, `safe_places`) y sus relaciones. Asegurada mediante IP Privada y *Allowlist* para conexiones externas.
* **Datastream:** Sincroniza la base de datos operativa con el Data Warehouse en tiempo real mediante *Change Data Capture (CDC)* y el método *Merge*, manteniendo un espejo exacto en BigQuery.

### 4. Transformación y Data Warehousing (dbt)
* **BigQuery + dbt:** Modularización del modelado de datos estructurado en tres capas analíticas:
  * **Staging:** Limpieza inicial de datos crudos (procedentes de Datastream y Dataflow), tratamiento de nulos y *casteo* de tipos (ej. conversión de coordenadas en *string* a objetos espaciales `GEOGRAPHY` nativos de BigQuery).
  * **Intermediate:** Modelos de cruce y lógica de negocio. Se pre-procesan y unen las entidades relacionales (por ejemplo, consolidando víctimas, agresores y sus órdenes de alejamiento en una sola vista intermedia) para optimizar el rendimiento de las consultas finales.
  * **Marts:** Capa de consumo final para visualización. Tablas anchas y desnormalizadas donde se enriquece el flujo de *streaming* de alertas con el contexto policial (fotos, nombres completos, lugares seguros recomendados y límites legales), listas para ser leídas por el Dashboard sin latencia ni cruces complejos en tiempo real.

### 5. Visualización y Consumo
* **Streamlit (Cloud Run):** Panel de control interactivo para la policía.
  * Se conecta vía **Websockets / Firestore** para ver a los actores moverse en el mapa en tiempo real.
  * Llama a la **API** para leer y editar perfiles de la base de datos relacional.
  * Lee directamente desde **Google Cloud Storage (Buckets)** para renderizar de forma segura las fotografías de víctimas y agresores.

---

## 🛠️ Stack Tecnológico

* **Lenguajes:** Python, SQL
* **GCP Data & Analytics:** BigQuery, Dataflow, Datastream, Pub/Sub, Cloud Storage
* **GCP Compute & Database:** Cloud Run, Cloud SQL (PostgreSQL), Firestore
* **Transformación:** dbt (Data Build Tool)
* **Frontend:** Streamlit
* **Infraestructura como Código (IaC):** Terraform *(Implementado para orquestación de recursos)*


---
*Proyecto desarrollado para demostrar capacidades avanzadas en Data Engineering, Streaming Processing y Cloud Architecture.*