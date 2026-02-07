resource "google_pubsub_topic" "victimas_datos" {
    name = var.topic_victimas_datos
}
resource "google_pubsub_subscription" "victimas_datos_sub" {
    name = var.subscription_victimas_datos
    topic = google_pubsub_topic.victimas_datos.name
}
resource "google_pubsub_topic" "agresores_datos" {
    name = var.topic_agresores_datos
}
resource "google_pubsub_subscription" "agresores_datos_sub" {
    name = var.subscription_agresores_datos
    topic = google_pubsub_topic.agresores_datos.name
}
resource "google_pubsub_topic" "policia_alertas" {
    name = var.topic_policia_alertas
}
resource "google_pubsub_subscription" "policia_alertas_sub" {
    name = var.subscription_policia_alertas
    topic = google_pubsub_topic.policia_alertas.name
}
resource "google_pubsub_topic" "victimas_alertas" {
    name = var.topic_victimas_alertas
}
resource "google_pubsub_subscription" "victimas_alertas_sub" {
    name = var.subscription_victimas_alertas
    topic = google_pubsub_topic.victimas_alertas.name
}
resource "google_pubsub_topic" "agresores_alertas" {
    name = var.topic_agresores_alertas
}
resource "google_pubsub_subscription" "agresores_alertas_sub" {
    name = var.subscription_agresores_alertas
    topic = google_pubsub_topic.agresores_alertas.name
}
resource "google_storage_bucket" "bucket_victimas_datos" {
    name = var.bucket_imagenes
    location = var.region
}
resource "google_firestore_database" "firestore_database" {
    name = "(default)"
    location_id = var.region
    type = "FIRESTORE_NATIVE"
}
resource "google_firestore_document" "victimas_datos_doc" {
    collection = var.firestore_collection_alertas
    document_id = "init"
    database = google_firestore_database.firestore_database.name
    fields = jsonencode({
        timestamp = {
            string_value = "${formatdate("YYYY-MM-DD'T'hh:mm:ss.000Z", timestamp())}"
        }
    })
}
resource "google_firestore_document" "agresores_datos_doc" {
    collection = var.firestore_collection_safezones
    document_id = "init"
    database = google_firestore_database.firestore_database.name
    fields = jsonencode({
        timestamp = {
            string_value = "${formatdate("YYYY-MM-DD'T'hh:mm:ss.000Z", timestamp())}"
        }
    })
}
resource "google_bigquery_dataset" "bigquery_dataset" {
    dataset_id = var.analitical_dataset
    project = var.project_id
    location = var.region
}