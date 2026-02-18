terraform {
  backend "gcs" {
    bucket  = "bucket-state-tf-dp2"
  }
}
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
# resource "google_pubsub_topic" "policia_alertas" {
#     name = var.topic_policia_alertas
# }
# resource "google_pubsub_subscription" "policia_alertas_sub" {
#     name = var.subscription_policia_alertas
#     topic = google_pubsub_topic.policia_alertas.name
# }
# resource "google_pubsub_topic" "victimas_alertas" {
#     name = var.topic_victimas_alertas
# }
# resource "google_pubsub_subscription" "victimas_alertas_sub" {
#     name = var.subscription_victimas_alertas
#     topic = google_pubsub_topic.victimas_alertas.name
# }
# resource "google_pubsub_topic" "agresores_alertas" {
#     name = var.topic_agresores_alertas
# }
# resource "google_pubsub_subscription" "agresores_alertas_sub" {
#     name = var.subscription_agresores_alertas
#     topic = google_pubsub_topic.agresores_alertas.name
# }
resource "google_storage_bucket" "bucket_victimas_datos" {
    name = var.bucket_imagenes
    location = var.region
}
resource "google_firestore_database" "firestore_database" {
    name = var.firestore_database
    location_id = var.region
    type = "FIRESTORE_NATIVE"
    project = var.project_id
}
resource "google_firestore_document" "doc_inicializacion" {
  project     = var.project_id
  database    = var.firestore_database
  collection  = var.firestore_collection_alertas
  document_id = "inicializacion_terraform" 
  fields = jsonencode({
    "id_victima": {
      "stringValue": "dummy_victima_000"
    },
    "id_agresor": {
      "stringValue": "dummy_agresor_000"
    },
    "distancia": {
      "integerValue": "0"
    },
    "tipo_alerta": {
      "stringValue": "inicializacion" # Para que no sea ni roja ni ámbar
    },
    "coordenadas_victima": {
      "geoPointValue": {
        "latitude": 40.4168,
        "longitude": -3.7038
      }
    },
    "coordenada_agresor": {
      "geoPointValue": {
        "latitude": 40.4168,
        "longitude": -3.7038
      }
    }
  })
}

resource "google_bigquery_dataset" "bigquery_dataset" {
    dataset_id = var.analitical_dataset
    project = var.project_id
    location = var.region
}
resource "google_sql_database_instance" "cloud_sql_instance" {
    name = var.cloud_sql_instance_name
    database_version = "POSTGRES_15"
    region = var.region
    deletion_protection = false
    settings {
        tier = "db-f1-micro"
    }
}
resource "google_sql_database" "database"{
    name = var.cloud_sql_instance_name
    instance = google_sql_database_instance.cloud_sql_instance.name
}
resource "google_sql_user" "db_user" {
    name = var.db_user
    instance = google_sql_database_instance.cloud_sql_instance.name
    password = var.db_password
}
resource "google_service_account" "cloud_run" {
    account_id = "crear-tablas"
    project = var.project_id
    display_name = "Service account para Cloud run"
}
resource "google_project_iam_member" "cloud_run_roles" {
    for_each = toset([
    "roles/logging.logWriter",
    "roles/cloudsql.client"
  ])
    project = var.project_id
    role    = each.value 
    member = "serviceAccount:${google_service_account.cloud_run.email}"
}

resource "google_cloud_run_v2_job" "crear_tablas" {
    name = "crear-tablas"
    location = var.region
    deletion_protection = false
    template {
        template{
            service_account = google_service_account.cloud_run.email
            containers {
                image = var.container_image
                args = [
                    var.db_user, var.db_password, google_sql_database.database.name, 
                    google_sql_database_instance.cloud_sql_instance.connection_name
                ]
                volume_mounts {
                  name = "cloudsql"
                  mount_path = "/cloudsql"
                }
            }
            volumes {
                name = "cloudsql"
                cloud_sql_instance {
                    instances = [google_sql_database_instance.cloud_sql_instance.connection_name]
                }
            }
        }
    }
    lifecycle {
        ignore_changes = [
            # Ignora cambios en la imagen (porque Cloud Build pondrá una nueva con cada commit)
            template[0].template[0].containers[0].image,
            
            client,
            client_version,
            launch_stage
        ]
    }
    depends_on = [ google_project_iam_member.cloud_run_roles, google_sql_user.db_user ]
}

# resource "null_resource" "ejecutar_job" {
#   # Solo se ejecutará si el Job se crea o cambia
#   triggers = {
#     job_id = google_cloud_run_v2_job.init_db_job.id
#   }

#   provisioner "local-exec" {
#     # Este comando se ejecuta en tu máquina al hacer el apply
#     # --wait asegura que Terraform no termine hasta que las tablas estén creadas
#     command = "gcloud run jobs execute ${google_cloud_run_v2_job.init_db_job.name} --region ${var.region} --project ${var.project_id} --wait"
#   }
# }
resource "google_artifact_registry_repository" "mi_repo" {
  location      = var.region
  repository_id = "repo-imagenes-proyecto"
  description   = "Repositorio Docker para Cloud Run"
  format        = "DOCKER"
}

resource "google_service_account" "cloudbuild_sa" {
  account_id   = "my-build-sa"
  display_name = "Service Account para Cloud Build (Terraform)"
  description  = "Cuenta con permisos mínimos para desplegar la DB"
}

resource "google_project_iam_member" "build_sa_roles" {
  for_each = toset([
    "roles/logging.logWriter",
    "roles/artifactregistry.writer",
    "roles/run.developer",
    "roles/iam.serviceAccountUser",
    "roles/storage.objectViewer",
    "roles/cloudbuild.builds.builder",
    "roles/developerconnect.readTokenAccessor"
  ])

  project = var.project_id
  role    = each.value
  member  = "serviceAccount:${google_service_account.cloudbuild_sa.email}"
}

resource "google_service_account" "firestore_sa2" {
  account_id   = "firestore-sa"
  display_name = "Service Account para Firestore"
  description  = "Cuenta con permisos mínimos para leer en Firestore"
}

resource "google_project_iam_member" "firestore_sa_roles" {
  project = var.project_id
  role    = "roles/datastore.viewer"
  member  = "serviceAccount:${google_service_account.firestore_sa2.email}"
}
resource "google_service_account_key" "firestore_sa_key" {
  service_account_id = google_service_account.firestore_sa2.name
  public_key_type    = "TYPE_X509_PEM_FILE"
}
resource "local_file" "service_account_json" {
  content  = base64decode(google_service_account_key.firestore_sa_key.private_key)
  filename = "${path.module}/firestore-key.json"
}
resource "google_storage_bucket" "dataflow_bucket"{
  location = var.region
  name = var.bucket_dataflow
}
resource "google_service_account" "dataflow_sa" {
  account_id   = "dataflow-sa"
  display_name = "Service Account para Dataflow"
  description  = "Cuenta con permisos mínimos para ejecutar Dataflow"
}
resource "google_project_iam_member" "dataflow_sa_roles" {
  for_each = toset([
    "roles/pubsub.subscriber",
    "roles/datastore.user",
    "roles/dataflow.worker",     
    "roles/bigquery.jobUser",
    "roles/bigquery.dataEditor"
  ])
  project = var.project_id
  role    = each.value
  member  = "serviceAccount:${google_service_account.dataflow_sa.email}"
}
resource "google_cloudbuild_trigger" "dataflow_deploy_trigger" {
  name        = "deploy-dataflow-on-push"
  description = "Despliega/Actualiza Dataflow al hacer push a main"
  project     = var.project_id
  github {
    owner = var.github_owner
    name  = var.github_repo
    push {
      branch = "^main$"
    }
  }
  filename = "dataflow/cloudbuild.yaml"
  substitutions = {
    _SERVICE_ACCOUNT = var.dataflow_sa_email
    _REGION          = var.region
  }
  included_files = ["dataflow/**"]
}