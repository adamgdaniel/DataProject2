
resource "google_project_service" "datastream_api" {
  project = var.project_id
  service = "datastream.googleapis.com"

  disable_on_destroy = false

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

resource "google_bigquery_table" "tabla_alertas" {
  dataset_id          = google_bigquery_dataset.bigquery_dataset.dataset_id
  table_id            = "alertas"
  deletion_protection = false 

  table_constraints {
    primary_key {
      columns = ["alerta"]
    }
  }

  time_partitioning {
    type  = "HOUR"
    field = "timestamp"
  }

  # Esquema completo de la tabla
  schema = <<EOF
[
  {
    "name": "alerta",
    "type": "STRING",
    "mode": "REQUIRED"
  },
  {
    "name": "activa",
    "type": "BOOLEAN",
    "mode": "NULLABLE"
  },
  {
    "name": "nivel",
    "type": "STRING",
    "mode": "NULLABLE"
  },
  {
    "name": "id_victima",
    "type": "STRING",
    "mode": "NULLABLE"
  },
  {
    "name": "id_agresor",
    "type": "STRING",
    "mode": "NULLABLE"
  },
  {
    "name": "distancia_metros",
    "type": "FLOAT",
    "mode": "NULLABLE"
  },
  {
    "name": "direccion_escape",
    "type": "STRING",
    "mode": "NULLABLE"
  },
  {
    "name": "coordenadas_agresor",
    "type": "STRING",
    "mode": "NULLABLE"
  },
  {
    "name": "coordenadas_victima",
    "type": "STRING",
    "mode": "NULLABLE"
  },
  {
    "name": "coordenadas_place",
    "type": "STRING",
    "mode": "NULLABLE"
  },
  {
    "name": "timestamp",
    "type": "TIMESTAMP",
    "mode": "REQUIRED"
  },
  {
    "name": "dist_seguridad",
    "type": "FLOAT",
    "mode": "NULLABLE"
  },
  {
    "name": "distancia_limite",
    "type": "FLOAT",
    "mode": "NULLABLE"
  },
  {
    "name": "id_place",
    "type": "STRING",
    "mode": "NULLABLE"
  },
  {
    "name": "nombre_place",
    "type": "STRING",
    "mode": "NULLABLE"
  },
  {
    "name": "radio_zona",
    "type": "FLOAT",
    "mode": "NULLABLE"
  }
]
EOF
}
resource "google_compute_global_address" "datastream_range_nuevo" {
  name          = "rango-datastream-v4"
  purpose       = "VPC_PEERING"
  address_type  = "INTERNAL"
  prefix_length = 16
  network       = "projects/${var.project_id}/global/networks/default"
}
# resource "google_service_networking_connection" "private_vpc_connection" {
#   network                 = "projects/${var.project_id}/global/networks/default"
#   service                 = "servicenetworking.googleapis.com"
#   reserved_peering_ranges = [google_compute_global_address.datastream_range_nuevo.name ]
# }

resource "google_sql_database_instance" "cloud_sql_instance" {
    name = var.cloud_sql_instance_name
    database_version = "POSTGRES_15"
    region = var.region
    deletion_protection = false
    settings {
        tier = "db-f1-micro"
        database_flags {
          name = "cloudsql.logical_decoding"
          value = "on"
          }
        ip_configuration {
            ipv4_enabled    = false
            private_network = "projects/${var.project_id}/global/networks/default" # La red VPC
            enable_private_path_for_google_cloud_services = true
        }
    }
    #depends_on = [google_service_networking_connection.private_vpc_connection]
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
    name                = "crear-tablas"
    location            = var.region
    deletion_protection = false

    template {
        template {
            service_account = google_service_account.cloud_run.email
            # vpc_access {
            #     network_interfaces {
            #         network    = data.google_compute_network.default.id
            #         subnetwork = data.google_compute_subnetwork.default.id
            #     }
            #     egress = "PRIVATE_RANGES_ONLY"
            # }
            containers {
                image = var.container_image
                args = [
                    var.db_user, 
                    var.db_password, 
                    google_sql_database.database.name, 
                    google_sql_database_instance.cloud_sql_instance.private_ip_address
                ]
            #     volume_mounts {
            #       name = "cloudsql"
            #       mount_path = "/cloudsql"
            #     }
            # }
            # volumes {
            #     name = "cloudsql"
            #     cloud_sql_instance {
            #         instances = [google_sql_database_instance.cloud_sql_instance.connection_name]
            #     }
            }
            
        }
    }
    lifecycle {
        ignore_changes = [
            template[0].template[0].containers[0].image,
            client,
            client_version,
            launch_stage
        ]
    }
    depends_on = [ 
        google_project_iam_member.cloud_run_roles, 
        google_sql_user.db_user,
        docker_registry_image.init_db_push,
        google_sql_database_instance.cloud_sql_instance
    ]
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

resource "docker_image" "init_db" {
  name = "${var.region}-docker.pkg.dev/${var.project_id}/${google_artifact_registry_repository.mi_repo.name}/crear_tablas:latest"
  build {
    context = path.module
    dockerfile = "Dockerfile"
  }
}

resource "docker_registry_image" "init_db_push" {
  name = docker_image.init_db.name
  keep_remotely = true
}

# --- IMAGEN 2: GENERADOR ---
locals {
  generador_hash = sha1(join("", [for f in fileset("${path.module}/../api", "**") : filesha1("${path.module}/../api/${f}")]))
}
resource "docker_image" "generador" {
  name = "${var.region}-docker.pkg.dev/${var.project_id}/${google_artifact_registry_repository.mi_repo.name}/api:latest"
  build {
    context = "${path.module}/../api"
    dockerfile = "${path.module}/../api/Dockerfile"
  }
  depends_on = [ docker_image.init_db ]
}

resource "docker_registry_image" "generador_push" {
  name = docker_image.generador.name
  keep_remotely = true
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
    "roles/developerconnect.readTokenAccessor",
    "roles/cloudbuild.builds.editor",
    "roles/storage.objectAdmin",
    "roles/dataflow.developer",
    "roles/iam.serviceAccountUser"
  ])

  project = var.project_id
  role    = each.value
  member  = "serviceAccount:${google_service_account.cloudbuild_sa.email}"
}
# data "google_project" "project" {
#   project_id = var.project_id
# }

# resource "google_project_iam_member" "default_cloudbuild_roles" {
#   for_each = toset([
#     "roles/cloudbuild.builds.editor",   
#     "roles/artifactregistry.writer",  
#     "roles/storage.objectAdmin" 
#   ])

#   project = var.project_id
#   role    = each.value
  
#   member  = "serviceAccount:${data.google_project.project.number}@cloudbuild.gserviceaccount.com"
# }
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
    "roles/bigquery.dataEditor",
    "roles/logging.viewer"
  ])
  project = var.project_id
  role    = each.value
  member  = "serviceAccount:${google_service_account.dataflow_sa.email}"
}
# resource "google_cloudbuild_trigger" "dataflow_deploy_trigger" {
#   name        = "deploy-dataflow-on-push"
#   description = "Despliega/Actualiza Dataflow al hacer push a main"
#   project     = var.project_id
#   github {
#     owner = var.github_owner
#     name  = var.github_repo
#     push {
#       branch = "^main$"
#     }
#   }
#   filename = "dataflow/cloudbuild.yaml"
#   substitutions = {
#     _SERVICE_ACCOUNT = var.dataflow_sa_email
#     _REGION          = var.region
#   }
#   included_files = ["dataflow/**"]
# }
resource "google_service_account" "api_sa" {
  account_id   = "api-backend-sa"
  display_name = "Service Account para API Cloud Run"
  description  = "Cuenta con permisos para SQL, Pub/Sub y Secret Manager"
}
  
resource "google_project_iam_member" "api_sa_roles" {
  for_each = toset([
    "roles/cloudsql.client",
    "roles/pubsub.publisher",
    "roles/secretmanager.secretAccessor",
    "roles/logging.logWriter",
    "roles/artifactregistry.writer"
  ])

  project = var.project_id
  role    = each.value
  member  = "serviceAccount:${google_service_account.api_sa.email}"
}

resource "google_cloud_run_v2_service" "api_backend" {
  name     = "api-backend-policia"
  location = var.region
  deletion_protection = false
  template {
    service_account = google_service_account.api_sa.email
    
    
    vpc_access {
      network_interfaces {
         network    = "projects/${var.project_id}/global/networks/default" 
        subnetwork = "projects/${var.project_id}/regions/${var.region}/subnetworks/default"  
      }
      egress = "PRIVATE_RANGES_ONLY" 
    }

    containers {
      image = docker_registry_image.generador_push.name
      env {
        name  = "PROJECT_ID"
        value = var.project_id
      }
      env {
        name  = "INSTANCE_CONNECTION_NAME"
        value = google_sql_database_instance.cloud_sql_instance.connection_name
      }
      env {
        name  = "DB_USER"
        value = var.db_user
      }
      env {
        name  = "DB_NAME"
        value = google_sql_database.database.name
      }
      
      env {
        name  = "DB_PASS"
        value = var.db_password 
      }
    }
  }

  lifecycle {
    ignore_changes = [
      client,
      client_version,
      launch_stage
    ]
  }

  depends_on = [google_project_iam_member.api_sa_roles,
  docker_registry_image.generador_push] 
}

resource "google_cloud_run_v2_service_iam_member" "api_invoker" {
  project  = google_cloud_run_v2_service.api_backend.project
  location = google_cloud_run_v2_service.api_backend.location
  name     = google_cloud_run_v2_service.api_backend.name
  role     = "roles/run.invoker"
  member   = "allUsers"
}
resource "google_compute_firewall" "allow_datastream_to_sql" {
  name    = "allow-datastream-to-sql"
  network = "default" 

  allow {
    protocol = "tcp"
    ports    = ["5432"]
  }
  source_ranges = ["10.2.0.0/29"] 
  

}



# 1. EL TÚNEL SECRETO (Private Connection)
# Como la BD no tiene IP pública, Datastream necesita un túnel VPN hacia tu red.
resource "google_datastream_private_connection" "private_connection" {
  display_name          = "Conexión privada Datastream"
  location              = var.region
  private_connection_id = "datastream-private-conn"

  vpc_peering_config {
    vpc    = "projects/${var.project_id}/global/networks/default"
    subnet = "10.2.0.0/29" 
  }
  depends_on = [ google_project_service.datastream_api ]
}

# 2. EL PERFIL DE ORIGEN (La conexión a Cloud SQL)
# resource "google_datastream_connection_profile" "postgres_source" {
#   display_name          = "Origen Cloud SQL"
#   location              = var.region
#   connection_profile_id = "cloudsql-source-profile"

#   postgresql_profile {
#     hostname = google_sql_database_instance.cloud_sql_instance.private_ip_address
#     port     = 5432
#     username = var.db_user
#     password = var.db_password
#     database = google_sql_database.database.name
#   }

#   # Le decimos que use el túnel que creamos arriba
#   private_connectivity {
#     private_connection = google_datastream_private_connection.private_connection.id
#   }
#   depends_on = [
#     google_project_service.datastream_api,
#     google_compute_firewall.allow_datastream_to_sql
#     ]
# }

# # 3. EL PERFIL DE DESTINO (La conexión a BigQuery)
# resource "google_datastream_connection_profile" "bigquery_dest" {
#   display_name          = "Destino BigQuery"
#   location              = var.region
#   connection_profile_id = "bigquery-dest-profile"

#   bigquery_profile {}
#   depends_on = [ google_project_service.datastream_api ]
# }

# # 4. LA TUBERÍA (El Stream que une todo)
# resource "google_datastream_stream" "cloudsql_to_bq" {
#   display_name  = "Replicacion BBDD a BigQuery"
#   location      = var.region
#   stream_id     = "replicacion-bq"
#   desired_state = "RUNNING" 

#   source_config {
#     source_connection_profile = google_datastream_connection_profile.postgres_source.id
#     postgresql_source_config {
#       replication_slot = "datastream_slot" ##funciona como un marcapáginas, para saber por donde se quedó leyendo
#       publication      = "datastream_pub"
#     }
#   }

#   destination_config {
#     destination_connection_profile = google_datastream_connection_profile.bigquery_dest.id
#     bigquery_destination_config {
#       data_freshness = "0s" # Tiempo real absoluto
#       single_target_dataset {
#         dataset_id = google_bigquery_dataset.bigquery_dataset.dataset_id
#       }
#     }
#   }
  


#   # Esto hace que copie los datos que ya existan, además de los nuevos
#   backfill_all {} 

#   # Terraform debe esperar a que el túnel exista antes de crear la tubería
#   depends_on = [
#     google_project_service.datastream_api,
#     google_datastream_private_connection.private_connection
#   ]
# }

/* =================================================================================
🚀 CONFIGURACIÓN DE DATASTREAM (CLOUD SQL -> BIGQUERY)
=================================================================================
NOTA IMPORTANTE: La replicación de datos en tiempo real mediante Datastream se 
ha configurado MANUALMENTE desde la consola de GCP y se ha excluido de Terraform.

¿POR QUÉ?
Google Cloud tiene una restricción física de red llamada "Peering Transitivo". 
Al intentar conectar Datastream -> Red VPC por defecto -> Cloud SQL (IP Privada), 
el tráfico se bloquea por defecto, provocando errores de timeout. 
Para evitar el coste y la complejidad de desplegar un proxy intermedio (HAProxy) 
en una máquina virtual, optamos por una solución híbrida más limpia y segura.

ARQUITECTURA Y OPCIONES ESCOGIDAS:

  1. Cloud SQL (Dual IP): 
      - IP Privada: Sigue siendo la vía exclusiva para nuestra API en Cloud Run.
      - IP Pública: Se activó con un "IP Allowlist" (candado de red) estricto. 
        Solo las 5 IPs oficiales de Datastream (europe-west6) tienen permiso 
        para llamar a esta puerta. Es invisible e inaccesible para el resto de internet.

  2. Perfil de Origen (PostgreSQL):
      - Conexión vía IP Pública + Allowlist.
      - Se crearon manualmente en la BD: PUBLICATION 'datastream_pub' y 
        REPLICATION SLOT 'datastream_slot' para la lectura secuencial (CDC).
      - Se seleccionaron EXCLUSIVAMENTE las 5 tablas de negocio (agresores, 
        victimas, rel_places_victimas, rel_victimas_agresores, safe_places), 
        ignorando los esquemas internos de Postgres (pg_catalog, etc.).

  3. Perfil de Destino y Stream (BigQuery):
      - Schema grouping: "Single dataset for all schemas" para centralizar todo.
      - Stream write mode: "MERGE". Elegimos Merge en lugar de Append-only para 
        mantener un espejo exacto del estado actual de las coordenadas, evitando 
        duplicar filas en el Data Warehouse con el histórico de movimientos.
      - Staleness limit: "0 seconds" (Tiempo real absoluto para las alertas).
   ================================================================================= */