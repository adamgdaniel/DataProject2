# 1. Definición de versiones y proveedores necesarios
terraform {
  required_version = ">= 1.0"

  required_providers {
    # Proveedor para crear la infraestructura en Google
    google = {
      source  = "hashicorp/google"
      version = "~> 4.50"
    }

    # Proveedor específico para hablar SQL y crear tablas dentro de Postgres
    postgresql = {
      source  = "cyrilgdn/postgresql"
      version = "1.19.0"
    }
  }
}

# 2. Configuración del Proveedor de Google
provider "google" {
  project = id_project 
  region  = "europe-west6"          
}

# 3. Configuración del Proveedor de PostgreSQL
provider "postgresql" {
  host            = google_sql_database_instance.av_instance.public_ip_address
  port            = 5432
  database        = google_sql_database.av_database.name
  username        = google_sql_user.admin_user.name
  password        = google_sql_user.admin_user.password
  sslmode         = "require"
  superuser       = false
  connect_timeout = 15
}
