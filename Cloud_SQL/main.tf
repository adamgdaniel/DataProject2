# 1. La Instancia de Cloud SQL
resource "google_sql_database_instance" "av_instance" {
  name             = "av-instance-v1"
  database_version = "POSTGRES_14"
  region           = "europe-west6" 
  deletion_protection = true       # CUIDADO: Solo para desarrollo

  settings {
    tier = "db-f1-micro"
    
    ip_configuration {
      ipv4_enabled = true
      
      # Permitir conexión desde cualquier lugar (para que Terraform funcione desde tu PC)
      authorized_networks {
        name  = "internet"
        value = "0.0.0.0/0"
      }
    }
  }
}

# 2. La Base de Datos
resource "google_sql_database" "av_database" {
  name     = "av_database"
  instance = google_sql_database_instance.av_instance.name
}

# 3. El Usuario Admin
resource "google_sql_user" "admin_user" {
  name     = username_db_sql
  instance = google_sql_database_instance.av_instance.name
  password = pass_db_sql 
}
