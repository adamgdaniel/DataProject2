variable "project_id"{
  description = "The ID of the project in which to create the resources."
  type = string
}
variable "region"{
    description = "The region in which to create the resources."
    type = string
}
variable "bucket_state"{
    description = "The name of the Cloud Storage bucket for state files."
    type = string
}
variable "zone"{
    description = "The zone in which to create the resources."
    type = string
}

variable "topic_victimas_datos"{
    description = "The name of the Pub/Sub topic for victim data."
    type = string
    default = "victimas-datos"
}

variable "subscription_victimas_datos"{
    description = "The name of the Pub/Sub subscription for victim data."
    type = string
    default = "victimas-datos-sub"
}

variable "topic_agresores_datos"{
    description = "The name of the Pub/Sub topic for aggressor data."
    type = string
    default = "agresores-datos"
}

variable "subscription_agresores_datos"{
    description = "The name of the Pub/Sub subscription for aggressor data."
    type = string
    default = "agresores-datos-sub"
}
variable "bucket_imagenes"{
    description = "The name of the Cloud Storage bucket for images."
    type = string
    default = "imagenes-agresores-victimas6"
}

variable "analitical_dataset"{
    description = "The name of the BigQuery dataset for analytical data."
    type = string
    default = "analitical_dataset5"
}

variable "firestore_collection_alertas"{
    description = "The name of the Firestore collection for storing alert data."
    type = string
    default = "alertas"
}

variable "firestore_database"{
    description = "The name of the Firestore database."
    type = string
}

variable "cloud_sql_instance_name"{
    description = "The name of the Cloud SQL instance."
    type = string
}

variable "db_user" {
    description = "The username for the Cloud SQL database."
    type = string
}

variable "db_password" {
    description = "The password for the Cloud SQL database."
    type = string
}

variable "container_image"{
    description = "URL image artifact registry"
    type = string
}
variable "container_image2"{
    description = "URL image artifact registry"
    type = string
}
variable "bucket_dataflow"{
    description = "Dataflow Bucket Name"
    type = string
}
variable "github_owner"{
    description = "Github Owner"
    type = string
}
variable "github_repo"{
    description = "Github Repo"
    type = string
}
variable "dataflow_sa_email"{
    description = "Dataflow Service Account Email"
    type = string
}