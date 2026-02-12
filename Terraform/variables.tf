variable "project_id"{
  description = "The ID of the project in which to create the resources."
  type = string
}

variable "region"{
    description = "The region in which to create the resources."
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

variable "topic_policia_alertas"{
    description = "The name of the Pub/Sub topic for police alerts."
    type = string
    default = "policia-alertas"
}

variable "subscription_policia_alertas"{
    description = "The name of the Pub/Sub subscription for police alerts."
    type = string
    default = "policia-alertas-sub"
}

variable "topic_victimas_alertas"{
    description = "The name of the Pub/Sub topic for victim alerts."
    type = string
    default = "victimas-alertas"
}

variable "subscription_victimas_alertas"{
    description = "The name of the Pub/Sub subscription for victim alerts."
    type = string
    default = "victimas-alertas-sub"
}

variable "topic_agresores_alertas"{
    description = "The name of the Pub/Sub topic for aggressor alerts."
    type = string
    default = "agresores-alertas"
}

variable "subscription_agresores_alertas"{
    description = "The name of the Pub/Sub subscription for aggressor alerts."
    type = string
    default = "agresores-alertas-sub"
}

variable "bucket_imagenes"{
    description = "The name of the Cloud Storage bucket for images."
    type = string
    default = "imagenes-agresores-victimas2"
}

variable "analitical_dataset"{
    description = "The name of the BigQuery dataset for analytical data."
    type = string
    default = "analitical_dataset"
}

variable "firestore_collection_alertas"{
    description = "The name of the Firestore collection for storing alert data."
    type = string
    default = "alertas"
}

variable "firestore_collection_safezones"{
    description = "The name of the Firestore collection for storing safe zone data."
    type = string
    default = "safe_zones"
}