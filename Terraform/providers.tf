terraform{
    backend "gcs" {
        bucket  = "bucket-state-tf-dp2"
    }
    required_providers {
        google = {
        source = "hashicorp/google"
        version = "7.14.1"
        }
        docker = {
      source  = "kreuzwerker/docker"
      version = "~> 3.0"
    }
    }
}
provider "google" {
    project = var.project_id
    region = var.region
    zone = var.zone
}

data "google_client_config" "default" {}

provider "docker" {
  host = "npipe:////./pipe/docker_engine"
  registry_auth {
    address  = "${var.region}-docker.pkg.dev"
    username = "oauth2accesstoken"
    password = data.google_client_config.default.access_token
  }
}