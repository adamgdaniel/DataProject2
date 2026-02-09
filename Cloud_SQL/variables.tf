variable "pass_db_sql" {
  description = "Contraseña para el admin de la base de datos"
  type        = string
  sensitive   = true # Esto evita que la contraseña salga impresa en la consola al hacer 'apply'
}
