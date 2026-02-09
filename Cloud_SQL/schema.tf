# ==========================================
# 1. ENTIDADES PRINCIPALES
# ==========================================

# Tabla 1: Víctimas
resource "postgresql_table" "victimas" {
  name   = "victimas"
  schema = "public"

  column {
    name     = "id_victima"
    type     = "varchar" # IDs tipo "VIC_01"
    nullable = false
  }
  column {
    name     = "nombre_victima"
    type     = "varchar"
  }
  column {
    name     = "apellido_victima"
    type     = "varchar"
  }
  column {
    name     = "url_foto_victima"
    type     = "varchar"
  }

  primary_key {
    columns = ["id_victima"]
  }
}

# Tabla 2: Agresores
resource "postgresql_table" "agresores" {
  name   = "agresores"
  schema = "public"

  column {
    name     = "id_agresor"
    type     = "varchar" # IDs tipo "AGR_01"
    nullable = false
  }
  column {
    name     = "nombre_agresor"
    type     = "varchar"
  }
  column {
    name     = "apellido_agresor"
    type     = "varchar"
  }
  column {
    name     = "url_foto_agresor"
    type     = "varchar"
  }

  primary_key {
    columns = ["id_agresor"]
  }
}

# Tabla 4: Safe Places (Lugares Seguros)
resource "postgresql_table" "safe_places" {
  name   = "safe_places"
  schema = "public"

  column {
    name     = "id_place"
    type     = "varchar"
    nullable = false
  }
  column {
    name     = "place_coordinates"
    type     = "varchar" # Ej: "39.4699,-0.3763"
  }
  column {
    name     = "radius"
    type     = "integer" # Radio en metros
  }

  primary_key {
    columns = ["id_place"]
  }
}

# ==========================================
# 2. TABLAS DE RELACIÓN (VINCULACIONES)
# ==========================================

# Tabla 3: Relación Victimas - Agresores (El caso a monitorear)
resource "postgresql_table" "rel_victimas_agresores" {
  name   = "rel_victimas_agresores"
  schema = "public"
  depends_on = [postgresql_table.victimas, postgresql_table.agresores]

  column {
    name     = "id_agresor"
    type     = "varchar"
    nullable = false
  }
  column {
    name     = "id_victima"
    type     = "varchar"
    nullable = false
  }
  column {
    name     = "riesgo_vital"
    type     = "boolean"
    default  = "false"
  }

  # Clave primaria compuesta (un par único)
  primary_key {
    columns = ["id_agresor", "id_victima"]
  }

  # Foreign Keys
  foreign_key {
    columns     = ["id_agresor"]
    ref_table   = postgresql_table.agresores.name
    ref_columns = ["id_agresor"]
    on_delete   = "CASCADE"
  }
  foreign_key {
    columns     = ["id_victima"]
    ref_table   = postgresql_table.victimas.name
    ref_columns = ["id_victima"]
    on_delete   = "CASCADE"
  }
}

# Tabla 5: Relación Places - Agresores (Lugares prohibidos para el agresor)
resource "postgresql_table" "rel_places_agresores" {
  name   = "rel_places_agresores"
  schema = "public"
  depends_on = [postgresql_table.safe_places, postgresql_table.agresores]

  column {
    name     = "id_agresor"
    type     = "varchar"
    nullable = false
  }
  column {
    name     = "id_place"
    type     = "varchar"
    nullable = false
  }

  primary_key {
    columns = ["id_agresor", "id_place"]
  }

  foreign_key {
    columns     = ["id_agresor"]
    ref_table   = postgresql_table.agresores.name
    ref_columns = ["id_agresor"]
    on_delete   = "CASCADE"
  }
  foreign_key {
    columns     = ["id_place"]
    ref_table   = postgresql_table.safe_places.name
    ref_columns = ["id_place"]
    on_delete   = "CASCADE"
  }
}

# Tabla 6: Relación Places - Victimas (Lugares asignados a la victima como casa/trabajo)

resource "postgresql_table" "rel_places_victimas" {
  name   = "rel_places_victimas"
  schema = "public"
  depends_on = [postgresql_table.safe_places, postgresql_table.victimas]

  column {
    name     = "id_victima"
    type     = "varchar"
    nullable = false
  }
  column {
    name     = "id_place"
    type     = "varchar"
    nullable = false
  }

  primary_key {
    columns = ["id_victima", "id_place"]
  }

  foreign_key {
    columns     = ["id_victima"]
    ref_table   = postgresql_table.victimas.name
    ref_columns = ["id_victima"]
    on_delete   = "CASCADE"
  }
  foreign_key {
    columns     = ["id_place"]
    ref_table   = postgresql_table.safe_places.name
    ref_columns = ["id_place"]
    on_delete   = "CASCADE"
  }
}