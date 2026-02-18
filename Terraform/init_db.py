import sys
import sqlalchemy
from sqlalchemy import text
import os
import time

def run():
    if len(sys.argv) < 5:
        print(f"ERROR: Se esperaban 4 argumentos. Recibidos: {sys.argv}")
        args_safe = [sys.argv[0]] + [sys.argv[1], "*****", sys.argv[3], sys.argv[4]]
        sys.exit(1)

    user = sys.argv[1]
    password = sys.argv[2]
    db_name = sys.argv[3]
    instance_connection = sys.argv[4]

    print(f"--- INICIO DIAGNÓSTICO ---")
    print(f"Buscando socket para: {instance_connection}")
    
    socket_dir = f"/cloudsql/{instance_connection}"
    
    # Verificamos si existe la carpeta raíz /cloudsql
    if os.path.exists("/cloudsql"):
        print(f"La carpeta /cloudsql existe. Contenido: {os.listdir('/cloudsql')}")
    else:
        print(f"ERROR FATAL: La carpeta /cloudsql NO existe. Revisa 'volume_mounts' en Terraform.")
    
    # Verificamos si existe la carpeta de la instancia
    if os.path.exists(socket_dir):
        print(f"La carpeta de la instancia {socket_dir} existe. Contenido: {os.listdir(socket_dir)}")
    else:
        print(f"ERROR FATAL: No se encuentra la carpeta {socket_dir}. ¿Es correcto el connection_name?")

    print(f"--- FIN DIAGNÓSTICO ---")

    socket_path = f"/cloudsql/{instance_connection}/.s.PGSQL.5432"
    
    db_url = sqlalchemy.engine.url.URL.create(
        drivername="postgresql+pg8000",
        username=user,
        password=password,
        database=db_name,
        query={"unix_sock": socket_path},
    )
    engine = sqlalchemy.create_engine(db_url)

    sql_commands = [
        """
        CREATE TABLE IF NOT EXISTS victimas (
            id_victima VARCHAR(50) PRIMARY KEY,
            nombre_victima VARCHAR(100),
            apellido_victima VARCHAR(100),
            url_foto_victima VARCHAR(255)
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS agresores (
            id_agresor VARCHAR(50) PRIMARY KEY,
            nombre_agresor VARCHAR(100),
            apellido_agresor VARCHAR(100),
            url_foto_agresor VARCHAR(255)
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS safe_places (
            id_place VARCHAR(50) PRIMARY KEY,
            place_coordinates VARCHAR(100),
            radius INTEGER,
            place_name VARCHAR(100)
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS rel_victimas_agresores (
            id_agresor VARCHAR(50) NOT NULL,
            id_victima VARCHAR(50) NOT NULL,
            dist_seguridad INTEGER DEFAULT 500,
            PRIMARY KEY (id_agresor, id_victima),
            CONSTRAINT fk_rva_agresor FOREIGN KEY (id_agresor) 
                REFERENCES agresores(id_agresor) ON DELETE CASCADE,
            CONSTRAINT fk_rva_victima FOREIGN KEY (id_victima) 
                REFERENCES victimas(id_victima) ON DELETE CASCADE
        );
        """,

        """
        CREATE TABLE IF NOT EXISTS rel_places_victimas (
            id_victima VARCHAR(50) NOT NULL,
            id_place VARCHAR(50) NOT NULL,
            PRIMARY KEY (id_victima, id_place),
            CONSTRAINT fk_rpv_victima FOREIGN KEY (id_victima) 
                REFERENCES victimas(id_victima) ON DELETE CASCADE,
            CONSTRAINT fk_rpv_place FOREIGN KEY (id_place) 
                REFERENCES safe_places(id_place) ON DELETE CASCADE
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS denuncias3 (
            id_denuncia SERIAL PRIMARY KEY,
            fecha TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            descripcion TEXT,
            id_victima VARCHAR(50),
            CONSTRAINT fk_denuncia_vic FOREIGN KEY (id_victima) 
                REFERENCES victimas(id_victima)
        );
        """

    ]

    try:
        with engine.connect() as conn:
            print(f"Conectado exitosamente a {db_name}. Creando tablas...")
            for command in sql_commands:
                conn.execute(text(command))
            conn.commit()
            print("Todas las tablas y relaciones han sido creadas con éxito")
            
    except Exception as e:
        print(f"\nCRASH: Ocurrió un error al conectar o ejecutar SQL.")
        print(f"Detalle del error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    run()