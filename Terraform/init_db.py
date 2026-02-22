import sys
import sqlalchemy
from sqlalchemy import text
import time

def run():
    # Seguimos esperando 4 argumentos, pero el último ahora será la IP Privada
    if len(sys.argv) < 5:
        print(f"ERROR: Se esperaban 4 argumentos. Recibidos: {sys.argv}")
        sys.exit(1)

    user = sys.argv[1]
    password = sys.argv[2]
    db_name = sys.argv[3]
    db_host = sys.argv[4] 
    db_port = 5432

    db_url = sqlalchemy.engine.url.URL.create(
        drivername="postgresql+pg8000",
        username=user,
        password=password,
        host=db_host,
        port=db_port,
        database=db_name
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
            dist_seguridad INTEGER NOT NULL DEFAULT 500,
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
        """
    ]

    try:
        with engine.connect() as conn:
            print(f"Conectado exitosamente a {db_name} en {db_host}. Creando tablas...")
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