import sys
import sqlalchemy
from sqlalchemy import text

def run():
    # 1. Recoger argumentos pasados desde Terraform
    user = sys.argv[1]
    password = sys.argv[2]
    db_name = sys.argv[3]
    instance_connection = sys.argv[4]

    # 2. Configurar la conexión (Unix Socket para Cloud Run)
    db_url = sqlalchemy.engine.url.URL.create(
        drivername="postgresql+pg8000",
        username=user,
        password=password,
        database=db_name,
        query={"unix_sock": f"/cloudsql/{instance_connection}/.s.PGSQL.5432"},
    )

    engine = sqlalchemy.create_engine(db_url)

    # 3. Definición de las sentencias SQL (Ordenadas por dependencias)
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
            radius INTEGER
        );
        """,
        
        # Relación Víctimas - Agresores
        """
        CREATE TABLE IF NOT EXISTS rel_victimas_agresores (
            id_agresor VARCHAR(50) NOT NULL,
            id_victima VARCHAR(50) NOT NULL,
            riesgo_vital BOOLEAN DEFAULT FALSE,
            PRIMARY KEY (id_agresor, id_victima),
            CONSTRAINT fk_rva_agresor FOREIGN KEY (id_agresor) 
                REFERENCES agresores(id_agresor) ON DELETE CASCADE,
            CONSTRAINT fk_rva_victima FOREIGN KEY (id_victima) 
                REFERENCES victimas(id_victima) ON DELETE CASCADE
        );
        """,
        
        # Relación Places - Agresores (Lugares prohibidos)
        """
        CREATE TABLE IF NOT EXISTS rel_places_agresores (
            id_agresor VARCHAR(50) NOT NULL,
            id_place VARCHAR(50) NOT NULL,
            PRIMARY KEY (id_agresor, id_place),
            CONSTRAINT fk_rpa_agresor FOREIGN KEY (id_agresor) 
                REFERENCES agresores(id_agresor) ON DELETE CASCADE,
            CONSTRAINT fk_rpa_place FOREIGN KEY (id_place) 
                REFERENCES safe_places(id_place) ON DELETE CASCADE
        );
        """,
        
        # Relación Places - Víctimas (Lugares asignados)
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

    # 4. Ejecución
    with engine.connect() as conn:
        print(f"Conectado a {db_name}. Iniciando creación de tablas...")
        
        for command in sql_commands:
            try:
                conn.execute(text(command))
            except Exception as e:
                print(f"Error ejecutando SQL: {e}")
        
        conn.commit()
        print("¡Todas las tablas y relaciones han sido creadas con éxito!")

if __name__ == "__main__":
    run()