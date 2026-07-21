import pandas as pd
import psycopg2
from sqlalchemy import create_engine


CSV_PATH = "data/processed/favorita_clean.csv"


DB_CONFIG = {
    "host": "localhost",
    "database": "favorita_dw",
    "user": "favorita_user",
    "password": "favorita123",
    "port": "5432"
}


def cargar_postgres():

    print("Leyendo archivo procesado...")

    df = pd.read_csv(
        CSV_PATH,
        low_memory=False
    )

    print("Registros cargados:", df.shape)


    print("Conectando a PostgreSQL...")


    conexion = psycopg2.connect(
        **DB_CONFIG
    )


    print("Cargando datos en PostgreSQL...")


    # Crear tabla usando SQLAlchemy
    engine = create_engine(
        "postgresql+psycopg2://"
        f"{DB_CONFIG['user']}:"
        f"{DB_CONFIG['password']}@"
        f"{DB_CONFIG['host']}:"
        f"{DB_CONFIG['port']}/"
        f"{DB_CONFIG['database']}"
    )


    df.to_sql(
        name="fact_sales",
        con=engine,
        if_exists="replace",
        index=False,
        chunksize=50000,
        method="multi"
    )


    conexion.close()


    print("Carga completada correctamente")


if __name__ == "__main__":

    cargar_postgres()
