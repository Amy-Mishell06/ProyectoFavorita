import pandas as pd
import psycopg2
import os


CSV_PATH = "data/processed/favorita_clean.csv"


DB_HOST = "localhost"
DB_PORT = "5432"
DB_NAME = "favorita_dw"
DB_USER = "favorita_user"
DB_PASSWORD = "favorita123"


def cargar_postgres():

    print("Leyendo archivo procesado...")

    df = pd.read_csv(CSV_PATH)

    print("Registros cargados:", df.shape)


    print("Conectando a PostgreSQL...")


    conn = psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        database=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD
    )


    cursor = conn.cursor()


    print("Eliminando tabla anterior...")


    cursor.execute("""
        DROP TABLE IF EXISTS fact_sales;
    """)


    print("Creando tabla fact_sales...")


    cursor.execute("""
        CREATE TABLE fact_sales (
            id BIGINT,
            date TEXT,
            store_nbr BIGINT,
            family TEXT,
            sales FLOAT,
            onpromotion BIGINT,
            year BIGINT,
            month BIGINT,
            weekday TEXT,
            city TEXT,
            state TEXT,
            type_x TEXT,
            cluster BIGINT,
            transactions FLOAT,
            dcoilwtico FLOAT,
            type_y TEXT,
            locale TEXT,
            locale_name TEXT,
            description TEXT,
            transferred BOOLEAN
        );
    """)


    print("Preparando archivo temporal...")


    temp_file = "/tmp/fact_sales.csv"


    df.to_csv(
        temp_file,
        index=False,
        header=False
    )


    print("Cargando datos con COPY...")


    with open(temp_file, "r") as f:

        cursor.copy_expert(
            """
            COPY fact_sales
            FROM STDIN
            WITH CSV
            """,
            f
        )


    conn.commit()


    cursor.close()
    conn.close()


    os.remove(temp_file)


    print("================================")
    print("Carga completada correctamente")
    print("================================")


if __name__ == "__main__":
    cargar_postgres()
