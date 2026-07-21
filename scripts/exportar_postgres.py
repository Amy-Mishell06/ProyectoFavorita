import os
import csv
import tempfile
import polars as pl
import psycopg2

# CONFIGURACIÓN

PROCESSED_DIR = "data/processed"

# Leer credenciales desde variables de entorno con valores por defecto
DB_HOST = os.environ.get("DB_HOST", "localhost")
DB_PORT = os.environ.get("DB_PORT", "5432")
DB_NAME = os.environ.get("DB_NAME", "favorita_db")
DB_USER = os.environ.get("DB_USER", "postgres")
DB_PASS = os.environ.get("DB_PASS", "postgres")


# CONEXIÓN

def conectar():
    return psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        database=DB_NAME,
        user=DB_USER,
        password=DB_PASS
    )


# TIPOS DE DATOS

def tipo_postgres(dtype):

    dtype = str(dtype)

    if "Int" in dtype:
        return "BIGINT"

    if "Float" in dtype:
        return "DOUBLE PRECISION"

    if "Boolean" in dtype:
        return "BOOLEAN"

    if "Date" in dtype:
        return "DATE"

    if "Datetime" in dtype:
        return "TIMESTAMP"

    return "TEXT"


# CREAR TABLA

def crear_tabla(cursor, nombre_tabla, df):

    cursor.execute(f'DROP TABLE IF EXISTS "{nombre_tabla}"')

    columnas = []

    for col in df.columns:

        tipo = tipo_postgres(df.schema[col])

        columnas.append(f'"{col}" {tipo}')

    sql = f'''
        CREATE TABLE "{nombre_tabla}"(
            {",".join(columnas)}
        )
    '''

    cursor.execute(sql)


# EXPORTAR CON COPY

def copiar_tabla(conn, nombre_tabla, archivo_parquet):

    print(f"\nExportando {nombre_tabla}...")

    df = pl.read_parquet(archivo_parquet)

    print(f"Registros: {df.height:,}")

    cur = conn.cursor()

    crear_tabla(cur, nombre_tabla, df)

    conn.commit()

    archivo_tmp = tempfile.NamedTemporaryFile(
        delete=False,
        suffix=".csv"
    )

    archivo_tmp.close()

    df.write_csv(archivo_tmp.name)

    with open(archivo_tmp.name, "r", encoding="utf-8") as f:

        next(f)

        cur.copy_expert(
            f'''
            COPY "{nombre_tabla}"
            FROM STDIN
            WITH (
                FORMAT CSV
            )
            ''',
            f
        )

    conn.commit()

    cur.close()

    os.remove(archivo_tmp.name)

    print(f"✅ {nombre_tabla} exportada.")

# EXPORTACIÓN PRINCIPAL


def exportar():

    print("=" * 60)
    print("EXPORTANDO PARQUET -> POSTGRESQL")
    print("=" * 60)

    conn = conectar()

    archivos = {
        "consolidado_final": "consolidado_final.parquet",
        "eda_ventas_familia": "eda_ventas_familia.parquet",
        "eda_top_10_mayores": "eda_top_10_mayores.parquet",
        "eda_top_10_menores": "eda_top_10_menores.parquet",
        "eda_ventas_geograficas": "eda_ventas_geograficas.parquet",
        "eda_ventas_temporal": "eda_ventas_temporal.parquet",
        "eda_impacto_feriados": "eda_impacto_feriados.parquet",
        "eda_sensibilidad_feriados": "eda_sensibilidad_feriados.parquet",
        "eda_impacto_promociones": "eda_impacto_promociones.parquet",
        "eda_mensual_economia": "eda_mensual_economia.parquet",
        "eda_ciudades_petroleo": "eda_ciudades_petroleo.parquet",
        "eda_ticket_promedio": "eda_ticket_promedio.parquet"
    }

    for tabla, archivo in archivos.items():

        ruta = os.path.join(PROCESSED_DIR, archivo)

        if not os.path.exists(ruta):
            print(f"⚠ No existe {archivo}")
            continue

        try:
            copiar_tabla(conn, tabla, ruta)

        except Exception as e:
            print(f"❌ Error exportando {tabla}")
            print(e)

    conn.close()

    print("\n")
    print("=" * 60)
    print("EXPORTACIÓN TERMINADA")
    print("=" * 60)


# MAIN

if __name__ == "__main__":

    try:
        exportar()

    except Exception as e:
        print("\nERROR GENERAL")
        print(e)
