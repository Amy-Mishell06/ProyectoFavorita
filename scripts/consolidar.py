import polars as pl
import os

PROCESSED_DIR = "data/processed"

def consolidar_datasets():
    print("=== TAREA 4: Consolidando DataFrames en estructura única ===")
    
    # Cargar archivos limpios
    train = pl.read_parquet(os.path.join(PROCESSED_DIR, "train_clean.parquet"))
    stores = pl.read_parquet(os.path.join(PROCESSED_DIR, "stores_clean.parquet"))
    oil = pl.read_parquet(os.path.join(PROCESSED_DIR, "oil_clean.parquet"))
    transactions = pl.read_parquet(os.path.join(PROCESSED_DIR, "transactions_clean.parquet"))
    holidays = pl.read_parquet(os.path.join(PROCESSED_DIR, "holidays_clean.parquet"))

    # Joins secuenciales
    print("Uniendo ventas con tiendas...")
    df = train.join(stores, on="store_nbr", how="left")

    print("Uniendo con transacciones diarias...")
    df = df.join(transactions, on=["date", "store_nbr"], how="left")

    print("Uniendo con precios de petróleo...")
    df = df.join(oil, on="date", how="left")

    print("Uniendo con feriados...")
    df = df.join(holidays, on="date", how="left")

    # Tratamiento post-unión: si no fue feriado, marcar como día laborable
    df = df.with_columns([
        pl.col("type_right").fill_null("Workday")
    ])

    # Guardar el gran dataset consolidado
    ruta_salida = os.path.join(PROCESSED_DIR, "consolidado_final.parquet")
    df.write_parquet(ruta_salida)
    
    print(f"✅ ¡Tarea 4 terminada! Dataset unificado guardado en: {ruta_salida}")
    print(f"Registros totales: {df.height:,}\n")

if __name__ == "__main__":
    consolidar_datasets()
