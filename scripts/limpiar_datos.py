import polars as pl
import os

DATA_DIR = "data"
PROCESSED_DIR = os.path.join(DATA_DIR, "processed")
os.makedirs(PROCESSED_DIR, exist_ok=True)

def imputar_y_limpiar():
    print("=== TAREA 3: Limpiando e imputando datos con Polars ===")
    
    # Cargar datos 
    train = pl.read_csv(os.path.join(DATA_DIR, "train.csv"))
    stores = pl.read_csv(os.path.join(DATA_DIR, "stores.csv"))
    oil = pl.read_csv(os.path.join(DATA_DIR, "oil.csv"))
    transactions = pl.read_csv(os.path.join(DATA_DIR, "transactions.csv"))
    holidays = pl.read_csv(os.path.join(DATA_DIR, "holidays_events.csv"))

    # Estandarizar Fechas y corregir tipos 
    train = train.with_columns(pl.col("date").str.to_date())
    transactions = transactions.with_columns(pl.col("date").str.to_date())
    oil = oil.with_columns(pl.col("date").str.to_date())
    holidays = holidays.with_columns(pl.col("date").str.to_date())

    #VLimpieza de Duplicados
    print("Removiendo filas duplicadas...")
    train = train.unique()
    stores = stores.unique()
    oil = oil.unique(subset=["date"])
    transactions = transactions.unique()
    holidays = holidays.unique(subset=["date"]) # Evitamos colisiones de feriados el mismo día

    #Imputación de Valores Nulos 
    
    # A) Petróleo: Interpolación Lineal (Se requiere pasar a Pandas temporalmente ya que Polars no tiene .interpolate() directo por defecto)
    print("Imputando precios de petróleo con interpolación lineal...")
    oil_pd = oil.to_pandas()
    oil_pd["dcoilwtico"] = oil_pd["dcoilwtico"].interpolate(method="linear").ffill().bfill()
    oil = pl.from_pandas(oil_pd)
    oil = oil.with_columns(pl.col("date").cast(pl.Date))

    # Transacciones: Si hay tiendas con nulos en transacciones, imputamos con la mediana (más robusta a valores atípicos)
    mediana_transacciones = transactions["transactions"].median()
    transactions = transactions.with_columns(
        pl.col("transactions").fill_null(mediana_transacciones)
    )

    # --- Guardar datasets limpios intermedios ---
    train.write_parquet(os.path.join(PROCESSED_DIR, "train_clean.parquet"))
    stores.write_parquet(os.path.join(PROCESSED_DIR, "stores_clean.parquet"))
    oil.write_parquet(os.path.join(PROCESSED_DIR, "oil_clean.parquet"))
    transactions.write_parquet(os.path.join(PROCESSED_DIR, "transactions_clean.parquet"))
    holidays.write_parquet(os.path.join(PROCESSED_DIR, "holidays_clean.parquet"))

    print("¡Tarea 3 terminada! Archivos limpios guardados en data/processed/\n")

if __name__ == "__main__":
    imputar_y_limpiar()
