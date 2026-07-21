import polars as pl


def transformar_datos(datos):

    train = datos["train"]
    stores = datos["stores"]
    transactions = datos["transactions"]
    oil = datos["oil"]
    holidays = datos["holidays"]

    print("Iniciando transformación...")


    # Convertir fechas
    train = train.with_columns(
        pl.col("date").str.to_date()
    )

    transactions = transactions.with_columns(
        pl.col("date").str.to_date()
    )

    oil = oil.with_columns(
        pl.col("date").str.to_date()
    )

    holidays = holidays.with_columns(
        pl.col("date").str.to_date()
    )


    # Variables temporales
    train = train.with_columns(
        pl.col("date").dt.year().alias("year"),
        pl.col("date").dt.month().alias("month"),
        pl.col("date").dt.strftime("%A").alias("weekday")
    )


    # Join tiendas
    df = train.join(
        stores,
        on="store_nbr",
        how="left"
    )


    # Join transacciones
    df = df.join(
        transactions,
        on=["date", "store_nbr"],
        how="left"
    )


    # Join petróleo
    df = df.join(
        oil,
        on="date",
        how="left"
    )


    # Join feriados
    df = df.join(
        holidays,
        on="date",
        how="left"
    )


    print("Transformación completada")
    print("Dataset final:")
    print(df.shape)


    return df



if __name__ == "__main__":

    from ingest import cargar_datos

    datos = cargar_datos()

    df_final = transformar_datos(datos)


    print(df_final.head())


    df_final.write_csv(
        "data/processed/favorita_clean.csv"
    )


    print(
        "Archivo generado: data/processed/favorita_clean.csv"
    )
