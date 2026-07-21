import pandas as pd


def transformar_datos(datos):

    train = datos["train"].copy()
    stores = datos["stores"].copy()
    transactions = datos["transactions"].copy()
    oil = datos["oil"].copy()
    holidays = datos["holidays"].copy()


    print("Iniciando transformación...")


    # Convertir fechas
    train["date"] = pd.to_datetime(train["date"])
    transactions["date"] = pd.to_datetime(transactions["date"])
    oil["date"] = pd.to_datetime(oil["date"])
    holidays["date"] = pd.to_datetime(holidays["date"])


    # Crear variables de fecha
    train["year"] = train["date"].dt.year
    train["month"] = train["date"].dt.month
    train["weekday"] = train["date"].dt.day_name()


    # Unir información de tiendas
    df = train.merge(
        stores,
        on="store_nbr",
        how="left"
    )


    # Unir transacciones
    df = df.merge(
        transactions,
        on=["date", "store_nbr"],
        how="left"
    )


    # Unir petróleo
    df = df.merge(
        oil,
        on="date",
        how="left"
    )


    # Unir feriados
    df = df.merge(
        holidays,
        on="date",
        how="left"
    )


    print("Transformación completada")
    print("Dataset final:", df.shape)


    return df


if __name__ == "__main__":

    from ingest import cargar_datos

    datos = cargar_datos()

    df_final = transformar_datos(datos)

    print(df_final.head())


# Guardar dataset transformado
df_final.to_csv(
    "data/processed/favorita_clean.csv",
    index=False
)

print("Archivo generado:")
print("data/processed/favorita_clean.csv")
