import pandas as pd
import os


DATA_PATH = "data"


def cargar_datos():

    archivos = {
        "train": "train.csv",
        "stores": "stores.csv",
        "transactions": "transactions.csv",
        "oil": "oil.csv",
        "holidays": "holidays_events.csv"
    }

    datos = {}

    for nombre, archivo in archivos.items():

        ruta = os.path.join(DATA_PATH, archivo)

        if os.path.exists(ruta):
            print(f"Cargando {archivo}...")
            datos[nombre] = pd.read_csv(ruta)
            print(f"{nombre}: {datos[nombre].shape}")

        else:
            print(f"No existe: {ruta}")

    return datos


if __name__ == "__main__":

    datasets = cargar_datos()

    print("\nArchivos cargados:")
    for nombre in datasets:
        print("-", nombre)
