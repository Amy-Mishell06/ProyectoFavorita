import polars as pl
import os
import json
import sys

# Rutas de carpetas
DATA_DIR = "data"
PROCESSED_DIR = "data/processed"
REPORTS_DIR = "reports"

# Crear carpetas si no existen
os.makedirs(PROCESSED_DIR, exist_ok=True)
os.makedirs(REPORTS_DIR, exist_ok=True)

# Listado de datasets esperados
DATASETS = {
    "train": "train.csv",
    "stores": "stores.csv",
    "transactions": "transactions.csv",
    "oil": "oil.csv",
    "holidays_events": "holidays_events.csv"
}

def tarea_1_cargar_datos():
    print("=== TAREA 1: Iniciando Carga de Datos ===")
    dfs = {}
    
    for clave, nombre_archivo in DATASETS.items():
        ruta = os.path.join(DATA_DIR, nombre_archivo)
        print(f"Cargando dataset: {nombre_archivo}...")
        
        if not os.path.exists(ruta):
            raise FileNotFoundError(f"Error critico: No se encontro el archivo obligatorio {ruta}")
        
        try:
            # Usamos scan_csv (Lazy mode) para cuidar los 8GB de RAM
            dfs[clave] = pl.scan_csv(ruta)
            print(f"--> {nombre_archivo} cargado exitosamente en modo Lazy.")
        except Exception as e:
            raise RuntimeError(f"Error al intentar leer {nombre_archivo} con Polars: {str(e)}")
            
    print("=== TAREA 1 COMPLETADA CON EXITO ===\n")
    return dfs

def tarea_2_eda_inicial(dfs):
    print("=== TAREA 2: Generando EDA Inicial (Diagnostico de Calidad) ===")
    reporte_calidad = {}

    	
    print("=== TAREA 2 COMPLETADA CON EXITO ===\n")

if __name__ == "__main__":
    try:
        dataframes_cargados = tarea_1_cargar_datos()
        tarea_2_eda_inicial(dataframes_cargados)
    except Exception as e:
        print(f"\n Error durante la ejecucion del pipeline: {e}")
        sys.exit(1)
