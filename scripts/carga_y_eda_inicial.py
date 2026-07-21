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

    for clave, lazy_df in dfs.items():
        print(f"Analizando calidad de: {clave}...")

        # Materializamos (collect) solo para el diagnóstico de este dataset
        df = lazy_df.collect()

        total_filas = df.height
        total_columnas = df.width
        filas_duplicadas = df.is_duplicated().sum()

        # Rango de fechas si existe columna 'date'
        rango_fechas = None
        if "date" in df.columns:
            fechas = df["date"].drop_nulls()
            if fechas.len() > 0:
                rango_fechas = {
                    "minimo": str(fechas.min()),
                    "maximo": str(fechas.max())
                }

        # Diagnóstico por columna: tipo de dato y nulos
        columnas_info = {}
        for col in df.columns:
            nulos_conteo = df[col].null_count()
            nulos_porcentaje = round((nulos_conteo / total_filas) * 100, 2) if total_filas > 0 else 0.0
            columnas_info[col] = {
                "tipo_dato": str(df[col].dtype),
                "nulos_conteo": int(nulos_conteo),
                "nulos_porcentaje": nulos_porcentaje
            }

        reporte_calidad[clave] = {
            "archivo": DATASETS[clave],
            "total_filas": total_filas,
            "total_columnas": total_columnas,
            "filas_duplicadas": int(filas_duplicadas),
            "rango_fechas": rango_fechas,
            "columnas": columnas_info
        }

    # Guardar el reporte completo en JSON
    ruta_reporte = os.path.join(REPORTS_DIR, "eda_inicial.json")
    with open(ruta_reporte, "w", encoding="utf-8") as f:
        json.dump(reporte_calidad, f, indent=4, ensure_ascii=False)

    print(f"--> Reporte de calidad guardado en: {ruta_reporte}")
    print("=== TAREA 2 COMPLETADA CON EXITO ===\n")

if __name__ == "__main__":
    try:
        dataframes_cargados = tarea_1_cargar_datos()
        tarea_2_eda_inicial(dataframes_cargados)
    except Exception as e:
        print(f"\n Error durante la ejecucion del pipeline: {e}")
        sys.exit(1)