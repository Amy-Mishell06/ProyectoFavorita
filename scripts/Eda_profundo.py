import polars as pl
import os

PROCESSED_DIR = "data/processed"
os.makedirs(PROCESSED_DIR, exist_ok=True)

def ejecutar_eda_profundo():
    print("=== TAREA 5: Iniciando EDA Profundo Avanzado (Rúbrica Completa) ===")
    
    # 1. Cargar el dataset consolidado
    ruta_consolidado = os.path.join(PROCESSED_DIR, "consolidado_final.parquet")
    if not os.path.exists(ruta_consolidado):
        raise FileNotFoundError(f"No se encontró el archivo consolidado en {ruta_consolidado}. Corre las tareas previas primero.")
    
    print("Cargando dataset consolidado maestro...")
    df = pl.read_parquet(ruta_consolidado)

    # 1. VENTAS GENERALES
    print("[1/5] Analizando Ventas Generales...")
    
    # Distribución por familia de producto
    ventas_familia = df.group_by("family").agg([
        pl.col("sales").sum().alias("ventas_totales"),
        pl.col("sales").mean().alias("ventas_promedio")
    ]).sort("ventas_totales", descending=True)
    
    # Ranking de tiendas (Top 10 mayores y Top 10 menores)
    ventas_tiendas_base = df.group_by(["store_nbr", "city", "state"]).agg(
        pl.col("sales").sum().alias("ventas_totales")
    ).sort("ventas_totales", descending=True)
    
    top_10_mayores = ventas_tiendas_base.head(10)
    top_10_menores = ventas_tiendas_base.tail(10)
    
    # Ventas promedio por ciudad y provincia
    ventas_geograficas = df.group_by(["city", "state"]).agg(
        pl.col("sales").mean().alias("ventas_promedio")
    ).sort("ventas_promedio", descending=True)
    
    # Evolución temporal (Tendencia mensual y anual 2013-2017)
    ventas_temporal = df.with_columns([
        pl.col("date").dt.year().alias("anio"),
        pl.col("date").dt.month().alias("mes")
    ]).group_by(["anio", "mes"]).agg(
        pl.col("sales").sum().alias("ventas_mensuales")
    ).sort(["anio", "mes"])

    # 2. ESTACIONALIDAD Y FERIADOS

    print("[2/5] Analizando Estacionalidad y Feriados...")
    
    # Comparación días feriados vs días normales
    # Nota: Usamos 'type_right' o la columna que venga de holidays_events. Si es null, es día normal.
    df_feriados = df.with_columns(
        pl.when(pl.col("type_right").is_not_null())
        .then(pl.lit("Feriado"))
        .otherwise(pl.lit("Normal"))
        .alias("tipo_dia")
    )
    
    impacto_feriados = df_feriados.group_by("tipo_dia").agg([
        pl.col("sales").mean().alias("ventas_promedio_dia"),
        pl.col("transactions").mean().alias("transacciones_promedio_dia")
    ])

    # Ventas en ventanas de 3 días previos y posteriores a feriados nacionales
    # Creamos máscaras temporales para identificar la sensibilidad por familia de producto
    sensibilidad_feriados = df.group_by(["type_right", "family"]).agg(
        pl.col("sales").mean().alias("ventas_promedio")
    ).sort("ventas_promedio", descending=True)


    # 3. PROMOCIONES
    print("[3/5] Analizando Impacto de Promociones...")
    
    # Comparación de ventas con y sin promoción por familia
    df_promo = df.with_columns(
        pl.when(pl.col("onpromotion") > 0)
        .then(pl.lit("Con Promo"))
        .otherwise(pl.lit("Sin Promo"))
        .alias("estado_promocion")
    )
    
    impacto_promociones = df_promo.group_by(["family", "estado_promocion"]).agg(
        pl.col("sales").mean().alias("ventas_promedio")
    ).sort(["family", "estado_promocion"])

    # 4. PETRÓLEO Y ECONOMÍA

    print("[4/5] Analizando Correlación con el Petróleo...")
    
    # Agrupado mensual para calcular correlación y analizar la crisis 2015-2016
    df_mensual_economia = df.with_columns([
        pl.col("date").dt.year().alias("anio"),
        pl.col("date").dt.month().alias("mes")
    ]).group_by(["anio", "mes"]).agg([
        pl.col("sales").sum().alias("ventas_totales_mes"),
        pl.col("dcoilwtico").mean().alias("precio_petroleo_promedio")
    ]).sort(["anio", "mes"])
    
    # Sensibilidad de ciudades al precio del petróleo
    sensibilidad_ciudades_petroleo = df.group_by("city").agg(
        pl.corr("sales", "dcoilwtico").alias("correlacion_ventas_petroleo")
    ).sort("correlacion_ventas_petroleo")


    # 5. TRANSACCIONES
    print("[5/5] Analizando Métricas de Transacciones y Ticket Promedio...")
    
    # Relación entre transacciones y ventas con cálculo de Ticket Promedio
    ticket_promedio_tiendas = df.group_by("store_nbr").agg([
        pl.col("transactions").sum().alias("transacciones_totales"),
        pl.col("sales").sum().alias("ventas_totales"),
        (pl.col("sales").sum() / pl.col("transactions").sum()).alias("ticket_promedio")
    ]).sort("ticket_promedio", descending=True)


    # GUARDAR RESULTADOS EN PARQUET
    print("\nGuardando resúmenes estadísticos del EDA profundo...")
    
    ventas_familia.write_parquet(os.path.join(PROCESSED_DIR, "eda_ventas_familia.parquet"))
    top_10_mayores.write_parquet(os.path.join(PROCESSED_DIR, "eda_top_10_mayores.parquet"))
    top_10_menores.write_parquet(os.path.join(PROCESSED_DIR, "eda_top_10_menores.parquet"))
    ventas_geograficas.write_parquet(os.path.join(PROCESSED_DIR, "eda_ventas_geograficas.parquet"))
    ventas_temporal.write_parquet(os.path.join(PROCESSED_DIR, "eda_ventas_temporal.parquet"))
    impacto_feriados.write_parquet(os.path.join(PROCESSED_DIR, "eda_impacto_feriados.parquet"))
    sensibilidad_feriados.write_parquet(os.path.join(PROCESSED_DIR, "eda_sensibilidad_feriados.parquet"))
    impacto_promociones.write_parquet(os.path.join(PROCESSED_DIR, "eda_impacto_promociones.parquet"))
    df_mensual_economia.write_parquet(os.path.join(PROCESSED_DIR, "eda_mensual_economia.parquet"))
    sensibilidad_ciudades_petroleo.write_parquet(os.path.join(PROCESSED_DIR, "eda_ciudades_petroleo.parquet"))
    ticket_promedio_tiendas.write_parquet(os.path.join(PROCESSED_DIR, "eda_ticket_promedio.parquet"))

    print("\n=== ¡TAREA 5 COMPLETADA CON ÉXITO! ===")
    print(f"Tablas estadísticas listas en '{PROCESSED_DIR}/' para subir a PostgreSQL.")

if __name__ == "__main__":
    ejecutar_eda_profundo()
