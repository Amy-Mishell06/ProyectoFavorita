"""
DAG - Pipeline de Análisis de Datos: Corporación Favorita
Orquesta las 5 tareas del pipeline: carga inicial y EDA inicial, limpieza,
consolidación, EDA profundo y exportación a PostgreSQL.
"""

import os
from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

# -----------------------------------------------------
# Configuración por defecto de las tareas
# -----------------------------------------------------
default_args = {
    "owner": "equipo_favorita",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

# Ruta del proyecto construida dinámicamente a partir de la ubicación
# de este propio archivo DAG (evita typos de rutas hardcodeadas).
# Este archivo vive en: ProyectoFavorita/airflow/dags/pipeline_favorita_dag.py
# por eso subimos 2 niveles para llegar a la raíz del proyecto.
DAGS_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_DIR = os.path.abspath(os.path.join(DAGS_DIR, ".."))  
VENV_PYTHON = os.path.join(PROJECT_DIR, "venv", "bin", "python")

# -----------------------------------------------------
# Definición del DAG
# -----------------------------------------------------
with DAG(
    dag_id="pipeline_favorita",
    default_args=default_args,
    description="Pipeline completo: carga, limpieza, consolidación, EDA y exportación",
    schedule_interval=None,  # None = ejecución manual (no programada por cron)
    start_date=datetime(2026, 7, 1),
    catchup=False,
    tags=["favorita", "analisis-datos"],
) as dag:

    # Nota: usamos "cd {PROJECT_DIR} && ..." porque los scripts leen/escriben
    # rutas relativas como "data/" y "data/processed/", así que deben
    # ejecutarse estando parados en la raíz del proyecto.

    # Tarea 1: Carga inicial + EDA de calidad
    tarea_carga_eda = BashOperator(
        task_id="carga_y_eda_inicial",
        bash_command=f"cd {PROJECT_DIR} && {VENV_PYTHON} scripts/carga_y_eda_inicial.py",
    )

    # Tarea 2: Limpieza e imputación
    tarea_limpieza = BashOperator(
        task_id="limpiar_datos",
        bash_command=f"cd {PROJECT_DIR} && {VENV_PYTHON} scripts/limpiar_datos.py",
    )

    # Tarea 3: Consolidación
    tarea_consolidacion = BashOperator(
        task_id="consolidar_datasets",
        bash_command=f"cd {PROJECT_DIR} && {VENV_PYTHON} scripts/consolidar.py",
    )

    # Tarea 4: EDA profundo
    tarea_eda_profundo = BashOperator(
        task_id="eda_profundo",
        bash_command=f"cd {PROJECT_DIR} && {VENV_PYTHON} scripts/Eda_profundo.py",
    )

    # Tarea 5: Exportación a PostgreSQL
    tarea_exportar = BashOperator(
        task_id="exportar_postgres",
        bash_command=f"cd {PROJECT_DIR} && {VENV_PYTHON} scripts/exportar_postgres.py",
    )

    # -----------------------------------------------------
    # Definición del orden de ejecución (dependencias)
    # -----------------------------------------------------
    tarea_carga_eda >> tarea_limpieza >> tarea_consolidacion >> tarea_eda_profundo >> tarea_exportar