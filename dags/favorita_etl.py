from airflow import DAG
from airflow.operators.bash import BashOperator

from datetime import datetime


PROJECT_PATH = "/home/jonathan/ProyectoFavorita"


default_args = {
    "owner": "jonathan",
}


with DAG(
    dag_id="favorita_etl_pipeline",
    default_args=default_args,
    start_date=datetime(2026, 7, 20),
    schedule="@daily",
    catchup=False,
    description="Pipeline ETL Proyecto Favorita Ecuador"
) as dag:


    tarea_ingesta = BashOperator(
        task_id="ingesta_datos",
        bash_command=f"""
        cd {PROJECT_PATH}
        source venv/bin/activate
        python scripts/ingest.py
        """
    )


    tarea_transformacion = BashOperator(
        task_id="transformacion_datos",
        bash_command=f"""
        cd {PROJECT_PATH}
        source venv/bin/activate
        python scripts/transform.py
        """
    )


    carga_postgresql = BashOperator(
        task_id="carga_postgresql",
        bash_command="""
        cd /home/jonathan/ProyectoFavorita &&
        python scripts/load.py
        """
    )


    tarea_ingesta >> tarea_transformacion >> carga_postgresql
