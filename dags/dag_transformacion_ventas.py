# dags/dag_transformacion_ventas.py
# Clase 7: orquesta el proyecto dbt de TiendaNova (staging -> marts) contra
# Snowflake usando Astronomer Cosmos. Cada modelo de dbt aparece como una
# tarea independiente de Airflow (a diferencia de correr "dbt build" entero
# dentro de un solo BashOperator, que es una caja negra).

from datetime import datetime
from pathlib import Path

from cosmos import DbtTaskGroup, ExecutionConfig, ProfileConfig, ProjectConfig
from cosmos.constants import ExecutionMode
from airflow.sdk import dag

# Ruta relativa a este archivo: funciona igual en el contenedor
# (/opt/airflow/dags/...) que en el runner de CI o en local.
DBT_PROJECT_DIR = Path(__file__).resolve().parent / "dbt" / "tiendanova"

profile_config = ProfileConfig(
    profile_name="tiendanova",
    target_name="dev",
    profiles_yml_filepath=DBT_PROJECT_DIR / "profiles.yml",
)

execution_config = ExecutionConfig(
    execution_mode=ExecutionMode.LOCAL,  # dbt corre dentro del mismo contenedor de Airflow
)


@dag(
    dag_id="dag_transformacion_ventas",
    start_date=datetime(2026, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["tiendanova", "dbt", "clase7"],
    default_args={"retries": 2},
)
def dag_transformacion_ventas():
    DbtTaskGroup(
        group_id="transformar_ventas",
        project_config=ProjectConfig(DBT_PROJECT_DIR),
        profile_config=profile_config,
        execution_config=execution_config,
    )


dag_transformacion_ventas()
