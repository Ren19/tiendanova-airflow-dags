# dags/ingesta_sucursales_v2.py
# Clase 5 (v2): TaskGroups, Dynamic Task Mapping, Trigger Rules y un
# Sensor Deferrable. Clase 6 solo le agrega, sin tocar esta logica,
# la posibilidad de ser testeado (ver dags/utils/transformaciones.py
# y tests/test_dags.py).

from __future__ import annotations

import pendulum
from airflow.sdk import dag, task, task_group
from airflow.sdk import Variable
from airflow.exceptions import AirflowNotFoundException

from dags.utils.transformaciones import calcular_filas_limpias

SUCURSALES = [
    "lima", "arequipa", "trujillo", "chiclayo",
    "piura", "cusco", "huancayo", "iquitos",
]


@task.sensor(poke_interval=30, timeout=60 * 30, mode="reschedule")
def esperar_tipo_cambio():
    """Sensor deferrable: libera el worker mientras espera a que el
    archivo del tipo de cambio del dia llegue al volumen compartido.
    El Triggerer, no un worker, hace el polling mientras tanto.
    """
    import os
    from airflow.sensors.base import PokeReturnValue

    ruta = "/opt/airflow/data/tipo_cambio_hoy.csv"
    existe = os.path.exists(ruta)
    return PokeReturnValue(is_done=existe, xcom_value=ruta if existe else None)


@dag(
    dag_id="ingesta_sucursales_v2",
    schedule="0 6 * * *",
    start_date=pendulum.datetime(2026, 1, 1, tz="America/Lima"),
    catchup=False,
    tags=["tiendanova", "ingesta", "clase5", "produccion"],
    default_args={"retries": 2, "retry_delay": pendulum.duration(minutes=5)},
)
def ingesta_sucursales_v2():

    esperar = esperar_tipo_cambio()

    @task
    def extraer_sucursal(sucursal: str) -> dict:
        # En produccion esto llama al ERP de la sucursal. Se simula
        # aqui con un numero de filas determinista por sucursal.
        filas_por_sucursal = {
            "lima": 5230, "arequipa": 2110, "trujillo": 1875, "chiclayo": 1420,
            "piura": 1390, "cusco": 980, "huancayo": 860, "iquitos": 640,
        }
        return {"sucursal": sucursal, "filas": filas_por_sucursal[sucursal]}

    @task
    def transformar(datos: dict) -> dict:
        # Toda la logica de negocio vive fuera de Airflow: se prueba
        # con un test unitario puro en tests/test_dags.py.
        return calcular_filas_limpias(datos)

    @task
    def cargar_sucursal(datos: dict) -> str:
        # Simulacion de carga a la capa staging del data warehouse.
        return f"{datos['sucursal']}: {datos['filas_limpias']} filas cargadas"

    @task_group(group_id="procesar_sucursales")
    def procesar_sucursales():
        extraidos = extraer_sucursal.expand(sucursal=SUCURSALES)
        transformados = transformar.expand(datos=extraidos)
        cargar_sucursal.expand(datos=transformados)

    @task(trigger_rule="all_success")
    def disparar_reporte_consolidado():
        try:
            Variable.get("umbral_alerta_variacion_tc")
        except AirflowNotFoundException:
            # Config opcional: si no existe, el reporte usa su default.
            pass
        return "listo para disparar reporte_consolidado"

    from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator

    disparar_downstream = TriggerDagRunOperator(
        task_id="trigger_reporte_consolidado",
        trigger_dag_id="reporte_consolidado",
        wait_for_completion=False,
        trigger_rule="all_success",
    )

    grupo = procesar_sucursales()
    marcador = disparar_reporte_consolidado()

    esperar >> grupo >> marcador >> disparar_downstream


ingesta_sucursales_v2()
