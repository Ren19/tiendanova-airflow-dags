# dags/reporte_consolidado.py
# DAG downstream, disparado por ingesta_sucursales_v2 via
# TriggerDagRunOperator (Clase 5). Consolida lo cargado por todas las
# sucursales en un unico reporte y calcula la variacion del tipo de
# cambio contra el dia anterior.

from __future__ import annotations

import pendulum
from airflow.sdk import dag, task

from dags.utils.transformaciones import calcular_variacion_tipo_cambio


@dag(
    dag_id="reporte_consolidado",
    schedule=None,  # solo se dispara via TriggerDagRunOperator
    start_date=pendulum.datetime(2026, 1, 1, tz="America/Lima"),
    catchup=False,
    tags=["tiendanova", "reporte", "clase5", "produccion"],
    default_args={"retries": 1, "retry_delay": pendulum.duration(minutes=5)},
)
def reporte_consolidado():

    @task
    def leer_tipo_cambio() -> dict:
        # En produccion lee el CSV que dejo el sensor deferrable de
        # ingesta_sucursales_v2. Se simula aqui con valores fijos.
        return {"hoy": 3.802, "ayer": 3.798}

    @task
    def calcular_variacion(tc: dict) -> float:
        return calcular_variacion_tipo_cambio(tc["hoy"], tc["ayer"])

    @task
    def generar_reporte(variacion: float) -> str:
        return f"Reporte consolidado generado. Variacion TC: {variacion}%"

    generar_reporte(calcular_variacion(leer_tipo_cambio()))


reporte_consolidado()
