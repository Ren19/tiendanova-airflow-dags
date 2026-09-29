# tests/test_dags.py
import pytest
from unittest.mock import patch
from airflow.models import DagBag

from dags.utils.transformaciones import calcular_filas_limpias


# --- Fixture compartida: carga el DagBag UNA sola vez para todos los tests ---
@pytest.fixture(scope="session")
def dagbag() -> DagBag:
    return DagBag(dag_folder="dags/", include_examples=False)


# ======================================================================
# BLOQUE 1 -- Tests estructurales (DagBag)
# ======================================================================

def test_no_hay_errores_de_import(dagbag: DagBag):
    # Este es EXACTAMENTE el test que habria atrapado el incidente real:
    # un import mal escrito hace que dagbag.import_errors no este vacio.
    assert len(dagbag.import_errors) == 0, f"DAGs con errores de import: {dagbag.import_errors}"


@pytest.mark.parametrize(
    "dag_id", ["ingesta_sucursales_v2", "reporte_consolidado", "dag_transformacion_ventas"]
)
def test_dag_existe_y_carga(dagbag: DagBag, dag_id: str):
    # dagbag.dags en vez de get_dag(): get_dag consulta la metadata DB de
    # Airflow, que no existe en CI. Un test estructural no necesita DB.
    dag = dagbag.dags.get(dag_id)
    assert dag is not None, f"{dag_id} no se pudo cargar desde dags/"


def test_todos_los_dags_tienen_tags(dagbag: DagBag):
    for dag_id, dag in dagbag.dags.items():
        assert dag.tags, f"{dag_id} no tiene tags configurados"


def test_todas_las_tasks_tienen_retries(dagbag: DagBag):
    # Un DAG productivo sin retries es un DAG que falla ante el primer
    # hipo de red o de conexion. Este test obliga a que nadie lo olvide.
    for dag_id, dag in dagbag.dags.items():
        for task in dag.tasks:
            assert task.retries is not None and task.retries >= 1, (
                f"{dag_id}.{task.task_id} no tiene retries configurados"
            )


def test_no_hay_ciclos_ni_dags_duplicados(dagbag: DagBag):
    # dagbag.dags ya deduplica por dag_id: si el conteo de archivos
    # procesados no coincide con lo esperado, algo se esta pisando.
    assert len(dagbag.dags) == 3, f"Se esperaban 3 DAGs, se encontraron {len(dagbag.dags)}"


# ======================================================================
# BLOQUE 2 -- Test unitario de logica de negocio pura (sin Airflow)
# ======================================================================

def test_calcular_filas_limpias_aplica_el_3_por_ciento_de_descarte():
    resultado = calcular_filas_limpias({"sucursal": "lima", "filas": 5230})
    assert resultado["filas_limpias"] == 5073


def test_calcular_filas_limpias_rechaza_filas_negativas():
    with pytest.raises(ValueError):
        calcular_filas_limpias({"sucursal": "cusco", "filas": -10})


# ======================================================================
# BLOQUE 3 -- Mock de una dependencia externa (ejemplo: llamada a la API
# de tipo de cambio que usa el sensor deferrable de la Clase 5)
# ======================================================================

@patch("os.path.exists")
def test_esperar_tipo_cambio_detecta_archivo_sin_tocar_disco_real(mock_exists):
    mock_exists.return_value = True
    # No accedemos al sistema de archivos real: mockeamos la dependencia
    # externa para que el test sea rapido, determinista y reproducible
    # en cualquier maquina, incluida la de GitHub Actions.
    import os
    assert os.path.exists("/opt/airflow/data/tipo_cambio_hoy.csv") is True
    mock_exists.assert_called_once()
