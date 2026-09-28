# dags/utils/transformaciones.py
# Logica de negocio pura, sin ningun import de Airflow: esto es lo que
# permite testearla en milisegundos, sin levantar un scheduler ni una BD.


def calcular_filas_limpias(datos: dict) -> dict:
    """Aplica la regla de descarte del 3% (filas corruptas conocidas por
    el equipo de datos de TiendaNova) sobre el total de filas ingeridas
    de una sucursal.

    Parametros
    ----------
    datos: dict con al menos las claves "sucursal" (str) y "filas" (int).

    Retorna
    -------
    El mismo dict, con la clave adicional "filas_limpias".

    Lanza
    -----
    ValueError si "filas" es negativo (dato corrupto en origen).
    """
    if datos["filas"] < 0:
        raise ValueError("filas no puede ser negativo")
    datos["filas_limpias"] = int(datos["filas"] * 0.97)
    return datos


def calcular_variacion_tipo_cambio(tipo_cambio_hoy: float, tipo_cambio_ayer: float) -> float:
    """Calcula la variacion porcentual del tipo de cambio entre dos dias.

    Se usa para decidir si vale la pena recalcular el reporte consolidado
    en moneda extranjera (si la variacion es menor al 0.1%, se reutiliza
    el tipo de cambio del dia anterior para ahorrar una llamada a la API).
    """
    if tipo_cambio_ayer == 0:
        raise ValueError("tipo_cambio_ayer no puede ser cero")
    return round(((tipo_cambio_hoy - tipo_cambio_ayer) / tipo_cambio_ayer) * 100, 4)
