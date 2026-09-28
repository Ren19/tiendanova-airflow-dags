-- stg_ventas: limpieza minima de la venta cruda.
-- Renombra columnas poco claras, filtra filas sin llave primaria, y
-- calcula el neto una sola vez para que nadie mas tenga que reinventarlo
-- (esto es justo lo que le faltaba a TiendaNova en el incidente de la
-- Clase 7: dos analistas calculando "ventas netas" cada uno a su manera).

with fuente as (
    select * from {{ source('raw', 'ventas') }}
),

limpio as (
    select
        id_venta,
        id_sucursal,
        fecha_venta::date                          as fecha_venta,
        coalesce(monto_bruto, 0)                    as monto_bruto,
        coalesce(monto_descuento, 0)                as monto_descuento,
        coalesce(monto_bruto, 0) - coalesce(monto_descuento, 0) as monto_neto
    from fuente
    where id_venta is not null
      and id_sucursal is not null
)

select * from limpio
