-- marts_ventas_netas: la UNICA fuente de verdad de "ventas netas" por
-- sucursal y dia. Esto es lo que Business Intelligence debe consultar
-- directamente -- nunca la tabla raw ni la de staging.

with ventas as (
    select * from {{ ref('stg_ventas') }}
)

select
    id_sucursal,
    fecha_venta,
    count(id_venta)        as cantidad_transacciones,
    sum(monto_bruto)       as ventas_brutas,
    sum(monto_descuento)   as total_descuentos,
    sum(monto_neto)        as ventas_netas
from ventas
group by 1, 2
