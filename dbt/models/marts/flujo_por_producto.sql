{{ config(materialized="table") }}

-- Flujo neto (entradas − salidas) por producto sobre el estado vigente.
-- Usa signed_amount (magnitud con signo según type) para netear correctamente.

select
    product,
    count(*)                                        as n_movimientos,
    sum(signed_amount)                              as flujo_neto,
    sum(case when type = 'ENTRADA' then amount end) as total_entradas,
    sum(case when type = 'SALIDA'  then amount end) as total_salidas
from {{ ref("movimientos_vigentes") }}
group by product
order by flujo_neto desc
