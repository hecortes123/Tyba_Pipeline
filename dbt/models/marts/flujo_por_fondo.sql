{{ config(materialized="table") }}

-- Flujo neto por fondo sobre el estado vigente.

select
    fund,
    count(*)           as n_movimientos,
    sum(signed_amount) as flujo_neto
from {{ ref("movimientos_vigentes") }}
group by fund
order by flujo_neto desc
