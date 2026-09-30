{{ config(materialized="view") }}

-- Estado vigente del negocio: última versión NO eliminada de cada movimiento.
-- Es el punto de entrada para la analítica y el consumo aguas abajo.

select *
from {{ source("core", "movimientos_historico") }}
where is_current
  and not is_deleted
