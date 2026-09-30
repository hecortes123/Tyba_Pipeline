-- Invariante SCD2: cada movimiento tiene A LO SUMO una versión vigente.
-- El test falla si devuelve filas (algún movement_key con >1 is_current).

select movement_key, count(*) as versiones_vigentes
from {{ source("core", "movimientos_historico") }}
where is_current
group by movement_key
having count(*) > 1
