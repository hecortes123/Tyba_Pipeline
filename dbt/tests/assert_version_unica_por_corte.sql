-- Invariante SCD2: no puede haber dos versiones del mismo movimiento que
-- entren en vigencia en el mismo corte (movement_key + valid_from único).

select movement_key, valid_from, count(*) as n
from {{ source("core", "movimientos_historico") }}
group by movement_key, valid_from
having count(*) > 1
