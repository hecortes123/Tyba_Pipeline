-- Invariante SCD2: la vigencia es coherente con valid_to.
--   * una versión vigente NO puede tener valid_to (cadena abierta)
--   * una versión cerrada SÍ debe tener valid_to
-- El test falla si aparece cualquier fila incoherente.

select 'vigente_con_valid_to' as problema, movement_key, version_id
from {{ source("core", "movimientos_historico") }}
where is_current and valid_to is not null

union all

select 'cerrada_sin_valid_to' as problema, movement_key, version_id
from {{ source("core", "movimientos_historico") }}
where not is_current and valid_to is null
