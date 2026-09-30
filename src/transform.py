"""
Transformación a `staging`: limpia, tipa y NORMALIZA A MAYÚSCULAS antes de
calcular los hashes (paso crítico: si no se normaliza, 'entrada' y 'ENTRADA'
producirían hashes distintos y el mismo movimiento se vería como dos).

Produce:
  - staging.movimientos_stg : filas VÁLIDAS del corte, con:
        * business_key_hash  (hash de IDENTIDAD sobre la clave de negocio)
        * attr_hash          (hash de ATRIBUTOS mutables -> detecta correcciones)
        * movement_key       (identidad final = business_key_hash + occurrence_seq)
        * flags de calidad (soft)
  - core.movimientos_cuarentena : filas que rompen la identidad (hard-fail)

Todo es SQL set-based -> escala a millones de filas.
"""
from __future__ import annotations

import duckdb

from .config import Cut, TYPE_CANONICAL_MAP, DATE_ALT_FORMATS, HASH_SEP, NULL_SENTINEL
from .logging_conf import get_logger

log = get_logger("tyba.transform")


def _type_case_sql(col: str) -> str:
    """SQL CASE que mapea type -> {ENTRADA, SALIDA} sobre UPPER(TRIM())."""
    whens = " ".join(
        f"WHEN '{k}' THEN '{v}'" for k, v in TYPE_CANONICAL_MAP.items()
    )
    return f"CASE UPPER(TRIM({col})) {whens} ELSE NULL END"


def _date_parse_sql(col: str) -> str:
    """Parseo robusto: ISO nativo + formatos alternativos (DD/MM/YYYY)."""
    alts = "".join(
        f", TRY_STRPTIME(TRIM({col}), '{fmt}')::DATE" for fmt in DATE_ALT_FORMATS
    )
    return f"COALESCE(TRY_CAST(TRIM({col}) AS DATE){alts})"


def build_staging(con: duckdb.DuckDBPyConnection, cut: Cut) -> tuple[int, int]:
    """
    Construye staging para el corte indicado y separa cuarentena.
    Devuelve (filas_validas, filas_cuarentena).
    """
    type_case = _type_case_sql("type")
    date_parse = _date_parse_sql("date")

    # 1) Vista normalizada del corte (una sola pasada sobre raw).
    con.execute("DROP TABLE IF EXISTS _norm;")
    con.execute(
        f"""
        CREATE TEMP TABLE _norm AS
        SELECT
            -- clave de negocio normalizada (MAYÚSCULAS SOSTENIDAS)
            UPPER(TRIM(id_cliente))                        AS id_cliente,
            {date_parse}                                   AS mov_date,
            UPPER(TRIM(product))                           AS product,
            {type_case}                                    AS type,
            UPPER(TRIM(fund))                              AS fund,
            -- atributos mutables
            amount                                         AS amount,
            NULLIF(UPPER(TRIM(description)), '')           AS description,
            NULLIF(UPPER(TRIM(commercial_name)), '')       AS commercial_name,
            -- señales para flags
            (TRY_CAST(TRIM(date) AS DATE) IS NULL)         AS _date_was_non_iso,
            (UPPER(TRIM(type)) NOT IN ('ENTRADA','SALIDA')) AS _type_was_noncanonical,
            _source_file, _cut_label, _cut_date, _row_num, _load_ts
        FROM raw.movimientos
        WHERE _cut_label = '{cut.label}' AND _cut_date = DATE '{cut.cut_date}';
        """
    )

    # 2) Cuarentena: cualquier columna de la clave inutilizable.
    con.execute(
        f"""
        DELETE FROM core.movimientos_cuarentena
        WHERE _cut_label = '{cut.label}' AND _cut_date = DATE '{cut.cut_date}';
        """
    )
    con.execute(
        f"""
        INSERT INTO core.movimientos_cuarentena
        SELECT
            id_cliente, mov_date::VARCHAR, product, type, fund, amount,
            description, commercial_name,
            trim(concat_ws('; ',
                CASE WHEN id_cliente IS NULL OR id_cliente='' THEN 'id_cliente vacio' END,
                CASE WHEN mov_date  IS NULL THEN 'date no parseable' END,
                CASE WHEN product   IS NULL OR product=''   THEN 'product vacio' END,
                CASE WHEN type      IS NULL THEN 'type no mapeable' END,
                CASE WHEN fund      IS NULL OR fund=''       THEN 'fund vacio' END
            )) AS _reasons,
            _source_file, _cut_label, _cut_date, _load_ts
        FROM _norm
        WHERE id_cliente IS NULL OR id_cliente=''
           OR mov_date IS NULL
           OR product IS NULL OR product=''
           OR type IS NULL
           OR fund IS NULL OR fund='';
        """
    )
    n_quar = con.execute(
        "SELECT COUNT(*) FROM core.movimientos_cuarentena WHERE _cut_label=? AND _cut_date=?",
        [cut.label, cut.cut_date],
    ).fetchone()[0]

    # 3) Staging válido con hashes + occurrence_seq + movement_key + flags.
    #    business_key_hash : IDENTIDAD  (clave de negocio normalizada)
    #    attr_hash         : CAMBIO     (atributos mutables normalizados)
    bk_concat = f"concat_ws('{HASH_SEP}', id_cliente, mov_date::VARCHAR, product, type, fund)"
    attr_concat = (
        f"concat_ws('{HASH_SEP}', "
        f"COALESCE(amount::VARCHAR,'{NULL_SENTINEL}'), "
        f"COALESCE(description,'{NULL_SENTINEL}'), "
        f"COALESCE(commercial_name,'{NULL_SENTINEL}'))"
    )

    con.execute("DROP TABLE IF EXISTS staging.movimientos_stg;")
    con.execute(
        f"""
        CREATE TABLE staging.movimientos_stg AS
        WITH base AS (
            SELECT *,
                md5({bk_concat}) AS business_key_hash,
                md5({attr_concat}) AS attr_hash,
                -- signed_amount: confiamos en `type` para la dirección y en
                -- |amount| para la magnitud (corrige los signos incoherentes).
                CASE WHEN amount IS NULL THEN NULL
                     WHEN type='ENTRADA' THEN abs(amount)
                     WHEN type='SALIDA'  THEN -abs(amount) END AS signed_amount
            FROM _norm
            WHERE NOT (id_cliente IS NULL OR id_cliente=''
                       OR mov_date IS NULL
                       OR product IS NULL OR product=''
                       OR type IS NULL
                       OR fund IS NULL OR fund='')
        ),
        seq AS (
            SELECT *,
                row_number() OVER (
                    PARTITION BY business_key_hash
                    ORDER BY amount NULLS LAST, description NULLS LAST,
                             commercial_name NULLS LAST, _row_num
                ) AS occurrence_seq
            FROM base
        )
        SELECT
            md5(business_key_hash || '#' || occurrence_seq) AS movement_key,
            business_key_hash, occurrence_seq,
            id_cliente, mov_date, product, type, fund,
            amount, signed_amount, description, commercial_name,
            attr_hash,
            -- flags de calidad (soft)
            (amount IS NULL)                               AS dq_amount_null,
            (amount = 0)                                   AS dq_amount_zero,
            (amount < 0)                                   AS dq_amount_negative,
            _date_was_non_iso                              AS dq_date_reformatted,
            _type_was_noncanonical                         AS dq_type_recoded,
            _source_file, _cut_label, _cut_date, _load_ts
        FROM seq;
        """
    )
    n_valid = con.execute("SELECT COUNT(*) FROM staging.movimientos_stg").fetchone()[0]

    # Sanidad: movement_key debe ser único dentro del corte (por construcción).
    dups = con.execute(
        "SELECT COUNT(*)-COUNT(DISTINCT movement_key) FROM staging.movimientos_stg"
    ).fetchone()[0]
    if dups != 0:
        raise RuntimeError(f"movement_key no es único en staging (dups={dups})")

    log.info(
        "STG  | corte %s: %s válidas, %s en cuarentena",
        cut.label, f"{n_valid:,}", f"{n_quar:,}",
    )
    return n_valid, n_quar
