"""
Motor CDC / SCD Tipo 2.

Compara el corte entrante (staging) contra el estado VIGENTE del histórico y
consolida la evolución con trazabilidad total:

  NEW        id nuevo (o reaparición de uno eliminado) -> nueva versión INSERT
  UPDATE     misma clave, attr_hash distinto           -> cierra versión + nueva UPDATE
  DELETE     clave vigente ausente en el corte         -> cierra versión + tombstone
  UNCHANGED  misma clave, mismo attr_hash              -> no-op

Propiedades:
  * Set-based (un FULL OUTER JOIN) -> escala a millones de filas.
  * Idempotente: reprocesar el mismo corte no cambia nada.
  * Trazable: cada evento queda en core.change_log; nunca se borra historia.
"""
from __future__ import annotations

from dataclasses import dataclass

import duckdb

from .config import Cut
from .logging_conf import get_logger

log = get_logger("tyba.scd2")


@dataclass
class MergeResult:
    new: int
    changed: int
    deleted: int
    unchanged: int


def merge_cut(con: duckdb.DuckDBPyConnection, cut: Cut, run_id: str) -> MergeResult:
    cd = f"DATE '{cut.cut_date}'"

    con.execute("BEGIN TRANSACTION;")
    try:
        # 1) Clasificación: staging (corte) FULL OUTER JOIN estado vigente.
        con.execute(
            f"""
            CREATE TEMP TABLE _actions AS
            WITH cur AS (
                SELECT * FROM core.movimientos_historico WHERE is_current
            ),
            j AS (
                SELECT
                    COALESCE(s.movement_key, c.movement_key) AS movement_key,
                    (s.movement_key IS NOT NULL)             AS in_stg,
                    (c.movement_key IS NOT NULL)             AS in_cur,
                    c.is_deleted                             AS cur_is_deleted,
                    c.attr_hash                              AS cur_attr_hash,
                    s.attr_hash                              AS stg_attr_hash,
                    c.version_id                             AS cur_version_id
                FROM staging.movimientos_stg s
                FULL OUTER JOIN cur c USING (movement_key)
            )
            SELECT
                movement_key, cur_version_id, cur_attr_hash, stg_attr_hash,
                CASE
                    WHEN in_stg AND NOT in_cur                       THEN 'INSERT'
                    WHEN in_stg AND in_cur AND cur_is_deleted        THEN 'INSERT'   -- reaparición
                    WHEN in_stg AND in_cur AND stg_attr_hash <> cur_attr_hash THEN 'UPDATE'
                    WHEN in_stg AND in_cur AND stg_attr_hash =  cur_attr_hash THEN 'UNCHANGED'
                    WHEN NOT in_stg AND in_cur AND NOT cur_is_deleted THEN 'DELETE'
                    ELSE 'NOOP'   -- ausente y ya eliminado -> idempotencia
                END AS action,
                -- ¿hay que cerrar la versión vigente actual?
                (   (in_stg AND in_cur AND cur_is_deleted)                       -- reaparición
                 OR (in_stg AND in_cur AND stg_attr_hash <> cur_attr_hash)       -- update
                 OR (NOT in_stg AND in_cur AND NOT cur_is_deleted)               -- delete
                ) AS close_current
            FROM j;
            """
        )

        # 2) Cerrar versiones vigentes afectadas (UPDATE / DELETE / reaparición).
        con.execute(
            f"""
            UPDATE core.movimientos_historico h
            SET is_current = FALSE, valid_to = {cd}
            WHERE h.version_id IN (
                SELECT cur_version_id FROM _actions
                WHERE close_current AND cur_version_id IS NOT NULL
            );
            """
        )

        # 3a) Insertar versiones nuevas para INSERT y UPDATE (atributos del corte).
        con.execute(
            f"""
            INSERT INTO core.movimientos_historico
            SELECT
                nextval('core.seq_version')                 AS version_id,
                s.movement_key, s.business_key_hash, s.occurrence_seq,
                s.id_cliente, s.mov_date, s.product, s.type, s.fund,
                s.amount, s.signed_amount, s.description, s.commercial_name, s.attr_hash,
                s.dq_amount_null, s.dq_amount_zero, s.dq_amount_negative,
                s.dq_date_reformatted, s.dq_type_recoded,
                a.action                                    AS change_type,
                TRUE                                        AS is_current,
                FALSE                                       AS is_deleted,
                {cd}                                        AS valid_from,
                NULL                                        AS valid_to,
                s._source_file, s._cut_label, s._load_ts
            FROM _actions a
            JOIN staging.movimientos_stg s USING (movement_key)
            WHERE a.action IN ('INSERT','UPDATE');
            """
        )

        # 3b) Insertar tombstones para DELETE (atributos copiados de la última versión).
        con.execute(
            f"""
            INSERT INTO core.movimientos_historico
            SELECT
                nextval('core.seq_version')                 AS version_id,
                h.movement_key, h.business_key_hash, h.occurrence_seq,
                h.id_cliente, h.mov_date, h.product, h.type, h.fund,
                h.amount, h.signed_amount, h.description, h.commercial_name, h.attr_hash,
                h.dq_amount_null, h.dq_amount_zero, h.dq_amount_negative,
                h.dq_date_reformatted, h.dq_type_recoded,
                'DELETE'                                     AS change_type,
                TRUE                                         AS is_current,
                TRUE                                         AS is_deleted,
                {cd}                                         AS valid_from,
                NULL                                         AS valid_to,
                h.source_file, '{cut.label}'                 AS cut_label, now() AS load_ts
            FROM _actions a
            JOIN core.movimientos_historico h ON h.version_id = a.cur_version_id
            WHERE a.action = 'DELETE';
            """
        )

        # 4) Bitácora de cambios (CDC) para eventos reales.
        con.execute(
            f"""
            INSERT INTO core.change_log
            SELECT '{run_id}', '{cut.label}', {cd},
                   movement_key, action, cur_attr_hash, stg_attr_hash, now()
            FROM _actions
            WHERE action IN ('INSERT','UPDATE','DELETE');
            """
        )

        counts = con.execute(
            """
            SELECT
                COUNT(*) FILTER (WHERE action='INSERT')    AS new,
                COUNT(*) FILTER (WHERE action='UPDATE')    AS changed,
                COUNT(*) FILTER (WHERE action='DELETE')    AS deleted,
                COUNT(*) FILTER (WHERE action='UNCHANGED') AS unchanged
            FROM _actions;
            """
        ).fetchone()

        con.execute("DROP TABLE IF EXISTS _actions;")
        con.execute("COMMIT;")
    except Exception:
        con.execute("ROLLBACK;")
        raise

    res = MergeResult(*counts)
    log.info(
        "SCD2 | corte %s -> nuevos=%s corregidos=%s eliminados=%s sin_cambio=%s",
        cut.label, f"{res.new:,}", f"{res.changed:,}", f"{res.deleted:,}", f"{res.unchanged:,}",
    )
    return res
