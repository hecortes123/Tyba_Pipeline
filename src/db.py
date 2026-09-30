"""
Conexión a DuckDB y creación del modelo físico (medallion + metadata).

Arquitectura por capas:
  raw     -> aterrizaje inmutable del parquet + linaje (append-only)
  staging -> tipado + normalización a MAYÚSCULAS + hashes + flags de calidad
  core    -> histórico SCD2, cuarentena y bitácora de cambios (CDC)
  analytics -> vistas de insights
  meta    -> control de corridas e idempotencia
"""
from __future__ import annotations

import duckdb

from .config import settings, SCHEMAS
from .logging_conf import get_logger

log = get_logger("tyba.db")


def connect() -> duckdb.DuckDBPyConnection:
    settings.output_dir.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(str(settings.db_path))
    # DuckDB usa todos los cores y hace spill a disco -> escala fuera de memoria.
    con.execute("PRAGMA threads=4;")
    return con


def bootstrap(con: duckdb.DuckDBPyConnection) -> None:
    """Crea esquemas y tablas si no existen. Idempotente."""
    for schema in SCHEMAS:
        con.execute(f"CREATE SCHEMA IF NOT EXISTS {schema};")

    # Secuencia para la clave surrogate de versiones del histórico.
    con.execute("CREATE SEQUENCE IF NOT EXISTS core.seq_version START 1;")

    # --- RAW: aterrizaje inmutable, append-only, con linaje ------------------ 
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS raw.movimientos (
            id_cliente       VARCHAR,
            date             VARCHAR,
            product          VARCHAR,
            type             VARCHAR,
            fund             VARCHAR,
            amount           DOUBLE,
            description      VARCHAR,
            commercial_name  VARCHAR,
            _source_file     VARCHAR,
            _cut_label       VARCHAR,
            _cut_date        DATE,
            _row_num         BIGINT,       -- orden físico dentro del archivo
            _load_ts         TIMESTAMP
        );
        """
    )

    # --- CORE: histórico SCD2 -----------------------------------------------
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS core.movimientos_historico (
            version_id         BIGINT PRIMARY KEY,
            movement_key       VARCHAR,      -- identidad = f(business_key_hash, occurrence_seq)
            business_key_hash  VARCHAR,      -- hash de la clave de negocio (identidad)
            occurrence_seq     INTEGER,      -- desempate de colisiones intra-corte
            -- clave de negocio (normalizada)
            id_cliente         VARCHAR,
            mov_date           DATE,
            product            VARCHAR,
            type               VARCHAR,
            fund               VARCHAR,
            -- atributos mutables (normalizados)
            amount             DOUBLE,
            signed_amount      DOUBLE,       -- magnitud con signo derivado de type
            description        VARCHAR,
            commercial_name    VARCHAR,
            attr_hash          VARCHAR,      -- hash de atributos (detección de cambios)
            -- flags de calidad (soft)
            dq_amount_null     BOOLEAN,
            dq_amount_zero     BOOLEAN,
            dq_amount_negative BOOLEAN,   -- magnitud negativa (anomalía real)
            dq_date_reformatted BOOLEAN,
            dq_type_recoded    BOOLEAN,
            -- metadata SCD2 + linaje
            change_type        VARCHAR,      -- INSERT | UPDATE | DELETE
            is_current         BOOLEAN,
            is_deleted         BOOLEAN,      -- tombstone de eliminación
            valid_from         DATE,
            valid_to           DATE,
            source_file        VARCHAR,
            cut_label          VARCHAR,
            load_ts            TIMESTAMP
        );
        """
    )
    # Índices que ayudan al merge y a las consultas del estado vigente (histórico)
    con.execute(
        "CREATE INDEX IF NOT EXISTS ix_hist_current ON core.movimientos_historico(movement_key, is_current);"
    )

    # --- CORE: cuarentena (registros que rompen la identidad de un movimiento) ---------------
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS core.movimientos_cuarentena (
            id_cliente      VARCHAR,
            date            VARCHAR,
            product         VARCHAR,
            type            VARCHAR,
            fund            VARCHAR,
            amount          DOUBLE,
            description     VARCHAR,
            commercial_name VARCHAR,
            _reasons        VARCHAR,      -- motivos separados por ';'
            _source_file    VARCHAR,
            _cut_label      VARCHAR,
            _cut_date       DATE,
            _load_ts        TIMESTAMP
        );
        """
    )

    # --- CORE: bitácora de cambios (CDC por corte) --------------------------
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS core.change_log (
            run_id         VARCHAR,
            cut_label      VARCHAR,
            cut_date       DATE,
            movement_key   VARCHAR,
            change_type    VARCHAR,       -- INSERT | UPDATE | DELETE
            prev_attr_hash VARCHAR,
            new_attr_hash  VARCHAR,
            ts             TIMESTAMP
        );
        """
    )

    # --- META: control de corridas e idempotencia ---------------------------
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS meta.pipeline_runs (
            run_id           VARCHAR,
            cut_label        VARCHAR,
            cut_date         DATE,
            source_file      VARCHAR,
            file_sha256      VARCHAR,
            source_rows      BIGINT,
            rows_valid       BIGINT,
            rows_quarantined BIGINT,
            rows_new         BIGINT,
            rows_changed     BIGINT,
            rows_deleted     BIGINT,
            rows_unchanged   BIGINT,
            status           VARCHAR,
            started_at       TIMESTAMP,
            finished_at      TIMESTAMP
        );
        """
    )
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS meta.cuts_processed (
            cut_date     DATE,
            source_file  VARCHAR,
            file_sha256  VARCHAR,
            processed_at TIMESTAMP
        );
        """
    )
    log.info("Esquemas verificados/creados (bootstrap idempotente).")
