"""
Ingesta cruda: aterriza el parquet en `raw.movimientos` con linaje completo.

Diseño a escala: DuckDB lee el parquet directamente (columnar, vectorizado,
con soporte out-of-core), por lo que la ingesta es set-based y no carga el
archivo completo en memoria de Python.
"""
from __future__ import annotations

import hashlib
from pathlib import Path

import duckdb

from .config import Cut
from .logging_conf import get_logger

log = get_logger("tyba.ingest")


def file_sha256(path: Path) -> str:
    """Hash del contenido del archivo -> huella para idempotencia."""
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def already_processed(con: duckdb.DuckDBPyConnection, cut_date: str, sha: str) -> bool:
    row = con.execute(
        "SELECT COUNT(*) FROM meta.cuts_processed WHERE cut_date = ? AND file_sha256 = ?",
        [cut_date, sha],
    ).fetchone()
    return row[0] > 0  # si la fila existe, se considera que el archivo ya fue procesado


def max_processed_cut_date(con: duckdb.DuckDBPyConnection):
    return con.execute("SELECT MAX(cut_date) FROM meta.cuts_processed").fetchone()[0]  # fecha del último corte procesado


def land_raw(con: duckdb.DuckDBPyConnection, cut: Cut, path: Path) -> int:
    """Inserta el corte en raw.movimientos. Devuelve el número de filas del origen."""
    # Reproceso del mismo corte: limpiamos su aterrizaje previo para no duplicar
    # el linaje en raw (el histórico es idempotente por sí solo).
    con.execute(
        "DELETE FROM raw.movimientos WHERE _cut_label = ? AND _cut_date = ?",
        [cut.label, cut.cut_date],
    )  # se eliminan las filas del corte si ya existen para no duplicar el linaje en raw
    con.execute(
        f"""
        INSERT INTO raw.movimientos
        SELECT
            id_cliente, date, product, type, fund, amount, description, commercial_name,
            '{path.name}'                              AS _source_file,
            '{cut.label}'                              AS _cut_label,
            DATE '{cut.cut_date}'                      AS _cut_date,
            row_number() OVER ()                       AS _row_num,
            now()                                      AS _load_ts
        FROM read_parquet('{path.as_posix()}');
        """
    )
    n = con.execute(
        "SELECT COUNT(*) FROM raw.movimientos WHERE _cut_label = ? AND _cut_date = ?",
        [cut.label, cut.cut_date],
    ).fetchone()[0]
    log.info("RAW  | corte %s (%s): %s filas aterrizadas", cut.label, cut.cut_date, f"{n:,}")
    return n
