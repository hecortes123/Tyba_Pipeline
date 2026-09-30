"""
Tests del motor CDC/SCD2 con un dataset sintético pequeño y controlado.

Cubre: INSERT / UPDATE / DELETE / UNCHANGED, normalización sucia (type y date),
cuarentena, tombstones e idempotencia del merge.
"""
from __future__ import annotations

import duckdb
import pytest

from src.config import Cut
from src.db import bootstrap
from src import ingest, transform, scd2


def _write_parquet(con, path, rows):
    """Escribe filas [(id_cliente,date,product,type,fund,amount,description,commercial_name)]."""
    con.execute("DROP TABLE IF EXISTS _tmp;")
    con.execute(
        """CREATE TABLE _tmp(id_cliente VARCHAR, date VARCHAR, product VARCHAR,
           type VARCHAR, fund VARCHAR, amount DOUBLE, description VARCHAR,
           commercial_name VARCHAR);"""
    )
    con.executemany("INSERT INTO _tmp VALUES (?,?,?,?,?,?,?,?)", rows)
    con.execute(f"COPY _tmp TO '{path}' (FORMAT PARQUET);")
    con.execute("DROP TABLE _tmp;")


@pytest.fixture()
def con(tmp_path):
    c = duckdb.connect(str(tmp_path / "test.duckdb"))
    bootstrap(c)
    yield c
    c.close()


def _run_cut(con, cut, path):
    ingest.land_raw(con, cut, path)
    n_valid, n_quar = transform.build_staging(con, cut)
    res = scd2.merge_cut(con, cut, run_id="test")
    return res, n_valid, n_quar


def test_cdc_full_scenario(con, tmp_path):
    T = tmp_path / "T.parquet"
    T1 = tmp_path / "T1.parquet"

    # --- Corte T ---
    _write_parquet(con, T, [
        ("CLI1", "2024-01-01", "ETF", "entrada", "F1", 100.0, "desc", "BBVA"),   # unchanged
        ("CLI1", "2024-01-02", "CDT", "salida",  "F2", 200.0, "d",    "X"),      # -> corregido
        ("CLI2", "2024-01-03", "BONOS", "IN",    "F1", 300.0, "d",    "Y"),      # -> eliminado
        ("CLI2", "2024-01-01", "ETF", "entrada", "F1", 100.0, None,   None),     # unchanged (nulos)
        ("CLI3", "no-es-fecha", "ETF", "entrada","F1",  50.0, "d",    "Z"),      # -> cuarentena
    ])
    res, n_valid, n_quar = _run_cut(con, Cut("T", "T.parquet", "2024-11-01"), T)
    assert n_quar == 1                       # fila con fecha inválida
    assert n_valid == 4
    assert (res.new, res.changed, res.deleted, res.unchanged) == (4, 0, 0, 0)

    # --- Corte T1 --- (type/date "sucios" que deben normalizar a lo mismo)
    _write_parquet(con, T1, [
        ("CLI1", "2024-01-01", "ETF", "ENTRADA", "F1", 100.0, "desc", "BBVA"),   # unchanged (type sucio)
        ("CLI1", "02/01/2024", "CDT", "salida",  "F2", 250.0, "d",    "X"),      # corregido (date sucio, amount 200->250)
        ("CLI2", "2024-01-01", "ETF", "entrada", "F1", 100.0, None,   None),     # unchanged
        ("CLI9", "2024-05-05", "ACCIONES", "out","F3", 999.0, "n",    "W"),      # nuevo
        # CLI2/2024-01-03/BONOS ausente -> eliminado
    ])
    res, n_valid, n_quar = _run_cut(con, Cut("T1", "T1.parquet", "2024-11-02"), T1)
    assert (res.new, res.changed, res.deleted, res.unchanged) == (1, 1, 1, 2)

    # Estado vigente: 4 no-eliminados + 1 tombstone
    vig = con.execute(
        "SELECT COUNT(*) FROM core.movimientos_historico WHERE is_current AND NOT is_deleted"
    ).fetchone()[0]
    tomb = con.execute(
        "SELECT COUNT(*) FROM core.movimientos_historico WHERE is_current AND is_deleted"
    ).fetchone()[0]
    assert vig == 4 and tomb == 1

    # La corrección dejó 2 versiones para esa clave (una cerrada, una vigente)
    versions_cli1_cdt = con.execute(
        """SELECT COUNT(*) FROM core.movimientos_historico
           WHERE id_cliente='CLI1' AND product='CDT'"""
    ).fetchone()[0]
    assert versions_cli1_cdt == 2


def test_idempotencia_merge(con, tmp_path):
    """Re-mergear el mismo corte no debe generar cambios."""
    T = tmp_path / "T.parquet"
    _write_parquet(con, T, [
        ("CLI1", "2024-01-01", "ETF", "entrada", "F1", 100.0, "d", "BBVA"),
        ("CLI2", "2024-01-02", "CDT", "salida",  "F2", 200.0, "d", "X"),
    ])
    cut = Cut("T", "T.parquet", "2024-11-01")
    _run_cut(con, cut, T)
    v1 = con.execute("SELECT COUNT(*) FROM core.movimientos_historico").fetchone()[0]

    # Segundo merge del MISMO corte (mismos datos) -> todo UNCHANGED, sin versiones nuevas.
    res, _, _ = _run_cut(con, cut, T)
    v2 = con.execute("SELECT COUNT(*) FROM core.movimientos_historico").fetchone()[0]
    assert (res.new, res.changed, res.deleted) == (0, 0, 0)
    assert res.unchanged == 2
    assert v1 == v2


def test_reaparicion(con, tmp_path):
    """Un id eliminado que reaparece se reactiva como nueva versión INSERT."""
    T = tmp_path / "T.parquet"
    T1 = tmp_path / "T1.parquet"
    T2 = tmp_path / "T2.parquet"
    row = ("CLI1", "2024-01-01", "ETF", "entrada", "F1", 100.0, "d", "BBVA")

    _write_parquet(con, T, [row])
    _run_cut(con, Cut("T", "T.parquet", "2024-11-01"), T)

    _write_parquet(con, T1, [("CLI2", "2024-02-02", "CDT", "salida", "F2", 5.0, "d", "X")])
    r1, _, _ = _run_cut(con, Cut("T1", "T1.parquet", "2024-11-02"), T1)
    assert r1.deleted == 1  # CLI1 desaparece

    _write_parquet(con, T2, [row])  # CLI1 reaparece
    r2, _, _ = _run_cut(con, Cut("T2", "T2.parquet", "2024-11-03"), T2)
    assert r2.new == 1
    # Debe quedar vigente y NO eliminado
    state = con.execute(
        """SELECT is_deleted FROM core.movimientos_historico
           WHERE id_cliente='CLI1' AND is_current"""
    ).fetchone()[0]
    assert state is False
