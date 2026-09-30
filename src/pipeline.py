"""
Orquestador del pipeline ETL de movimientos financieros (Tyba).

Uso:
  python -m src.pipeline --all                 # procesa el manifiesto (T, luego T1)
  python -m src.pipeline --file f.parquet \\
         --cut-date 2024-11-03 --cut-label T2   # procesa un corte diario puntual
  python -m src.pipeline --all --reset         # recrea la base desde cero

Flujo por corte:  raw -> staging (+cuarentena) -> SCD2 merge -> analítica
Idempotente y seguro ante reprocesos y cortes fuera de orden.
"""
from __future__ import annotations

import argparse
import os
import shutil
import subprocess
import uuid
from datetime import datetime
from pathlib import Path

import duckdb

from . import analytics, ingest, transform, scd2
from .config import BASE_DIR, Cut, settings
from .db import bootstrap, connect
from .logging_conf import get_logger

log = get_logger("tyba.pipeline")


def run_dbt(args: list[str], fatal: bool = True) -> int:
    """Invoca dbt (build/test/docs) sobre la MISMA base DuckDB.

    Se llama después de cerrar la conexión de Python para no chocar con el
    lock de escritor único de DuckDB. Un fallo de `dbt build` (p.ej. un test
    que no pasa) detiene el pipeline: es el quality gate.
    """
    dbt_bin = shutil.which("dbt")
    if dbt_bin is None:
        msg = "dbt no está instalado (pip install dbt-duckdb)."
        if fatal:
            raise RuntimeError(msg)
        log.warning("%s Se omite la capa dbt.", msg)
        return 127
    project = str(BASE_DIR / "dbt")
    env = os.environ.copy()
    env["DB_PATH"] = str(settings.db_path.resolve())  # ruta absoluta para el profile
    cmd = [dbt_bin, *args, "--project-dir", project, "--profiles-dir", project]
    log.info("dbt  | %s", " ".join(args))
    r = subprocess.run(cmd, cwd=str(BASE_DIR), env=env)
    if r.returncode != 0 and fatal:
        raise RuntimeError(f"dbt {' '.join(args)} falló (exit {r.returncode})")
    return r.returncode


def _register_run(con, **kw) -> None:
    cols = ", ".join(kw.keys())
    ph = ", ".join(["?"] * len(kw))
    con.execute(f"INSERT INTO meta.pipeline_runs ({cols}) VALUES ({ph})", list(kw.values()))


def process_cut(con: duckdb.DuckDBPyConnection, cut: Cut) -> None:
    path = Path(settings.raw_dir) / cut.file
    if not path.exists():
        raise FileNotFoundError(f"No existe el archivo del corte: {path}")

    run_id = uuid.uuid4().hex[:12]
    started = datetime.now()
    sha = ingest.file_sha256(path)

    # --- Guarda de idempotencia / orden ------------------------------------
    if ingest.already_processed(con, cut.cut_date, sha):
        log.info("SKIP | corte %s (%s) ya procesado con el mismo archivo. No-op.",  # no-op: no se hace nada
                 cut.label, cut.cut_date)
        return
    max_cd = ingest.max_processed_cut_date(con)  # fecha del último corte procesado
    if max_cd is not None and datetime.fromisoformat(cut.cut_date).date() < max_cd:  # si la fecha del corte es menor a la fecha del último corte procesado, se lanza un error
        raise RuntimeError(  # se lanza un error porque los cortes deben ingerirse en orden cronológico
            f"Corte fuera de orden: {cut.cut_date} < último procesado {max_cd}. "
            "Los cortes deben ingerirse en orden cronológico."
        )

    log.info("=== Procesando corte %s (%s) — run %s ===", cut.label, cut.cut_date, run_id)  # se informa el corte que se está procesando

    # 1) RAW
    source_rows = ingest.land_raw(con, cut, path)
    # 2) STAGING (+ cuarentena)  # se construye la tabla staging con las filas validas y las que rompen la identidad
    n_valid, n_quar = transform.build_staging(con, cut)
    # 3) SCD2 merge (CDC)  # se realiza el merge del corte con el histórico para aplicar las correcciones
    res = scd2.merge_cut(con, cut, run_id)  # se devuelve el resultado del merge

    # 4) Marcar corte como procesado (idempotencia) + registrar corrida.  # se marca el corte como procesado y se registra la corrida
    con.execute(
        "INSERT INTO meta.cuts_processed VALUES (?, ?, ?, now())",
        [cut.cut_date, cut.file, sha],
    )
    _register_run(
        con, run_id=run_id, cut_label=cut.label, cut_date=cut.cut_date,
        source_file=cut.file, file_sha256=sha, source_rows=source_rows,
        rows_valid=n_valid, rows_quarantined=n_quar,
        rows_new=res.new, rows_changed=res.changed, rows_deleted=res.deleted,
        rows_unchanged=res.unchanged, status="SUCCESS",
        started_at=started, finished_at=datetime.now(),
    )


def run(all_cuts: bool, single: Cut | None, reset: bool, skip_dbt: bool = False) -> None:
    if reset and settings.db_path.exists():  # si se resetea la base de datos, se elimina la base de datos
        settings.db_path.unlink()
        log.info("Base reiniciada (--reset).")

    # --- Fase 1: ELT en Python (ingesta + staging + CDC/SCD2) --------------  # se realiza la ingesta, el staging y el CDC/SCD2
    con = connect()
    try:
        bootstrap(con)
        cuts = settings.cuts if all_cuts else [single]  # type: ignore[list-item]
        for cut in cuts:
            process_cut(con, cut)
    finally:
        con.close()  # libera el lock de DuckDB antes de que entre dbt (se libera el lock de DuckDB para que dbt pueda escribir en la base de datos)

    # --- Fase 2: dbt (marts + tests declarativos/singulares + docs) --------  # se realizan las pruebas de calidad, la documentación y el linaje
    if not skip_dbt:
        run_dbt(["build"])                       # modelos + tests (quality gate)  # se realizan las pruebas de calidad
        run_dbt(["docs", "generate"], fatal=False)  # catálogo/linaje (no crítico)  # se genera la documentación y el linaje
    else:
        # Fallback: si se omite dbt, se construyen las vistas de analítica en Python
        # para que los insights sigan generándose.  # si se omite dbt, se construyen las vistas de analítica en Python para que los insights sigan generándose
        con = connect()  # se conecta a la base de datos para construir las vistas de analítica
        try:
            analytics.build_views(con)
        finally:
            con.close()

    # --- Fase 3: insights desde los marts ----------------------------------
    con = connect()
    try:
        insights = analytics.generate_insights(con)
        _print_summary(con, insights)
    finally:
        con.close()


def _print_summary(con, insights: dict) -> None:
    log.info("──────────────── RESUMEN ────────────────")
    for r in insights["cdc_por_corte"]:
        log.info(
            "  CDC %s (%s): nuevos=%s corregidos=%s eliminados=%s",
            r["cut_label"], r["cut_date"], f"{r['nuevos']:,}",  # se informa el número de nuevos, corregidos y eliminados
            f"{r['corregidos']:,}", f"{r['eliminados']:,}",
        )
    e = insights["estado_vigente"]
    log.info("  Vigentes=%s | eliminados=%s | versiones=%s | cuarentena=%s",
             f"{e['movimientos_vigentes']:,}", f"{e['eliminados_vigentes']:,}",  # se informa el número de movimientos vigentes, eliminados y versiones totales
             f"{e['versiones_totales']:,}", f"{insights['cuarentena_total']:,}")
    log.info("  Insights -> data/output/insights.md  y  insights.json")  # se informa la ruta de los insights
    log.info("─────────────────────────────────────────")


def main() -> None:  # función principal que se encarga de ejecutar el pipeline
    p = argparse.ArgumentParser(description="Pipeline de movimientos financieros (Tyba)")
    g = p.add_mutually_exclusive_group(required=True)
    g.add_argument("--all", action="store_true", help="Procesa el manifiesto de cortes (T, T1)")
    g.add_argument("--file", help="Archivo parquet de un corte puntual (en data/raw)")
    p.add_argument("--cut-date", help="Fecha del corte (YYYY-MM-DD), requerida con --file")
    p.add_argument("--cut-label", help="Etiqueta del corte, requerida con --file")
    p.add_argument("--reset", action="store_true", help="Recrea la base desde cero")
    p.add_argument("--skip-dbt", action="store_true", help="Omite la capa dbt (usa vistas Python)")
    args = p.parse_args()

    single = None
    if args.file:
        if not (args.cut_date and args.cut_label):
            p.error("--file requiere --cut-date y --cut-label")
        single = Cut(label=args.cut_label, file=args.file, cut_date=args.cut_date)

    run(all_cuts=args.all, single=single, reset=args.reset, skip_dbt=args.skip_dbt)


if __name__ == "__main__":
    main()
