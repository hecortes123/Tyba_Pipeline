"""
Capa de analítica / insights.

Crea vistas consultables sobre el estado vigente del histórico y genera un
reporte de insights (insights.md + insights.json) en data/output.
"""
from __future__ import annotations

import json
from pathlib import Path

import duckdb

from .config import settings
from .logging_conf import get_logger

log = get_logger("tyba.analytics")


def build_views(con: duckdb.DuckDBPyConnection) -> None:
    """Vistas de negocio sobre el histórico SCD2."""
    # Estado vigente = últimas versiones NO eliminadas.
    con.execute(
        """
        CREATE OR REPLACE VIEW analytics.movimientos_vigentes AS
        SELECT * FROM core.movimientos_historico
        WHERE is_current AND NOT is_deleted;
        """
    )
    # Flujo neto por producto (entradas - salidas) sobre estado vigente.
    con.execute(
        """
        CREATE OR REPLACE VIEW analytics.flujo_por_producto AS
        SELECT
            product,
            COUNT(*)                                            AS n_movimientos,
            SUM(signed_amount)                                  AS flujo_neto,
            SUM(CASE WHEN type='ENTRADA' THEN amount END)       AS total_entradas,
            SUM(CASE WHEN type='SALIDA'  THEN amount END)       AS total_salidas
        FROM analytics.movimientos_vigentes
        GROUP BY product
        ORDER BY flujo_neto DESC;
        """
    )
    # Flujo neto por fondo.
    con.execute(
        """
        CREATE OR REPLACE VIEW analytics.flujo_por_fondo AS
        SELECT fund,
               COUNT(*) AS n_movimientos,
               SUM(signed_amount) AS flujo_neto
        FROM analytics.movimientos_vigentes
        GROUP BY fund ORDER BY flujo_neto DESC;
        """
    )
    log.info("Vistas de analítica creadas.")


def _df(con, sql):
    return con.execute(sql).df()


def generate_insights(con: duckdb.DuckDBPyConnection) -> dict:
    """Calcula insights y los devuelve como dict (además de exportarlos)."""
    insights: dict = {}

    # --- Resumen CDC entre cortes (nuevos/corregidos/eliminados) ------------
    cdc = _df(
        con,
        """
        SELECT cut_label, cut_date::VARCHAR AS cut_date,
               COUNT(*) FILTER (WHERE change_type='INSERT') AS nuevos,
               COUNT(*) FILTER (WHERE change_type='UPDATE') AS corregidos,
               COUNT(*) FILTER (WHERE change_type='DELETE') AS eliminados
        FROM core.change_log
        GROUP BY cut_label, cut_date ORDER BY cut_date;
        """,
    )
    insights["cdc_por_corte"] = cdc.to_dict(orient="records")

    # --- Estado vigente -----------------------------------------------------
    estado = _df(
        con,
        """
        SELECT
          (SELECT COUNT(*) FROM analytics.movimientos_vigentes)                       AS movimientos_vigentes,
          (SELECT COUNT(*) FROM core.movimientos_historico WHERE is_current AND is_deleted) AS eliminados_vigentes,
          (SELECT COUNT(*) FROM core.movimientos_historico)                          AS versiones_totales,
          (SELECT COUNT(DISTINCT id_cliente) FROM analytics.movimientos_vigentes)    AS clientes_activos;
        """,
    )
    insights["estado_vigente"] = estado.to_dict(orient="records")[0]

    # --- Flujo neto por producto -------------------------------------------
    insights["flujo_por_producto"] = _df(
        con, "SELECT * FROM analytics.flujo_por_producto"
    ).to_dict(orient="records")

    # --- Flujo neto por tipo (dirección) -----------------------------------
    insights["por_tipo"] = _df(
        con,
        """
        SELECT type, COUNT(*) n_movimientos, SUM(amount) total_monto
        FROM analytics.movimientos_vigentes GROUP BY type ORDER BY 2 DESC;
        """,
    ).to_dict(orient="records")

    # --- Calidad de datos (sobre estado vigente) ---------------------------
    calidad = _df(
        con,
        """
        SELECT
          COUNT(*)::BIGINT                     total,
          SUM(dq_amount_null::INT)::BIGINT      amount_nulos,
          SUM(dq_amount_zero::INT)::BIGINT      amount_ceros,
          SUM(dq_amount_negative::INT)::BIGINT  amount_negativos,
          SUM(dq_date_reformatted::INT)::BIGINT fecha_reformateada,
          SUM(dq_type_recoded::INT)::BIGINT     type_recodificado
        FROM analytics.movimientos_vigentes;
        """,
    )
    insights["calidad_vigente"] = calidad.to_dict(orient="records")[0]
    insights["cuarentena_total"] = int(
        con.execute("SELECT COUNT(*) FROM core.movimientos_cuarentena").fetchone()[0]
    )

    # --- Top clientes por volumen (|monto|) --------------------------------
    insights["top_clientes"] = _df(
        con,
        """
        SELECT id_cliente,
               COUNT(*) n_movimientos,
               SUM(abs(amount)) volumen
        FROM analytics.movimientos_vigentes
        GROUP BY id_cliente ORDER BY volumen DESC NULLS LAST LIMIT 10;
        """,
    ).to_dict(orient="records")

    _export(insights)
    return insights


def _export(insights: dict) -> None:
    out = settings.output_dir
    out.mkdir(parents=True, exist_ok=True)

    # JSON (machine-readable)
    (out / "insights.json").write_text(
        json.dumps(insights, indent=2, ensure_ascii=False, default=str), encoding="utf-8"
    )

    # Markdown
    md = _render_markdown(insights)
    (out / "insights.md").write_text(md, encoding="utf-8")
    log.info("Insights exportados: %s , %s", out / "insights.md", out / "insights.json")


def _render_markdown(ins: dict) -> str:
    lines = ["# Insights — Pipeline de Movimientos Financieros (Tyba)\n"]

    lines.append("## 1. Evolución entre cortes (CDC)\n")
    lines.append("| Corte | Fecha | Nuevos | Corregidos | Eliminados |")
    lines.append("|---|---|---:|---:|---:|")
    for r in ins["cdc_por_corte"]:
        lines.append(
            f"| {r['cut_label']} | {r['cut_date']} | {r['nuevos']:,} | "
            f"{r['corregidos']:,} | {r['eliminados']:,} |"
        )

    e = ins["estado_vigente"]
    lines.append("\n## 2. Estado vigente\n")
    lines.append(f"- Movimientos vigentes: **{e['movimientos_vigentes']:,}**")
    lines.append(f"- Marcados como eliminados (tombstones vigentes): {e['eliminados_vigentes']:,}")
    lines.append(f"- Versiones totales en el histórico: {e['versiones_totales']:,}")
    lines.append(f"- Clientes activos: {e['clientes_activos']:,}")

    lines.append("\n## 3. Flujo neto por producto (entradas − salidas)\n")
    lines.append("| Producto | Nº mov. | Flujo neto | Entradas | Salidas |")
    lines.append("|---|---:|---:|---:|---:|")
    for r in ins["flujo_por_producto"]:
        lines.append(
            f"| {r['product']} | {r['n_movimientos']:,} | {r['flujo_neto']:,.0f} | "
            f"{(r['total_entradas'] or 0):,.0f} | {(r['total_salidas'] or 0):,.0f} |"
        )

    q = ins["calidad_vigente"]
    lines.append("\n## 4. Calidad de datos (estado vigente)\n")
    lines.append(f"- Registros en cuarentena (excluidos del histórico): **{ins['cuarentena_total']:,}**")
    lines.append(f"- amount nulos: {int(q['amount_nulos']):,}")
    lines.append(f"- amount en cero: {int(q['amount_ceros']):,}")
    lines.append(f"- amount negativos — magnitud inválida, dirección la da `type`: {int(q['amount_negativos']):,}")
    lines.append(f"- fechas reformateadas (no-ISO DD/MM/YYYY → DATE): {int(q['fecha_reformateada']):,}")
    lines.append(f"- `type` recodificado a ENTRADA/SALIDA: {int(q['type_recodificado']):,}")

    lines.append("\n## 5. Top 10 clientes por volumen (|amount| vigente)\n")
    lines.append("| Cliente | Nº mov. | Volumen |")
    lines.append("|---|---:|---:|")
    for r in ins["top_clientes"]:
        lines.append(f"| {r['id_cliente']} | {r['n_movimientos']:,} | {(r['volumen'] or 0):,.0f} |")

    return "\n".join(lines) + "\n"
