# Guía de contribución

Gracias por tu interés en el proyecto. Esta guía cubre cómo levantar el entorno,
correr las pruebas y las convenciones del repo.

## Requisitos

- Python 3.12+
- Docker (opcional, para la corrida reproducible con `docker compose`)

## Setup local

```bash
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
```

## Correr el pipeline

```bash
make run        # pipeline completo: ingesta + CDC/SCD2 + dbt build + insights
make reset      # igual, recreando la base desde cero
make up         # equivalente en Docker (docker compose up --build)
```

## Pruebas (dos capas)

```bash
make test       # pytest: motor CDC/SCD2 (4 casos, idempotencia, reaparición, cuarentena)
make dbt-test   # tests dbt: declarativos + singulares (invariantes SCD2) sobre datos reales
```

Ambas capas deben quedar en verde antes de abrir un PR. El CI (GitHub Actions) las
ejecuta automáticamente en cada push a `main` y en cada PR.

## Documentación y linaje

```bash
make dbt-docs   # genera y sirve el grafo de linaje interactivo (http://localhost:8080)
```

## Dónde tocar qué

| Necesitas… | Archivo |
|---|---|
| Cambiar la clave de negocio o los atributos mutables | `src/config.py` (`BUSINESS_KEY`, `MUTABLE_ATTRS`) |
| Añadir un corte al manifiesto | `src/config.py` (`CUTS`) |
| Ajustar normalización / reglas de calidad | `src/transform.py` |
| Modificar la lógica CDC/SCD2 | `src/scd2.py` |
| Agregar un mart o un test declarativo | `dbt/models/` |
| Agregar una invariante como test | `dbt/tests/` |

### Procesar un corte diario puntual (no en el manifiesto)

```bash
python -m src.pipeline --file movimientos_dia_T2.parquet \
       --cut-date 2024-11-03 --cut-label T2
```

## Convenciones

- **Commits:** [Conventional Commits](https://www.conventionalcommits.org)
  (`feat:`, `fix:`, `docs:`, `refactor:`, `test:`, `chore:`).
- **SQL de dbt:** minúsculas, CTEs con nombres claros, `ref()`/`source()` siempre.
- **Datos:** la normalización canónica es a **MAYÚSCULAS** y se aplica **antes** de
  calcular los hashes (ver `src/transform.py`). Mantener esa invariante.
- **Idempotencia:** cualquier cambio en el motor debe preservar que reprocesar un
  corte sea un no-op (cubierto por `tests/test_pipeline.py::test_idempotencia_merge`).
