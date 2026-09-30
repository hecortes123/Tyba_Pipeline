.PHONY: up run reset test dbt-build dbt-test dbt-docs query clean

# --- Docker ---------------------------------------------------------------
up:            ## Corre el pipeline completo (Python + dbt) en Docker
	docker compose up --build

# --- Local ----------------------------------------------------------------
run:           ## Pipeline completo en local (ingesta + SCD2 + dbt + insights)
	python -m src.pipeline --all

reset:         ## Pipeline completo desde cero (recrea la base)
	python -m src.pipeline --all --reset

test:          ## Tests del motor CDC/SCD2 (pytest)
	python -m pytest tests/ -v

# --- dbt (capa de marts + tests + docs) -----------------------------------
dbt-build:     ## Construye marts y ejecuta todos los tests dbt
	DB_PATH=$(PWD)/data/output/tyba.duckdb dbt build --project-dir dbt --profiles-dir dbt

dbt-test:      ## Solo ejecuta los tests dbt (generic + singular)
	DB_PATH=$(PWD)/data/output/tyba.duckdb dbt test --project-dir dbt --profiles-dir dbt

dbt-docs:      ## Genera y sirve docs + grafo de linaje (http://localhost:8080)
	DB_PATH=$(PWD)/data/output/tyba.duckdb dbt docs generate --project-dir dbt --profiles-dir dbt
	DB_PATH=$(PWD)/data/output/tyba.duckdb dbt docs serve --project-dir dbt --profiles-dir dbt

query:         ## Abre la DuckDB en la CLI
	duckdb data/output/tyba.duckdb

clean:         ## Borra artefactos generados
	rm -f data/output/*.duckdb data/output/*.wal data/output/insights.*
	rm -rf __pycache__ src/__pycache__ tests/__pycache__ .pytest_cache
	rm -rf dbt/target dbt/logs dbt/dbt_packages
