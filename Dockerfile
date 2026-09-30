FROM python:3.12-slim

# Buenas prácticas de runtime Python en contenedor
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /app

# 1) Dependencias primero (mejor cacheo de capas)
COPY requirements.txt .
RUN pip install -r requirements.txt

# 2) Código, proyecto dbt y datos
COPY src/ ./src/
COPY dbt/ ./dbt/
COPY tests/ ./tests/
COPY data/ ./data/

# Por defecto procesa el manifiesto completo (corte T y luego T1) y genera insights.
CMD ["python", "-m", "src.pipeline", "--all"]
