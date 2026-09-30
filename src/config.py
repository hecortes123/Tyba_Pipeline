"""
Configuración central del pipeline.

Todas las decisiones de diseño que dependen del negocio viven aquí para que
sean explícitas, auditables y fáciles de cambiar sin tocar la lógica del ETL.
"""
from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path

# --------------------------------------------------------------------------- #
# Rutas
# --------------------------------------------------------------------------- #
# BASE_DIR = raíz del repo (un nivel arriba de src/)
BASE_DIR = Path(__file__).resolve().parent.parent
RAW_DIR = Path(os.getenv("RAW_DIR", BASE_DIR / "data" / "raw"))
OUTPUT_DIR = Path(os.getenv("OUTPUT_DIR", BASE_DIR / "data" / "output"))
DB_PATH = Path(os.getenv("DB_PATH", OUTPUT_DIR / "tyba.duckdb"))

# --------------------------------------------------------------------------- #
# Clave de negocio  (DECISIÓN DE DISEÑO — ver README "Definir la clave de negocio")
# --------------------------------------------------------------------------- #
# El origen NO trae un id de transacción (el glosario promete `id`, pero el
# archivo solo trae `id_cliente`, que no es único: 17 filas por cliente).
# Se define la identidad de un MOVIMIENTO con las columnas que describen el
# hecho y que una corrección NO debería alterar:
BUSINESS_KEY = ["id_cliente", "mov_date", "product", "type", "fund"]

# Atributos MUTABLES: lo que una corrección puede editar. El attr_hash se
# calcula sobre estas columnas para detectar correcciones de forma barata.
MUTABLE_ATTRS = ["amount", "description", "commercial_name"]

# --------------------------------------------------------------------------- #
# Normalización  (DECISIÓN: canonicalizamos a MAYÚSCULAS SOSTENIDAS)
# --------------------------------------------------------------------------- #
# Mapa canónico de `type` -> {ENTRADA, SALIDA}. Se aplica sobre UPPER(TRIM()).
# Cualquier valor fuera de este mapa se envía a cuarentena (rompe la identidad).
TYPE_CANONICAL_MAP = {
    "ENTRADA": "ENTRADA",
    "IN": "ENTRADA",
    "SALIDA": "SALIDA",
    "OUT": "SALIDA",
}

# Formatos de fecha aceptados (además del ISO nativo YYYY-MM-DD).
# DECISIÓN: el 7% no-ISO viene como DD/MM/YYYY (convención LATAM/Colombia).
DATE_ALT_FORMATS = ["%d/%m/%Y"]

# Centinela para representar NULL dentro de los hashes (evita que NULL y ''
# colisionen y que la concatenación sea ambigua).
NULL_SENTINEL = "<NULL>"
HASH_SEP = "||"  # separador que no aparece en los datos para evitar colisiones

# --------------------------------------------------------------------------- #
# Declaración de cortes diarios
# --------------------------------------------------------------------------- #
# En producción se procesa un corte por día. Aquí se declaran los dos cortes
# entregados por Tyba en orden cronológico. `cut_date` es la fecha "as-of" del
# corte (distinta de la columna `date`, que es la fecha del hecho).
@dataclass(frozen=True)
class Cut:
    label: str
    file: str
    cut_date: str  # formato de fecha: ISO YYYY-MM-DD


# Lista de cortes diarios
CUTS: list[Cut] = [
    Cut(label="T", file="movimientos_dia_T.parquet", cut_date="2024-11-01"),
    Cut(label="T1", file="movimientos_dia_T1.parquet", cut_date="2024-11-02"),
]

# --------------------------------------------------------------------------- #
# Esquemas de la base de datos
# --------------------------------------------------------------------------- #
SCHEMAS = ["raw", "staging", "core", "analytics", "meta"]


@dataclass
class Settings:
    raw_dir: Path = RAW_DIR  # directorio de entrada de los archivos de origen  
    output_dir: Path = OUTPUT_DIR  # directorio de salida de los archivos de salida y la base de datos
    db_path: Path = DB_PATH  # ruta de la base de datos DuckDB
    cuts: list[Cut] = field(default_factory=lambda: CUTS)  # lista de cortes diarios declarada arriba


settings = Settings()   # instancia de la configuración
