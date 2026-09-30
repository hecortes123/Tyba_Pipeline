# Insights — Pipeline de Movimientos Financieros (Tyba)

## 1. Evolución entre cortes (CDC)

| Corte | Fecha | Nuevos | Corregidos | Eliminados |
|---|---|---:|---:|---:|
| T | 2024-11-01 | 50,000 | 0 | 0 |
| T1 | 2024-11-02 | 9,991 | 3,888 | 10,991 |

## 2. Estado vigente

- Movimientos vigentes: **49,000**
- Marcados como eliminados (tombstones vigentes): 10,991
- Versiones totales en el histórico: 74,870
- Clientes activos: 3,000

## 3. Flujo neto por producto (entradas − salidas)

| Producto | Nº mov. | Flujo neto | Entradas | Salidas |
|---|---:|---:|---:|---:|
| DIVISAS | 6,107 | 18,940,076,739 | 75,138,417,534 | 62,921,159,427 |
| CDT | 6,181 | 18,721,810,387 | 76,419,287,643 | 63,699,954,906 |
| BONOS | 6,169 | 17,928,622,888 | 75,886,923,817 | 64,889,050,401 |
| ETF | 6,065 | 16,428,332,658 | 73,906,186,583 | 63,183,236,434 |
| FONDO DE PENSIÓN | 6,083 | 15,172,266,533 | 72,787,369,996 | 64,114,511,723 |
| ACCIONES | 6,079 | 14,700,767,095 | 72,745,479,337 | 64,713,897,170 |
| CUENTA DE AHORRO | 6,221 | 12,691,224,504 | 73,918,114,979 | 67,296,817,931 |
| FONDO DE INVERSIÓN | 6,095 | 12,439,537,763 | 74,256,469,996 | 67,428,964,105 |

## 4. Calidad de datos (estado vigente)

- Registros en cuarentena (excluidos del histórico): **0**
- amount nulos: 1,440
- amount en cero: 903
- amount negativos — magnitud inválida, dirección la da `type`: 989
- fechas reformateadas (no-ISO DD/MM/YYYY → DATE): 3,433
- `type` recodificado a ENTRADA/SALIDA: 1,929

## 5. Top 10 clientes por volumen (|amount| vigente)

| Cliente | Nº mov. | Volumen |
|---|---:|---:|
| CLI001058 | 32 | 846,560,368 |
| CLI000021 | 30 | 804,896,686 |
| CLI002487 | 28 | 797,963,069 |
| CLI000434 | 24 | 794,577,194 |
| CLI002770 | 26 | 792,710,060 |
| CLI000975 | 30 | 788,604,142 |
| CLI001935 | 31 | 761,725,134 |
| CLI000075 | 26 | 758,524,109 |
| CLI002675 | 28 | 758,431,202 |
| CLI001315 | 24 | 746,790,906 |
