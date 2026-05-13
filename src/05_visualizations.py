"""
Archivo: 05_visualizations.py
Proyecto: Sistema Comercial Big Data

Objetivo:
Generar gráficos a partir de los KPIs generados por Spark.

Comando:
docker compose exec spark python src/05_visualizations.py
"""

from pathlib import Path

import pandas as pd
import matplotlib.pyplot as plt


# ============================================================
# RUTAS
# ============================================================

BASE_DIR = Path(__file__).resolve().parents[1]

KPI_DIR = BASE_DIR / "output" / "kpis"

CHARTS_DIR = BASE_DIR / "output" / "charts"

CHARTS_DIR.mkdir(parents=True, exist_ok=True)


# ============================================================
# LEER CSV
# ============================================================

def read_kpi_csv(filename):

    path = KPI_DIR / filename

    if not path.exists():
        raise FileNotFoundError(
            f"No existe {filename}. "
            "Primero ejecuta 03_batch_etl_spark.py"
        )

    return pd.read_csv(path)


# ============================================================
# GUARDAR GRÁFICO
# ============================================================

def save_chart(filename):

    output_path = CHARTS_DIR / filename

    plt.savefig(
        output_path,
        bbox_inches="tight",
        dpi=140
    )

    plt.close()

    print(f"Gráfico generado: {filename}")


# ============================================================
# GRÁFICO 1
# VENTAS POR CATEGORÍA
# ============================================================

def chart_ventas_por_categoria():

    df = read_kpi_csv("ventas_por_categoria.csv")

    df = df.sort_values(
        "monto_total",
        ascending=False
    )

    plt.figure(figsize=(10, 6))

    plt.bar(
        df["categoria_producto"],
        df["monto_total"]
    )

    plt.title("Ventas Totales Por Categoría")

    plt.xlabel("Categoría")

    plt.ylabel("Monto Total")

    plt.xticks(rotation=30)

    for index, value in enumerate(df["monto_total"]):

        plt.text(
            index,
            value,
            f"{value:,.0f}",
            ha="center",
            va="bottom",
            fontsize=8
        )

    save_chart("ventas_por_categoria.png")


# ============================================================
# GRÁFICO 2
# PRODUCTOS TOP
# ============================================================

def chart_productos_top():

    df = read_kpi_csv("productos_top.csv")

    top_df = df.head(10)

    plt.figure(figsize=(12, 6))

    plt.bar(
        top_df["nombre_producto"],
        top_df["cantidad_vendida"]
    )

    plt.title("Top 10 Productos Más Vendidos")

    plt.xlabel("Producto")

    plt.ylabel("Cantidad Vendida")

    plt.xticks(rotation=45, ha="right")

    for index, value in enumerate(top_df["cantidad_vendida"]):

        plt.text(
            index,
            value,
            str(value),
            ha="center",
            va="bottom",
            fontsize=8
        )

    save_chart("productos_top.png")


# ============================================================
# GRÁFICO 3
# MÉTODOS DE PAGO
# ============================================================

def chart_metodos_pago():

    df = read_kpi_csv("metodos_pago.csv")

    plt.figure(figsize=(8, 5))

    plt.bar(
        df["metodo_pago"],
        df["monto_total"]
    )

    plt.title("Monto Total Por Método De Pago")

    plt.xlabel("Método De Pago")

    plt.ylabel("Monto Total")

    for index, value in enumerate(df["monto_total"]):

        plt.text(
            index,
            value,
            f"{value:,.0f}",
            ha="center",
            va="bottom",
            fontsize=8
        )

    save_chart("metodos_pago.png")


# ============================================================
# GRÁFICO 4
# VENTAS POR CIUDAD
# ============================================================

def chart_ventas_por_ciudad():

    df = read_kpi_csv("ventas_por_ciudad.csv")

    top_df = df.head(10)

    plt.figure(figsize=(11, 6))

    plt.bar(
        top_df["ciudad"],
        top_df["ingresos"]
    )

    plt.title("Top 10 Ciudades Con Más Ingresos")

    plt.xlabel("Ciudad")

    plt.ylabel("Ingresos")

    plt.xticks(rotation=45)

    for index, value in enumerate(top_df["ingresos"]):

        plt.text(
            index,
            value,
            f"{value:,.0f}",
            ha="center",
            va="bottom",
            fontsize=8
        )

    save_chart("ventas_por_ciudad.png")


# ============================================================
# GRÁFICO 5
# CLIENTES PREMIUM
# ============================================================

def chart_clientes_premium():

    df = read_kpi_csv("clientes_premium.csv")

    top_df = df.head(10)

    plt.figure(figsize=(12, 6))

    plt.bar(
        top_df["nombre"],
        top_df["gasto_total"]
    )

    plt.title("Top 10 Clientes Premium")

    plt.xlabel("Cliente")

    plt.ylabel("Gasto Total")

    plt.xticks(rotation=45, ha="right")

    for index, value in enumerate(top_df["gasto_total"]):

        plt.text(
            index,
            value,
            f"{value:,.0f}",
            ha="center",
            va="bottom",
            fontsize=8
        )

    save_chart("clientes_premium.png")


# ============================================================
# MAIN
# ============================================================

def main():

    print("=" * 80)
    print("GENERANDO VISUALIZACIONES")
    print("=" * 80)

    chart_ventas_por_categoria()

    chart_productos_top()

    chart_metodos_pago()

    chart_ventas_por_ciudad()

    chart_clientes_premium()

    print("=" * 80)
    print("VISUALIZACIONES GENERADAS")
    print("Carpeta: output/charts/")
    print("=" * 80)


if __name__ == "__main__":
    main()