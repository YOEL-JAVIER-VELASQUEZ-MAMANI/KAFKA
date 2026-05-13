# 01_generate_batch_data.py (Adaptado al caso comercial
"""
Archivo: 01_generate_batch_data.py
Proyecto: Sistema Comercial Big Data

Objetivo:
Generar datos históricos ficticios para el análisis comercial.

Archivos generados:
- data/raw/productos.csv
- data/raw/ventas.csv
- data/raw/clientes.json
- data/raw/ventas.json
- data/raw/logs.txt

Comando:
docker compose exec spark python src/01_generate_batch_data.py
"""

from pathlib import Path
from datetime import datetime, timedelta
import random
import json

import numpy as np
import pandas as pd
from faker import Faker


# ============================================================
# CONFIGURACIÓN GENERAL
# ============================================================

SEED = 2026
random.seed(SEED)
np.random.seed(SEED)

fake = Faker("es_ES")
Faker.seed(SEED)

N_PRODUCTOS = 500
N_CLIENTES = 10000
N_VENTAS = 50000
N_VENTAS_ONLINE = 15000
N_LOGS = 8000

BASE_DIR = Path(__file__).resolve().parents[1]
RAW_DIR = BASE_DIR / "data" / "raw"
RAW_DIR.mkdir(parents=True, exist_ok=True)


# ============================================================
# FUNCIONES AUXILIARES
# ============================================================


def save_csv(df: pd.DataFrame, filename: str):
    output_path = RAW_DIR / filename
    df.to_csv(output_path, index=False, encoding="utf-8")
    print(f"{filename} creado: {len(df):,} registros")



def save_json(df: pd.DataFrame, filename: str):
    output_path = RAW_DIR / filename

    df.to_json(
        output_path,
        orient="records",
        indent=4,
        force_ascii=False
    )

    print(f"{filename} creado: {len(df):,} registros")



def save_txt(lines, filename: str):
    output_path = RAW_DIR / filename

    with open(output_path, "w", encoding="utf-8") as f:
        for line in lines:
            f.write(line + "\n")

    print(f"{filename} creado: {len(lines):,} registros")


# ============================================================
# PRODUCTOS
# ============================================================


def generate_productos() -> pd.DataFrame:

    categorias = [
        "Electronica",
        "Hogar",
        "Ropa",
        "Deportes",
        "Tecnologia"
    ]

    rows = []

    for i in range(1, N_PRODUCTOS + 1):

        categoria = random.choice(categorias)

        precio = round(random.uniform(20, 5000), 2)

        stock = random.randint(10, 500)

        rows.append({
            "producto_id": f"PROD-{i:05d}",
            "nombre_producto": fake.word().capitalize(),
            "categoria": categoria,
            "precio": precio,
            "stock": stock,
            "fecha_registro": fake.date_between(start_date="-3y", end_date="today")
        })

    return pd.DataFrame(rows)


# ============================================================
# CLIENTES
# ============================================================


def generate_clientes() -> pd.DataFrame:

    ciudades = [
        "Lima",
        "Arequipa",
        "Cusco",
        "Piura",
        "Trujillo"
    ]

    segmentos = [
        "nuevo",
        "frecuente",
        "premium"
    ]

    rows = []

    for i in range(1, N_CLIENTES + 1):

        rows.append({
            "cliente_id": f"CLI-{i:06d}",
            "nombre": fake.name(),
            "correo": fake.email(),
            "telefono": fake.phone_number(),
            "ciudad": random.choice(ciudades),
            "segmento": random.choice(segmentos),
            "fecha_registro": fake.date_between(start_date="-5y", end_date="today")
        })

    return pd.DataFrame(rows)


# ============================================================
# VENTAS HISTÓRICAS
# ============================================================


def generate_ventas(productos_df: pd.DataFrame, clientes_df: pd.DataFrame) -> pd.DataFrame:

    productos = productos_df.to_dict("records")
    clientes = clientes_df.to_dict("records")

    payment_methods = [
        "tarjeta",
        "yape",
        "plin",
        "efectivo"
    ]

    rows = []

    for i in range(1, N_VENTAS + 1):

        producto = random.choice(productos)
        cliente = random.choice(clientes)

        cantidad = random.randint(1, 5)

        subtotal = round(producto["precio"] * cantidad, 2)

        descuento = round(subtotal * random.uniform(0, 0.15), 2)

        total = round(subtotal - descuento, 2)

        rows.append({
            "venta_id": f"VEN-{i:07d}",
            "cliente_id": cliente["cliente_id"],
            "producto_id": producto["producto_id"],
            "categoria": producto["categoria"],
            "cantidad": cantidad,
            "precio_unitario": producto["precio"],
            "subtotal": subtotal,
            "descuento": descuento,
            "total": total,
            "metodo_pago": random.choice(payment_methods),
            "fecha_venta": fake.date_time_between(start_date="-6mo", end_date="now")
        })

    return pd.DataFrame(rows)


# ============================================================
# VENTAS ONLINE JSON
# ============================================================


def generate_ventas_online(productos_df: pd.DataFrame) -> pd.DataFrame:

    productos = productos_df.to_dict("records")

    plataformas = [
        "web",
        "android",
        "ios"
    ]

    rows = []

    for i in range(1, N_VENTAS_ONLINE + 1):

        producto = random.choice(productos)

        rows.append({
            "venta_online_id": f"VON-{i:06d}",
            "producto_id": producto["producto_id"],
            "categoria": producto["categoria"],
            "plataforma": random.choice(plataformas),
            "monto": round(random.uniform(30, 4000), 2),
            "fecha": fake.date_time_between(start_date="-4mo", end_date="now")
        })

    return pd.DataFrame(rows)


# ============================================================
# LOGS DEL SISTEMA
# ============================================================


def generate_logs():

    eventos = [
        "LOGIN_OK",
        "ERROR_PAGO",
        "VENTA_EXITOSA",
        "CLIENTE_NUEVO",
        "ERROR_SERVIDOR",
        "COMPRA_CANCELADA"
    ]

    lines = []

    for _ in range(N_LOGS):

        timestamp = fake.date_time_between(start_date="-2mo", end_date="now")

        event = random.choice(eventos)

        line = f"{timestamp} - {event}"

        lines.append(line)

    return lines


# ============================================================
# MAIN
# ============================================================


def main():

    print("Generando datos históricos comerciales...")
    print("Carpeta de salida:", RAW_DIR)
    print("-" * 60)

    productos_df = generate_productos()
    save_csv(productos_df, "productos.csv")

    clientes_df = generate_clientes()
    save_json(clientes_df, "clientes.json")

    ventas_df = generate_ventas(productos_df, clientes_df)
    save_csv(ventas_df, "ventas.csv")

    ventas_online_df = generate_ventas_online(productos_df)
    save_json(ventas_online_df, "ventas.json")

    logs = generate_logs()
    save_txt(logs, "logs.txt")

    print("-" * 60)
    print("Proceso finalizado correctamente")
    print("Archivos generados en data/raw/")
    print("-" * 60)

    print("Vista previa de ventas.csv")
    print(ventas_df.head(5).to_string(index=False))

    print("-" * 60)
    print("Ventas por categoría")
    print(ventas_df["categoria"].value_counts().to_string())


if __name__ == "__main__":
    main()
