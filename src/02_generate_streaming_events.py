"""
Archivo: 02_generate_streaming_events.py
Proyecto: Sistema Comercial Big Data

Objetivo:
Generar eventos simulados de ventas online y enviarlos a Kafka.

Topic usado:
- ventas-online

Comando:
docker compose exec spark python src/02_generate_streaming_events.py --events 600 --delay 0.1
"""

from pathlib import Path
from datetime import datetime
import argparse
import json
import random
import time

import pandas as pd
from confluent_kafka import Producer


# ============================================================
# 1. CONFIGURACIÓN
# ============================================================

BASE_DIR = Path(__file__).resolve().parents[1]
RAW_DIR = BASE_DIR / "data" / "raw"

KAFKA_TOPIC = "ventas-online"
KAFKA_BOOTSTRAP_SERVERS = "rapidex-broker:9092"

EVENT_TYPES = [
    "venta_creada",
    "venta_confirmada",
    "venta_pagada",
    "venta_entregada",
    "venta_cancelada",
    "venta_fraudulenta"
]

EVENT_WEIGHTS = [
    0.25,
    0.20,
    0.20,
    0.20,
    0.10,
    0.05
]


# ============================================================
# 2. CARGAR DATOS HISTÓRICOS
# ============================================================

def load_reference_data():

    ventas_path = RAW_DIR / "ventas.csv"
    productos_path = RAW_DIR / "productos.csv"
    clientes_path = RAW_DIR / "clientes.json"

    required_files = [
        ventas_path,
        productos_path,
        clientes_path
    ]

    for file_path in required_files:
        if not file_path.exists():
            raise FileNotFoundError(
                f"No se encontró {file_path}"
            )

    ventas_df = pd.read_csv(ventas_path)
    productos_df = pd.read_csv(productos_path)
    clientes_df = pd.read_json(clientes_path)

    producto_categoria_map = dict(
        zip(productos_df["producto_id"], productos_df["categoria"])
    )

    cliente_ciudad_map = dict(
        zip(clientes_df["cliente_id"], clientes_df["ciudad"])
    )

    return {
        "ventas_df": ventas_df,
        "producto_categoria_map": producto_categoria_map,
        "cliente_ciudad_map": cliente_ciudad_map
    }


# ============================================================
# 3. CALLBACK KAFKA
# ============================================================

def delivery_report(err, msg):

    if err is not None:
        print(f"Error enviando mensaje: {err}")


# ============================================================
# 4. CREAR EVENTO STREAMING
# ============================================================

def create_event(event_number, reference_data):

    ventas_df = reference_data["ventas_df"]
    producto_categoria_map = reference_data["producto_categoria_map"]
    cliente_ciudad_map = reference_data["cliente_ciudad_map"]

    venta = ventas_df.sample(1).iloc[0]

    event_type = random.choices(
        EVENT_TYPES,
        weights=EVENT_WEIGHTS,
        k=1
    )[0]

    monto = round(float(venta["total"]), 2)

    # score de fraude
    if monto < 500:
        score_fraude = round(random.uniform(0.05, 0.30), 2)

    elif monto < 3000:
        score_fraude = round(random.uniform(0.25, 0.60), 2)

    else:
        score_fraude = round(random.uniform(0.60, 0.95), 2)

    riesgo_fraude = (
        score_fraude >= 0.80
        or event_type == "venta_fraudulenta"
    )

    producto_id = venta["producto_id"]
    cliente_id = venta["cliente_id"]

    evento = {
    "evento_id": f"EVT-{event_number:06d}",
    "venta_id": venta["venta_id"],
    "producto_id": producto_id,
    "cliente_id": cliente_id,
    "categoria": producto_categoria_map.get(producto_id, "desconocido"),
    "metodo_pago": venta["metodo_pago"],
    "ciudad": cliente_ciudad_map.get(cliente_id, "desconocido"),
    "cantidad": int(venta.get("cantidad", 1)),
    "total": float(monto),
    "riesgo_fraude": float(score_fraude),
    "timestamp_evento": datetime.now().isoformat(timespec="seconds")
}

    return evento


# ============================================================
# 5. MAIN
# ============================================================

def main():

    parser = argparse.ArgumentParser(
        description="Generador de eventos streaming"
    )

    parser.add_argument(
        "--events",
        type=int,
        default=600
    )

    parser.add_argument(
        "--delay",
        type=float,
        default=0.1
    )

    args = parser.parse_args()

    print("=" * 70)
    print("PRODUCTOR KAFKA - SISTEMA COMERCIAL")
    print("=" * 70)

    print(f"Topic: {KAFKA_TOPIC}")
    print(f"Eventos: {args.events}")
    print(f"Delay: {args.delay}")

    print("=" * 70)

    reference_data = load_reference_data()

    producer = Producer({
        "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS
    })

    for event_number in range(1, args.events + 1):

        evento = create_event(
            event_number,
            reference_data
        )

        message_value = json.dumps(
            evento,
            ensure_ascii=False
        )

        producer.produce(
            topic=KAFKA_TOPIC,
            key=evento["venta_id"],
            value=message_value,
            callback=delivery_report
        )

        producer.poll(0)

        if event_number <= 5 or event_number % 100 == 0:
            print(
                f"Evento enviado {event_number}: "
                f"{message_value}"
            )

        time.sleep(args.delay)

    producer.flush()

    print("=" * 70)
    print("Eventos enviados correctamente")
    print("=" * 70)


if __name__ == "__main__":
    main()