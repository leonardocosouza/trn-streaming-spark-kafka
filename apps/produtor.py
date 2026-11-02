"""Gera pedidos fictícios no tópico do Kafka, no ritmo definido pelo painel."""
import json
import random
import time
import uuid
from datetime import datetime, timedelta, timezone

from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

import estado

REGIOES = {"Sudeste": 45, "Sul": 20, "Nordeste": 18, "Centro-Oeste": 10, "Norte": 7}
PRODUTOS = {"Notebook": 3500, "Celular": 2200, "Fone": 250, "Monitor": 1200, "Teclado": 180}
TICK_S = 0.2


def criar_produtor():
    while True:
        try:
            return KafkaProducer(
                bootstrap_servers=estado.KAFKA,
                value_serializer=lambda v: json.dumps(v).encode(),
                key_serializer=str.encode,
            )
        except NoBrokersAvailable:
            print("Aguardando o Kafka...", flush=True)
            time.sleep(2)


def novo_pedido(atrasados_pct):
    momento = datetime.now(timezone.utc)
    if random.uniform(0, 100) < atrasados_pct:
        momento -= timedelta(minutes=5)
    produto = random.choice(list(PRODUTOS))
    quantidade = random.randint(1, 3)
    return {
        "pedido_id": str(uuid.uuid4()),
        "evento_ts": momento.isoformat(),
        "regiao": random.choices(list(REGIOES), weights=REGIOES.values())[0],
        "produto": produto,
        "quantidade": quantidade,
        "valor": round(PRODUTOS[produto] * quantidade * random.uniform(0.9, 1.1), 2),
        "status": random.choices(["aprovado", "cancelado"], weights=[95, 5])[0],
    }


def main():
    produtor = criar_produtor()
    print("Produtor pronto; aguardando o comando do painel.", flush=True)
    while True:
        atual = estado.ler()
        if atual["produzindo"]:
            for _ in range(max(1, round(atual["taxa"] * TICK_S))):
                pedido = novo_pedido(atual["atrasados_pct"])
                produtor.send(estado.TOPICO, key=pedido["pedido_id"], value=pedido)
            produtor.flush()
        time.sleep(TICK_S)


if __name__ == "__main__":
    main()
