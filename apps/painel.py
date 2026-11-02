"""Painel da demo: controla produção e consumo e mostra as métricas ao vivo."""
import json
import threading
import time
from collections import deque
from pathlib import Path

import altair as alt
import duckdb
import pandas as pd
import streamlit as st
from kafka import KafkaConsumer, TopicPartition

import estado

BRONZE = "/dados/bronze/*.parquet"
ATUALIZA = "2s"

PERGUNTAS = {
    "Quantos pedidos e qual a receita até agora?":
        "SELECT count(*) AS pedidos, round(sum(valor), 2) AS receita FROM pedidos",
    "Qual região vende mais?":
        "SELECT regiao, count(*) AS pedidos, round(sum(valor), 2) AS receita\n"
        "FROM pedidos GROUP BY regiao ORDER BY receita DESC",
    "Qual produto vende mais?":
        "SELECT produto, sum(quantidade) AS unidades, round(sum(valor), 2) AS receita\n"
        "FROM pedidos GROUP BY produto ORDER BY receita DESC",
    "Qual a taxa de cancelamento por região?":
        "SELECT regiao, round(100.0 * count(*) FILTER (status = 'cancelado') / count(*), 1) AS pct_cancelado\n"
        "FROM pedidos GROUP BY regiao ORDER BY pct_cancelado DESC",
    "Quanto tempo o pedido leva do evento até o Kafka?":
        "SELECT round(avg(date_diff('millisecond', evento_ts, kafka_ts)) / 1000.0, 3) AS atraso_medio_s\n"
        "FROM pedidos",
    "Nenhuma mensagem foi duplicada?":
        "SELECT count(*) AS linhas, count(DISTINCT pedido_id) AS pedidos_distintos FROM pedidos",
}

st.set_page_config(page_title="Streaming com Kafka e Spark", page_icon="📡", layout="wide")


class Amostrador(threading.Thread):
    """Consulta o Kafka em segundo plano e guarda o histórico de produção e lag."""

    def __init__(self):
        super().__init__(daemon=True)
        self.fim = {}
        self.lidos = {}
        self.conectado = False
        self.historico = deque(maxlen=300)

    def run(self):
        consumidor = None
        while True:
            try:
                consumidor = consumidor or KafkaConsumer(bootstrap_servers=estado.KAFKA)
                ids = consumidor.partitions_for_topic(estado.TOPICO) or set()
                pts = [TopicPartition(estado.TOPICO, p) for p in sorted(ids)]
                # Lê o Spark antes do Kafka para o lag nunca sair negativo.
                self.lidos = offsets_lidos(ler_metricas("bronze"))
                self.fim = {tp.partition: o for tp, o in consumidor.end_offsets(pts).items()}
                self.conectado = bool(pts)
                produzidas = sum(self.fim.values())
                consumidas = sum(self.lidos.values())
                self.historico.append({
                    "hora": pd.Timestamp.now(tz="UTC"),
                    "produzidas": produzidas,
                    "consumidas": consumidas,
                    "lag": max(produzidas - consumidas, 0),
                })
            except Exception:
                self.conectado = False
                consumidor = None
            time.sleep(2)


def ler_metricas(consulta, limite=400):
    """Lê o progresso publicado pelo Spark, um registro por micro-lote."""
    try:
        linhas = Path(estado.ARQ_METRICAS).read_text().splitlines()[-limite * 2:]
    except FileNotFoundError:
        return []
    registros = []
    for linha in linhas:
        try:
            registro = json.loads(linha)
        except json.JSONDecodeError:
            continue
        if registro.get("name") == consulta:
            registros.append(registro)
    return registros[-limite:]


def offsets_lidos(metricas):
    """Posição de leitura do Spark, por partição, segundo o último micro-lote."""
    if not metricas:
        return {}
    fim = metricas[-1]["sources"][0]["endOffset"]
    fim = json.loads(fim) if isinstance(fim, str) else fim
    return {int(p): o for p, o in fim.get(estado.TOPICO, {}).items()}


@st.cache_resource
def amostrador():
    a = Amostrador()
    a.start()
    return a


def ultimas_mensagens(fim, quantidade=10):
    consumidor = KafkaConsumer(
        bootstrap_servers=estado.KAFKA, consumer_timeout_ms=800, enable_auto_commit=False,
    )
    try:
        pts = [TopicPartition(estado.TOPICO, p) for p in fim]
        consumidor.assign(pts)
        for tp in pts:
            consumidor.seek(tp, max(fim[tp.partition] - quantidade, 0))
        linhas = []
        for m in consumidor:
            linhas.append({"partição": m.partition, "offset": m.offset, **json.loads(m.value)})
            if len(linhas) >= quantidade * len(pts):
                break
    finally:
        consumidor.close()
    linhas.sort(key=lambda l: l["evento_ts"], reverse=True)
    return pd.DataFrame(linhas[:quantidade])


def consultar(sql):
    con = duckdb.connect()
    con.execute(f"CREATE VIEW pedidos AS SELECT * FROM read_parquet('{BRONZE}')")
    return con.execute(sql).df()


def controles():
    atual = estado.ler()
    st.sidebar.title("Controles da demo")

    st.sidebar.subheader("1. Produtor")
    st.sidebar.caption("Simula os sistemas que publicam pedidos no Kafka.")
    if st.sidebar.button("⏸ Pausar produção" if atual["produzindo"] else "▶ Iniciar produção",
                         use_container_width=True):
        estado.gravar(produzindo=not atual["produzindo"])
        st.rerun()
    taxa = st.sidebar.slider("Pedidos por segundo", 5, 500, atual["taxa"], step=5)
    atrasados = st.sidebar.slider("% de eventos atrasados (5 min)", 0, 30, atual["atrasados_pct"],
                                  help="Eventos antigos caem fora do watermark de 1 minuto.")

    st.sidebar.subheader("2. Consumidor Spark")
    st.sidebar.caption("Parar não perde dados: o checkpoint guarda a posição de leitura.")
    if st.sidebar.button("⏸ Parar consumo" if atual["consumindo"] else "▶ Iniciar consumo",
                         use_container_width=True, type="primary"):
        estado.gravar(consumindo=not atual["consumindo"])
        st.rerun()
    trigger = st.sidebar.select_slider("Intervalo do micro-lote (s)", [1, 2, 5, 10, 15, 30], atual["trigger_s"])
    maximo = st.sidebar.select_slider("Máximo de mensagens por lote", [100, 250, 500, 1000, 2500, 5000, 10000],
                                      atual["max_por_lote"],
                                      help="Limita o ritmo para o consumo do acúmulo ficar visível.")

    mudancas = {"taxa": taxa, "atrasados_pct": atrasados, "trigger_s": trigger, "max_por_lote": maximo}
    if any(atual[k] != v for k, v in mudancas.items()):
        estado.gravar(**mudancas)

    st.sidebar.divider()
    st.sidebar.markdown(
        "[Spark Master](http://localhost:9091) · [Streaming no Spark](http://localhost:4040/StreamingQuery/) · "
        "[History Server](http://localhost:18081)"
    )


@st.fragment(run_every=ATUALIZA)
def consumo_ao_vivo():
    a = amostrador()
    if not a.conectado:
        st.warning("Aguardando o Kafka e o tópico ficarem prontos...")
        return

    atual = estado.ler()
    metricas = ler_metricas("bronze")
    ultimo = metricas[-1] if metricas else {}
    lidos = a.lidos
    produzidas, consumidas = sum(a.fim.values()), sum(lidos.values())
    lag = max(produzidas - consumidas, 0)

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Produtor", "Ativo" if atual["produzindo"] else "Parado",
              f"{atual['taxa']} pedidos/s" if atual["produzindo"] else None)
    c2.metric("Consumidor", "Ativo" if atual["consumindo"] else "Parado",
              f"lote a cada {atual['trigger_s']}s" if atual["consumindo"] else None)
    c3.metric("No Kafka", f"{produzidas:,}".replace(",", "."))
    c4.metric("Consumidas", f"{consumidas:,}".replace(",", "."))

    c5, c6, c7, c8 = st.columns(4)
    c5.metric("Lag", f"{lag:,}".replace(",", "."),
              help="Mensagens no Kafka que o Spark ainda não leu.")
    c6.metric("Entrada", f"{ultimo.get('inputRowsPerSecond', 0):.0f}/s",
              help="Mensagens por segundo que chegaram no último lote.")
    c7.metric("Processamento", f"{ultimo.get('processedRowsPerSecond', 0):.0f}/s",
              help="Mensagens por segundo que o Spark processou no último lote. Se ficar abaixo da entrada, o lag cresce.")
    duracao = ultimo.get("durationMs", {}).get("triggerExecution", 0) / 1000
    c8.metric("Duração do lote", f"{duracao:.2f} s", help=f"Lote número {ultimo.get('batchId', '-')}.")

    historico = pd.DataFrame(a.historico)
    esquerda, direita = st.columns(2)
    with esquerda:
        st.subheader("Lag ao longo do tempo")
        st.caption("Sobe quando o produtor é mais rápido que o consumidor; desce quando o Spark recupera a fila.")
        if not historico.empty:
            st.altair_chart(
                alt.Chart(historico).mark_area(line=True, opacity=0.35, color="#0f766e").encode(
                    x=alt.X("hora:T", title=None), y=alt.Y("lag:Q", title="mensagens")
                ).properties(height=240),
                use_container_width=True,
            )
    with direita:
        st.subheader("Entrada e processamento por lote")
        st.caption("Cada ponto é um micro-lote do Spark.")
        lotes = pd.DataFrame([{
            "lote": m["batchId"], "Entrada/s": m["inputRowsPerSecond"], "Processamento/s": m["processedRowsPerSecond"],
        } for m in metricas[-60:]])
        if not lotes.empty:
            longo = lotes.melt("lote", var_name="medida", value_name="mensagens/s")
            st.altair_chart(
                alt.Chart(longo).mark_line(point=True).encode(
                    x=alt.X("lote:Q", title="micro-lote", axis=alt.Axis(tickMinStep=1)),
                    y=alt.Y("mensagens/s:Q"),
                    color=alt.Color("medida:N", title=None,
                                    scale=alt.Scale(range=["#d97706", "#0f766e"]),
                                    legend=alt.Legend(orient="top")),
                ).properties(height=240),
                use_container_width=True,
            )
        else:
            st.info("Inicie o consumo para ver os micro-lotes.")

    st.subheader("Posição por partição")
    st.caption("O tópico é dividido em partições; o offset é a posição de cada mensagem. O lag é a diferença.")
    st.dataframe(pd.DataFrame([{
        "Partição": p, "Último offset no Kafka": fim, "Lido pelo Spark": lidos.get(p, 0),
        "Lag": max(fim - lidos.get(p, 0), 0),
    } for p, fim in sorted(a.fim.items())]), hide_index=True, use_container_width=True)

    if ultimo:
        with st.expander("Progresso bruto do último micro-lote (como o Spark reporta)"):
            st.json(ultimo)


@st.fragment(run_every=ATUALIZA)
def mensagens():
    a = amostrador()
    st.subheader("Últimas mensagens no tópico")
    st.caption("Lidas direto do Kafka: cada linha é um pedido publicado pelo produtor.")
    if not sum(a.fim.values()):
        st.info("Inicie a produção para ver as mensagens.")
        return
    st.dataframe(ultimas_mensagens(a.fim), hide_index=True, use_container_width=True)


@st.fragment(run_every=ATUALIZA)
def janelas():
    st.subheader("Pedidos por região em janelas de 30 segundos")
    st.caption("Agregação calculada pelo Spark sobre o horário do evento, não o de chegada.")
    try:
        dados = pd.DataFrame(json.loads(Path(estado.ARQ_JANELAS).read_text()))
    except (FileNotFoundError, json.JSONDecodeError):
        dados = pd.DataFrame()
    if not dados.empty:
        dados["inicio"] = pd.to_datetime(dados["inicio"])
        st.altair_chart(
            alt.Chart(dados).mark_bar().encode(
                x=alt.X("inicio:T", title="início da janela"),
                y=alt.Y("pedidos:Q", title="pedidos"),
                color=alt.Color("regiao:N", title="Região"),
            ).properties(height=320),
            use_container_width=True,
        )

    metricas = ler_metricas("janelas")
    if metricas:
        ultimo = metricas[-1]
        operador = (ultimo.get("stateOperators") or [{}])[0]
        descartados = sum((m.get("stateOperators") or [{}])[0].get("numRowsDroppedByWatermark", 0) for m in metricas)
        c1, c2, c3 = st.columns(3)
        c1.metric("Watermark", (ultimo.get("eventTime", {}).get("watermark") or "-")[11:19] + " UTC",
                  help="Eventos mais antigos que este horário são descartados.")
        c2.metric("Janelas em memória", operador.get("numRowsTotal", 0))
        c3.metric("Eventos atrasados descartados", descartados,
                  help="Soma dos lotes recentes. Use o controle de eventos atrasados para ver subir.")
    elif dados.empty:
        st.info("Inicie o consumo para ver as janelas.")


def perguntas():
    st.subheader("Perguntas sobre os dados consumidos")
    st.caption("Consulta os Parquet gravados pelo Spark. Escolha uma pergunta ou edite o SQL durante a conversa.")
    pergunta = st.selectbox("Pergunta", list(PERGUNTAS))
    sql = st.text_area("SQL (tabela: pedidos)", PERGUNTAS[pergunta], height=110, key=f"sql-{pergunta}")
    if st.button("Consultar"):
        try:
            st.dataframe(consultar(sql), hide_index=True, use_container_width=True)
        except duckdb.Error as erro:
            if "No files found" in str(erro):
                st.info("Ainda não há dados: inicie a produção e o consumo.")
            else:
                st.error(f"Erro na consulta: {erro}")


def entenda():
    st.subheader("Entenda o que está na tela")
    st.markdown("""
| Termo | O que significa |
|---|---|
| **Tópico** | Fila de mensagens do Kafka; aqui, `pedidos`. |
| **Partição** | Divisão do tópico que permite ler e gravar em paralelo. |
| **Offset** | Posição de uma mensagem na partição. |
| **Lag** | Mensagens publicadas que ainda não foram lidas. |
| **Micro-lote** | Pequeno lote de mensagens que o Spark processa a cada intervalo. |
| **Checkpoint** | Registro da posição e do estado; permite retomar sem perder dados. |
| **Janela** | Agrupamento por intervalo de tempo do evento. |
| **Watermark** | Limite de atraso aceito; eventos mais antigos são descartados. |
""")


st.title("📡 Streaming de dados com Kafka e Spark")
controles()
aba1, aba2, aba3, aba4, aba5 = st.tabs(["Consumo ao vivo", "Mensagens", "Janelas", "Perguntas", "Entenda"])
with aba1:
    consumo_ao_vivo()
with aba2:
    mensagens()
with aba3:
    janelas()
with aba4:
    perguntas()
with aba5:
    entenda()
