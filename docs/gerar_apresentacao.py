"""Gera docs/streaming-kafka-spark.pptx. Uso: .venv/bin/python docs/gerar_apresentacao.py"""
from pathlib import Path

from pptx import Presentation
from pptx.dml.color import RGBColor
from pptx.enum.shapes import MSO_CONNECTOR, MSO_SHAPE
from pptx.enum.text import MSO_ANCHOR, PP_ALIGN
from pptx.util import Inches, Pt

AQUI = Path(__file__).parent
TINTA = RGBColor(0x0B, 0x2B, 0x2E)
TEAL = RGBColor(0x0F, 0x76, 0x6E)
AMBAR = RGBColor(0xD9, 0x77, 0x06)
CLARO = RGBColor(0xE6, 0xF4, 0xF1)
AMBAR_CLARO = RGBColor(0xFD, 0xF0, 0xDC)
CINZA = RGBColor(0x55, 0x65, 0x68)
BRANCO = RGBColor(0xFF, 0xFF, 0xFF)
TITULO = "Cambria"
CORPO = "Calibri"

prs = Presentation()
prs.slide_width, prs.slide_height = Inches(13.333), Inches(7.5)
LAYOUT = prs.slide_layouts[5]  # só título


def texto(slide, x, y, w, h, conteudo, tam=16, cor=TINTA, negrito=False, alinha=PP_ALIGN.LEFT,
          ancora=MSO_ANCHOR.TOP, fonte=CORPO):
    caixa = slide.shapes.add_textbox(Inches(x), Inches(y), Inches(w), Inches(h))
    preencher(caixa.text_frame, conteudo, tam, cor, negrito, alinha, ancora, fonte)
    return caixa


def preencher(tf, conteudo, tam=16, cor=TINTA, negrito=False, alinha=PP_ALIGN.LEFT,
              ancora=MSO_ANCHOR.TOP, fonte=CORPO):
    tf.word_wrap = True
    tf.vertical_anchor = ancora
    tf.margin_left = tf.margin_right = tf.margin_top = tf.margin_bottom = 0
    itens = conteudo if isinstance(conteudo, list) else [conteudo]
    for i, item in enumerate(itens):
        p = tf.paragraphs[0] if i == 0 else tf.add_paragraph()
        p.alignment = alinha
        p.space_after = Pt(6)
        run = p.add_run()
        run.text = item
        run.font.size, run.font.bold, run.font.name = Pt(tam), negrito, fonte
        run.font.color.rgb = cor


def bloco(slide, x, y, w, h, fundo=CLARO, forma=MSO_SHAPE.ROUNDED_RECTANGLE):
    s = slide.shapes.add_shape(forma, Inches(x), Inches(y), Inches(w), Inches(h))
    s.fill.solid()
    s.fill.fore_color.rgb = fundo
    s.line.fill.background()
    s.shadow.inherit = False
    if forma == MSO_SHAPE.ROUNDED_RECTANGLE:
        s.adjustments[0] = min(0.12, 0.12 / min(w, h) * 1.0)
    return s


def cartao(slide, x, y, w, h, titulo, corpo, fundo=CLARO, cor_titulo=TEAL, tam=15):
    bloco(slide, x, y, w, h, fundo)
    texto(slide, x + 0.25, y + 0.2, w - 0.5, 0.45, titulo, 20, cor_titulo, True)
    texto(slide, x + 0.25, y + 0.75, w - 0.5, h - 0.9, corpo, tam)


def seta(slide, x1, y1, x2, y2, cor=CINZA):
    c = slide.shapes.add_connector(MSO_CONNECTOR.STRAIGHT, Inches(x1), Inches(y1), Inches(x2), Inches(y2))
    c.line.color.rgb, c.line.width = cor, Pt(2.25)
    linha = c.line._get_or_add_ln()
    fim = linha.makeelement("{http://schemas.openxmlformats.org/drawingml/2006/main}tailEnd",
                            {"type": "triangle", "w": "med", "len": "med"})
    linha.append(fim)


def novo(titulo, notas, escuro=False):
    s = prs.slides.add_slide(LAYOUT)
    if escuro:
        s.background.fill.solid()
        s.background.fill.fore_color.rgb = TINTA
    t = s.shapes.title
    t.left, t.top, t.width, t.height = Inches(0.6), Inches(0.45), Inches(12.1), Inches(1.15)
    preencher(t.text_frame, titulo, 32, BRANCO if escuro else TINTA, True, fonte=TITULO, ancora=MSO_ANCHOR.MIDDLE)
    s.notes_slide.notes_text_frame.text = notas
    return s


# 1. Capa
s = prs.slides.add_slide(prs.slide_layouts[5])
s.background.fill.solid()
s.background.fill.fore_color.rgb = TINTA
t = s.shapes.title
t.left, t.top, t.width, t.height = Inches(0.8), Inches(2.1), Inches(8.2), Inches(2.0)
preencher(t.text_frame, "Streaming de dados com Kafka e Spark", 46, BRANCO, True, fonte=TITULO)
texto(s, 0.8, 4.3, 8, 0.6, "Da mensagem à métrica, ao vivo", 24, RGBColor(0x9F, 0xE0, 0xD5))
for i, cor in enumerate([TEAL, RGBColor(0x14, 0xA0, 0x8F), AMBAR]):
    bloco(s, 9.9 + i * 0.3, 1.9 + i * 1.15, 2.3, 1.0, cor)
s.notes_slide.notes_text_frame.text = (
    "Abertura. Objetivo: mostrar, com um ambiente rodando de verdade, como os dados saem do sistema de origem, "
    "passam pelo Kafka e viram métrica no Spark. Combine que perguntas são bem-vindas durante a demonstração."
)

# 2. Por quê
s = novo("Streaming reduz o tempo entre o fato e a decisão",
         "Não é para substituir todo batch. Pergunte ao cliente: qual decisão melhora se o dado chegar em segundos? "
         "Se nenhuma melhorar, o batch continua sendo a melhor resposta.")
cartao(s, 0.6, 2.0, 5.9, 3.9, "Processamento em lote",
       ["Os dados se acumulam e são processados em horários definidos",
        "Bom para fechamento, relatórios e grandes volumes históricos",
        "A resposta chega depois do próximo ciclo"], fundo=RGBColor(0xEE, 0xF1, 0xF2), cor_titulo=CINZA, tam=20)
cartao(s, 6.85, 2.0, 5.9, 3.9, "Processamento em streaming",
       ["Cada evento é processado logo depois de acontecer",
        "Bom para monitorar, alertar e reagir durante a operação",
        "A resposta chega em segundos"], tam=20)

# 3. Arquitetura
s = novo("Três peças separam quem produz de quem consome",
         "Ponto-chave: produtor e consumidor não se conhecem. O Kafka fica no meio e guarda as mensagens. "
         "Por isso um pode parar sem derrubar o outro, e é isso que vamos mostrar na demo.")
etapas = [(0.6, "Produtor", "Sistemas publicam pedidos", CLARO, TINTA),
          (3.75, "Kafka", "Tópico pedidos, com 3 partições", TEAL, BRANCO),
          (6.9, "Spark", "Structured Streaming em micro-lotes", TEAL, BRANCO)]
for x, nome, desc, fundo, cor in etapas:
    bloco(s, x, 2.6, 2.6, 1.9, fundo)
    texto(s, x + 0.2, 2.8, 2.2, 0.5, nome, 24, cor, True, PP_ALIGN.CENTER)
    texto(s, x + 0.2, 3.4, 2.2, 0.9, desc, 15, cor, alinha=PP_ALIGN.CENTER)
seta(s, 3.2, 3.55, 3.75, 3.55)
seta(s, 6.35, 3.55, 6.9, 3.55)
for y, nome, desc in [(1.95, "Parquet", "Cada pedido, para consulta"), (3.75, "Janelas", "Totais por região a cada 30 s"),
                      (5.55, "Painel", "Métricas de consumo ao vivo")]:
    bloco(s, 10.05, y, 2.7, 1.45, AMBAR_CLARO)
    texto(s, 10.25, y + 0.15, 2.3, 0.4, nome, 19, AMBAR, True)
    texto(s, 10.25, y + 0.6, 2.3, 0.7, desc, 14)
    seta(s, 9.5, 3.55, 10.05, y + 0.72)

# 4. Kafka
s = novo("O Kafka guarda as mensagens em ordem e cada consumidor sabe onde parou",
         "Explique com o desenho. Cada partição é uma sequência. O offset é a posição. O lag é a distância entre "
         "a última mensagem publicada e a última lida. Se o consumidor parar, as mensagens continuam guardadas.")
lidos = [6, 5, 7]
for lin, n_lido in enumerate(lidos):
    y = 2.1 + lin * 1.0
    texto(s, 0.6, y + 0.13, 1.6, 0.45, f"Partição {lin}", 16, TINTA, True)
    for col in range(9):
        fundo = TEAL if col < n_lido else AMBAR_CLARO
        q = bloco(s, 2.3 + col * 0.72, y, 0.62, 0.72, fundo)
        preencher(q.text_frame, str(col), 16, BRANCO if col < n_lido else AMBAR, True, PP_ALIGN.CENTER, MSO_ANCHOR.MIDDLE)
bloco(s, 2.3, 5.3, 0.4, 0.3, TEAL)
texto(s, 2.85, 5.27, 3, 0.4, "Já lidas pelo consumidor", 15)
bloco(s, 2.3, 5.8, 0.4, 0.3, AMBAR_CLARO)
texto(s, 2.85, 5.77, 3, 0.4, "Lag: ainda a consumir", 15)
cartao(s, 9.1, 2.1, 3.65, 4.2, "Na prática",
       ["Tópico: a fila de um assunto", "Partição: permite paralelismo", "Offset: posição da mensagem",
        "Lag: o que falta consumir"], tam=19)

# 5. Spark
s = novo("O Spark processa em micro-lotes e lembra onde parou",
         "Quatro conceitos que vão aparecer no painel. O watermark costuma gerar perguntas: é a escolha entre esperar "
         "dados atrasados e liberar o resultado mais cedo.")
pares = [("Micro-lote", "A cada intervalo, lê o que chegou e processa como um pequeno lote"),
         ("Checkpoint", "Salva posição e estado; depois de uma falha, retoma do ponto certo"),
         ("Janela", "Agrupa pelo horário em que o evento aconteceu, não pelo de chegada"),
         ("Watermark", "Define até quanto atraso aceitar; mais antigo que isso é descartado")]
for i, (nome, desc) in enumerate(pares):
    cartao(s, 0.6 + (i % 2) * 6.25, 1.9 + (i // 2) * 2.4, 5.9, 2.15, nome, desc, tam=18)

# 6. Roteiro da demo
s = novo("Na demonstração, você comanda cada etapa do fluxo",
         "Mude para o painel em http://localhost:8501. O ambiente já está no ar, mas nada é produzido nem consumido "
         "até você clicar. Siga docs/roteiro.md e pare em cada etapa para perguntas.")
passos = [("1", "Produzir", "Mensagens entram e o lag sobe"), ("2", "Consumir", "O Spark recupera a fila"),
          ("3", "Acelerar", "Pico de pedidos por segundo"), ("4", "Parar", "Nada se perde com o consumo parado"),
          ("5", "Perguntar", "SQL sobre o que foi consumido")]
for i, (n, nome, desc) in enumerate(passos):
    x = 0.6 + i * 2.47
    bloco(s, x, 2.2, 2.25, 3.4, CLARO)
    c = bloco(s, x + 0.72, 2.45, 0.8, 0.8, TEAL, MSO_SHAPE.OVAL)
    preencher(c.text_frame, n, 24, BRANCO, True, PP_ALIGN.CENTER, MSO_ANCHOR.MIDDLE)
    texto(s, x + 0.15, 3.45, 1.95, 0.5, nome, 20, TEAL, True, PP_ALIGN.CENTER)
    texto(s, x + 0.2, 4.0, 1.85, 1.3, desc, 15, alinha=PP_ALIGN.CENTER)

# 7. Painel
s = novo("O lag mostra se o consumo está acompanhando a produção",
         "Imagem real do painel: a fila passou de 6 mil mensagens com o consumo parado. Ao iniciar o Spark, o "
         "processamento ficou bem acima da entrada e o lag caiu para perto de zero. Se a demo ao vivo estiver rodando, "
         "use a tela real em vez desta imagem.")
s.shapes.add_picture(str(AQUI / "img/painel-consumo.jpg"), Inches(0.6), Inches(1.75), height=Inches(5.2))
cartao(s, 8.95, 1.75, 3.8, 5.2, "Como ler",
       ["Processamento acima da entrada: a fila diminui",
        "Processamento abaixo da entrada: a fila cresce",
        "Lag estável perto de zero: consumo saudável"], fundo=AMBAR_CLARO, cor_titulo=AMBAR, tam=20)

# 8. Métricas
s = novo("Quatro métricas respondem se o consumo está saudável",
         "Use a linguagem do negócio: o lag vira 'quantos segundos de atraso temos agora'. Pergunte quais destes "
         "limites fariam sentido na operação do cliente.")
linhas = [("Métrica", "Pergunta que responde", "Sinal de alerta"),
          ("Lag", "Quanto falta consumir?", "Cresce sem parar"),
          ("Entrada e processamento", "O consumidor dá conta do volume?", "Processamento abaixo da entrada"),
          ("Duração do lote", "Cabe no intervalo definido?", "Maior que o intervalo do lote"),
          ("Eventos descartados", "Estamos perdendo dados atrasados?", "Contador subindo com frequência")]
tb = s.shapes.add_table(len(linhas), 3, Inches(0.6), Inches(1.95), Inches(12.15), Inches(4.2)).table
tb.columns[0].width, tb.columns[1].width, tb.columns[2].width = Inches(3.65), Inches(4.5), Inches(4.0)
for i, linha in enumerate(linhas):
    for j, valor in enumerate(linha):
        c = tb.cell(i, j)
        c.fill.solid()
        c.fill.fore_color.rgb = TEAL if i == 0 else (CLARO if i % 2 else BRANCO)
        c.vertical_anchor = MSO_ANCHOR.MIDDLE
        c.margin_left = Inches(0.15)
        preencher(c.text_frame, valor, 18, BRANCO if i == 0 else TINTA, i == 0 or j == 0)
        c.margin_left = Inches(0.15)

# 9. Confiabilidade
s = novo("Derrubamos o consumidor e nenhuma mensagem se perdeu",
         "Resultado do teste feito neste ambiente local: o container do Spark foi encerrado à força com pedidos "
         "entrando, depois o consumo foi parado e retomado. Ao final, as contagens bateram. É um teste de demonstração, "
         "não um teste de carga.")
for i, (num, rot) in enumerate([("9.800", "mensagens publicadas no Kafka"), ("9.800", "pedidos distintos gravados"),
                                ("0", "duplicadas ou perdidas")]):
    x = 0.6 + i * 4.15
    bloco(s, x, 2.0, 3.85, 2.9, CLARO if i < 2 else AMBAR_CLARO)
    texto(s, x + 0.2, 2.3, 3.45, 1.3, num, 66, TEAL if i < 2 else AMBAR, True, PP_ALIGN.CENTER, fonte=TITULO)
    texto(s, x + 0.3, 3.8, 3.25, 0.8, rot, 17, alinha=PP_ALIGN.CENTER)
texto(s, 0.6, 5.3, 12.1, 1.0, "O Kafka retém as mensagens, o checkpoint guarda a posição e a gravação em Parquet evita "
      "duplicar o que já foi escrito. Teste feito neste ambiente de demonstração.", 18)

# 10. Produção
s = novo("Para produção, o desenho é o mesmo e o rigor aumenta",
         "Seja transparente: esta é uma demo local. Use este slide para abrir a conversa sobre o ambiente do cliente. "
         "Kafka e Spark podem ser gerenciados por serviços de nuvem.")
linhas = [("", "Nesta demo", "Em produção"),
          ("Kafka", "1 servidor", "Vários servidores, com réplicas"),
          ("Segurança", "Sem autenticação", "TLS, autenticação e permissão por tópico"),
          ("Formato", "JSON", "Contrato de dados e controle de versões"),
          ("Monitoramento", "Painel da demo", "Alertas de lag, falhas e custo")]
tb = s.shapes.add_table(len(linhas), 3, Inches(0.6), Inches(1.95), Inches(12.15), Inches(4.2)).table
tb.columns[0].width, tb.columns[1].width, tb.columns[2].width = Inches(3.0), Inches(3.75), Inches(5.4)
for i, linha in enumerate(linhas):
    for j, valor in enumerate(linha):
        c = tb.cell(i, j)
        c.fill.solid()
        c.fill.fore_color.rgb = TEAL if i == 0 else (CLARO if i % 2 else BRANCO)
        c.vertical_anchor = MSO_ANCHOR.MIDDLE
        preencher(c.text_frame, valor, 18, BRANCO if i == 0 else TINTA, i == 0 or j == 0)
        c.margin_left = Inches(0.15)

# 11. Fechamento
s = novo("Vamos aplicar isso aos seus dados",
         "Feche com a conversa. A melhor saída desta reunião é um caso de uso concreto e um dono para o próximo passo.",
         escuro=True)
cartao(s, 0.6, 2.0, 5.9, 3.6, "Perguntas para discutir",
       ["Qual decisão melhora com dados em segundos?", "Quais sistemas geram esses eventos?",
        "Quanto atraso é aceitável?"], tam=20)
cartao(s, 6.85, 2.0, 5.9, 3.6, "Próximos passos",
       ["Escolher um caso de uso", "Mapear origens e volumes", "Construir uma prova de conceito com dados de vocês"],
       fundo=AMBAR_CLARO, cor_titulo=AMBAR, tam=20)

prs.save(AQUI / "streaming-kafka-spark.pptx")
print("ok", len(prs.slides._sldIdLst), "slides")
