# Roteiro da apresentação

**Duração sugerida:** 40 minutos: 15 de slides, 20 de demonstração e 5 de fechamento.
**Apresentação:** [streaming-kafka-spark.pptx](streaming-kafka-spark.pptx), com notas do apresentador em cada slide.

## Antes da reunião
- [ ] Docker Desktop aberto, com pelo menos 8 GB de memória
- [ ] `make reset` para zerar os dados (faça com internet na primeira vez, para baixar as imagens)
- [ ] Abrir http://localhost:8501 e confirmar Produtor e Consumidor como "Parado"
- [ ] Abrir http://localhost:4040/StreamingQuery/ em outra aba (a página fica vazia até o consumo iniciar)

## Demonstração

| Etapa | No painel | O que mostrar | Mensagem |
|---|---|---|---|
| 1. Produzir | Em Produtor, clique em **Iniciar produção** | "No Kafka" e "Lag" sobem; "Consumidas" fica em 0. Abra a aba **Mensagens** | O Kafka recebe e guarda, mesmo sem ninguém consumindo |
| 2. Consumir | Clique em **Iniciar consumo**, com máximo de 1000 por lote | Lote a lote, o processamento fica acima da entrada e o lag cai | O consumidor pega o acúmulo e alcança a produção |
| 3. Acelerar | Suba para 500 pedidos/s e reduza o máximo por lote para 500 | O lag volta a crescer | O gráfico mostra a falta de capacidade antes de virar problema. Depois, suba o máximo para 5000 |
| 4. Parar | Clique em **Parar consumo**, espere 20 s e inicie de novo | O lag sobe e depois cai. Na aba **Perguntas**, rode "Nenhuma mensagem foi duplicada?" | O checkpoint retoma do ponto certo |
| 5. Atraso | Coloque 10% de eventos atrasados e abra a aba **Janelas** | "Eventos atrasados descartados" sobe | O watermark define quanto atraso aceitar |
| 6. Perguntar | Abra a aba **Perguntas** e edite o SQL com as dúvidas do cliente | Resposta sobre os dados consumidos | O dado já chega consultável |

Para mostrar uma falha real na etapa 4, rode em um terminal:

```bash
docker restart streaming-spark-streaming
```

O consumo volta sozinho em alguns segundos, porque o painel mantém a ordem de consumir.

## Perguntas frequentes

| Pergunta | Resposta |
|---|---|
| Qual o atraso de ponta a ponta? | Depende do intervalo do lote. Aqui é de poucos segundos; o intervalo é ajustável no painel |
| E se o Spark cair? | As mensagens seguem no Kafka e o checkpoint retoma do ponto certo |
| Duplica dados? | Nesta demo, a gravação em Parquet é exactly-once; a pergunta pronta do painel confirma |
| Dá para escalar? | Sim: mais partições no Kafka e mais executores no Spark |
| Os dados são reais? | Não. São pedidos fictícios, gerados pelo produtor |
| Serve para produção? | O desenho sim; a demo não. O slide 10 lista o que muda |

## Se algo falhar
- **Painel fora do ar:** rode `make status` e depois `make demo`.
- **Dados de um ensaio anterior:** rode `make reset`.
- **Plano B:** use o slide 7, que tem uma imagem real do painel.
