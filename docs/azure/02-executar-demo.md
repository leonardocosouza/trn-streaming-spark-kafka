# Executar a demo no Azure

Este guia conduz a apresentação da demo de streaming com Event Hubs e Azure Databricks. O ambiente precisa estar pronto, como descrito em [01-criar-e-excluir-ambiente.md](01-criar-e-excluir-ambiente.md).

**Duração sugerida:** 40 minutos: 15 de slides, 20 de demonstração e 5 de fechamento.

## O que o público vai ver

| Peça | Papel | Onde aparece |
|---|---|---|
| Produtor | Publica pedidos fictícios | Notebook `01_produtor` |
| Event Hubs | Guarda as mensagens, com protocolo Kafka | Portal do Azure, se quiser mostrar |
| Spark Structured Streaming | Consome em micro-lotes | Notebook `02_consumidor` |
| Delta | Guarda pedidos e janelas | Notebook `04_perguntas` |
| Painel | Mostra produção, consumo e lag | Notebook `03_painel` |

## Antes da reunião

**Uma hora antes**
- [ ] Se o namespace foi apagado, recrie o Event Hubs e atualize o segredo.
- [ ] Ligue o cluster `streaming-demo`.
- [ ] Se houve ensaio, recrie o hub `pedidos` e execute `05_resetar`.

**Dez minutos antes**
- [ ] Abra cada notebook em uma aba, nesta ordem: `03_painel`, `01_produtor`, `02_consumidor` e `04_perguntas`.
- [ ] Ligue todos ao cluster `streaming-demo`.
- [ ] Em `01_produtor`, confira a taxa em `50` e os atrasados em `0`.
- [ ] Em `02_consumidor`, confira o intervalo em `5` e o máximo por lote em `1000`.
- [ ] Execute `03_painel` e confira produzidas, consumidas e lag em zero.
- [ ] Abra os slides `docs/streaming-kafka-spark.pptx`.

## Como controlar a demo

| Quero | Faça |
|---|---|
| Iniciar o produtor ou o consumidor | Clique em **Run all** no notebook |
| Parar | Clique em **Interrupt** no notebook |
| Mudar taxa, atrasados, intervalo ou limite | Interrompa, altere o campo no topo e execute a última célula de novo |

O painel continua atualizando sozinho, sem depender dos outros notebooks.

## Sequência da apresentação

| Tempo | O que fazer |
|---|---|
| 15 min | Slides 1 a 5: por que usar streaming, as peças e os conceitos |
| 20 min | Slide 6 e demonstração |
| 5 min | Slides 9 a 11: resultado, produção e próximos passos |

Os slides 7 e 8 explicam o que o painel já mostrou. Pule os dois se o tempo apertar.

Os slides foram feitos para a demo local. Ao chegar neles, explique que o Event Hubs ocupa o lugar do Kafka e usa o mesmo protocolo. Os 9.800 do slide 9 são de um teste da versão local, então cite os números da sua tela.

## Roteiro da demonstração

### 1. Produzir sem consumir

1. Em `01_produtor`, clique em **Run all**.
2. Volte para `03_painel`.

**O que mostrar:** produzidas e lag sobem, e consumidas fica em zero.

**O que dizer:** "O Event Hubs recebe e guarda as mensagens, mesmo sem ninguém consumindo. Produtor e consumidor não se conhecem."

### 2. Consumir

1. Em `02_consumidor`, clique em **Run all**.
2. Volte para `03_painel`.

**O que mostrar:** lote a lote, o processamento fica acima da entrada e o lag cai.

**O que dizer:** "O limite por lote é proposital. Ele nos permite ver o Spark alcançando a produção."

### 3. Acelerar

1. Em `01_produtor`, interrompa, mude a taxa para `500` e execute a última célula.
2. Em `02_consumidor`, interrompa, mude o máximo por lote para `500` e execute a última célula.
3. Mostre o lag crescendo em `03_painel`.
4. Em `02_consumidor`, interrompa, mude o máximo para `5000` e execute a última célula.

**O que mostrar:** o lag sobe com o limite baixo e cai depois do ajuste.

**O que dizer:** "O gráfico mostra a falta de capacidade antes de virar problema para o negócio."

Depois, volte a taxa para `50`.

### 4. Parar e derrubar o consumidor

1. Em `02_consumidor`, clique em **Interrupt**.
2. Espere 20 segundos em `03_painel`. O lag sobe.
3. Clique em **Run all** em `02_consumidor`. O lag cai.

Para mostrar uma falha real:
1. Com o produtor ligado, abra o menu do cluster em `02_consumidor` e escolha **Detach & re-attach**.
2. Clique em **Run all** de novo.

**O que dizer:** "As mensagens seguiram no Event Hubs, e o checkpoint retomou do ponto certo."

Em `04_perguntas`, rode **Nenhuma mensagem foi duplicada?**. Linhas e pedidos distintos devem ser iguais.

### 5. Eventos atrasados

1. Em `01_produtor`, interrompa, mude os atrasados para `10` e execute a última célula.
2. Em `03_painel`, acompanhe **Eventos atrasados descartados**.
3. Em `04_perguntas`, rode a consulta de janelas.

**O que dizer:** "A janela usa o horário do evento, não o de chegada. O watermark define quanto atraso aceitamos."

Depois, volte os atrasados para `0`.

### 6. Perguntar aos dados

Em `04_perguntas`, rode as consultas prontas e edite o SQL com as dúvidas do cliente.

**O que dizer:** "O dado já chega consultável."

### 7. Conferir o resultado

1. Interrompa `01_produtor`.
2. Espere o lag zerar em `03_painel`.
3. Em `04_perguntas`, execute a seção **Conferência final**.

**O que mostrar:** os três números iguais: no tópico, na tabela e distintos.

## Encerrar

1. Interrompa `02_consumidor` e `03_painel`.
2. Termine o cluster em **Compute**.
3. Apague o namespace, como descrito na Parte 3 de [01-criar-e-excluir-ambiente.md](01-criar-e-excluir-ambiente.md).

## Perguntas frequentes

| Pergunta | Resposta |
|---|---|
| Isso é Kafka? | O Event Hubs fala o protocolo Kafka. O Spark conecta como a um Kafka, sem cluster para administrar |
| Qual o atraso de ponta a ponta? | Depende do intervalo do lote. Aqui, poucos segundos |
| E se o Spark cair? | As mensagens seguem no Event Hubs, dentro da retenção, e o checkpoint retoma do ponto certo |
| Duplica dados? | A gravação em Delta com checkpoint é exactly-once. A consulta da demo confirma |
| Dá para escalar? | Sim, com mais unidades e partições no Event Hubs e um cluster maior |
| Os dados são reais? | Não. São pedidos fictícios, gerados pelo produtor |
| Serve para produção? | O desenho sim, a demo não. O slide 10 mostra o que muda |

## Se algo falhar

| Sintoma | O que fazer |
|---|---|
| Notebook sem cluster | Ligue `streaming-demo` e conecte o notebook |
| Painel com erro de tabela ou arquivo | Execute-o de novo depois de iniciar o consumo |
| Erro de autenticação no Kafka | A chave mudou. Atualize o segredo e execute o notebook de novo |
| Erro de tempo esgotado no Kafka | Confira se o namespace existe e se a porta `9093` está liberada |
| Consumidor sem novos lotes | Confira se a célula final está rodando e se o produtor está ligado |
| Números do ensaio aparecem na tela | Recrie o hub e execute `05_resetar` |
| Nada resolve | Use o slide 7, que tem uma imagem real do painel da versão local |
