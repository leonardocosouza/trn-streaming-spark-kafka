# Criar e excluir o ambiente no Azure

Este guia monta a infraestrutura da demo de streaming com Event Hubs e Azure Databricks, e explica como desligá-la e excluí-la. Para apresentar, use [02-executar-demo.md](02-executar-demo.md).

**Tempo estimado:** 40 minutos na primeira vez.

## O que será criado

| Recurso | Nome | Onde fica |
|---|---|---|
| Namespace do Event Hubs | `evh-streaming-demo` | `rsg-streaming-demo` |
| Hub, equivalente ao tópico Kafka | `pedidos`, com 3 partições | Namespace |
| Secret scope | `streaming-demo` | Workspace |
| Catálogo, schema e Volumes | `streaming_demo.pedidos`, com `checkpoints` e `controle` | Unity Catalog |
| Cluster | `streaming-demo` | Workspace |
| Notebooks | Pasta `demo-streaming` | Workspace |

O resource group, o workspace, o storage, o Access Connector e a rede já existem.

Os nomes deste guia são exemplos. Troque-os pelos do seu ambiente antes de rodar os comandos, inclusive `NOME_DA_SUA_ASSINATURA`. Os nomes do namespace e do storage precisam ser únicos no Azure.

## Parte 1: criar

### 1. Preparar o Cloud Shell

No portal, abra o **Cloud Shell** em modo **PowerShell** e selecione a assinatura:

```powershell
az account set --subscription "NOME_DA_SUA_ASSINATURA"
```

Guarde a URL do workspace em uma variável:

```powershell
$workspace = az resource show --resource-group rsg-streaming-demo --name dbw-streaming-demo --resource-type Microsoft.Databricks/workspaces --query properties.workspaceUrl -o tsv
```

Confira o plano do workspace. O resultado precisa ser `premium`:

```powershell
az resource show --resource-group rsg-streaming-demo --name dbw-streaming-demo --resource-type Microsoft.Databricks/workspaces --query sku.name -o tsv
```

### 2. Criar o Event Hubs

```powershell
az provider register --namespace Microsoft.EventHub
```

```powershell
az eventhubs namespace create --resource-group rsg-streaming-demo --name evh-streaming-demo --location eastus --sku Standard --capacity 1 --tags owner="seu-nome"
```

```powershell
az eventhubs eventhub create --resource-group rsg-streaming-demo --namespace-name evh-streaming-demo --name pedidos --partition-count 3 --cleanup-policy Delete --retention-time-in-hours 24
```

A regra de acesso fica no namespace, para que a chave continue válida quando o hub for recriado:

```powershell
az eventhubs namespace authorization-rule create --resource-group rsg-streaming-demo --namespace-name evh-streaming-demo --name demo --rights Listen Send
```

### 3. Criar o secret scope

O Cloud Shell precisa de um login que acesse o Databricks. Entre com a sua conta de parceiro:

```powershell
az logout
```

```powershell
az login --use-device-code --scope "2ff814a6-3304-4ab8-85cb-cd0e6f879c1d/.default"
```

```powershell
az account set --subscription "NOME_DA_SUA_ASSINATURA"
```

Crie o scope:

```powershell
'{"scope":"streaming-demo"}' | Set-Content scope.json; az rest --method post --url "https://$workspace/api/2.0/secrets/scopes/create" --resource 2ff814a6-3304-4ab8-85cb-cd0e6f879c1d --body "@scope.json"; Remove-Item scope.json
```

Guarde a connection string:

```powershell
$c = az eventhubs namespace authorization-rule keys list --resource-group rsg-streaming-demo --namespace-name evh-streaming-demo --name demo --query primaryConnectionString -o tsv; @{scope="streaming-demo";key="eventhub-connection";string_value=$c} | ConvertTo-Json -Compress | Set-Content segredo.json; az rest --method post --url "https://$workspace/api/2.0/secrets/put" --resource 2ff814a6-3304-4ab8-85cb-cd0e6f879c1d --body "@segredo.json"; Remove-Item segredo.json; Remove-Variable c
```

Confira. O resultado deve listar `eventhub-connection`, sem mostrar a chave:

```powershell
az rest --method get --url "https://$workspace/api/2.0/secrets/list?scope=streaming-demo" --resource 2ff814a6-3304-4ab8-85cb-cd0e6f879c1d
```

Se esses comandos continuarem falhando no Cloud Shell, use a [alternativa com Key Vault](#alternativa-secret-scope-com-key-vault).

### 4. Criar o catálogo

No **SQL Editor** do Databricks, rode:

```sql
CREATE CATALOG IF NOT EXISTS streaming_demo;
CREATE SCHEMA IF NOT EXISTS streaming_demo.pedidos;
CREATE VOLUME IF NOT EXISTS streaming_demo.pedidos.checkpoints;
CREATE VOLUME IF NOT EXISTS streaming_demo.pedidos.controle;
```

Se o `CREATE CATALOG` pedir um local de armazenamento:

1. Crie um container no Cloud Shell:

   ```powershell
   az storage container create --account-name ststreamingdemo --name streaming-demo --auth-mode login
   ```

2. Em **Catalog → External Data**, crie uma External Location para `abfss://streaming-demo@ststreamingdemo.dfs.core.windows.net/`, com a credencial ligada a `acc-streaming-demo`.
3. Troque a primeira linha do SQL por esta e rode tudo de novo:

   ```sql
   CREATE CATALOG IF NOT EXISTS streaming_demo
   MANAGED LOCATION 'abfss://streaming-demo@ststreamingdemo.dfs.core.windows.net/';
   ```

O storage precisa ter hierarchical namespace ativo, e o Access Connector precisa do papel `Storage Blob Data Contributor`.

### 5. Criar o cluster

Em **Compute → Create compute**:

| Campo | Valor |
|---|---|
| Nome | `streaming-demo` |
| Modo | Single node |
| Runtime | A versão LTS mais recente |
| Acesso | Dedicated |
| Máquina | `Standard_D8ds_v5`, porque produtor, consumidor e painel rodam juntos |
| Encerrar por inatividade | 60 minutos |

Se faltar cota, escolha outra máquina de 8 vCPUs.

### 6. Importar os notebooks

1. Em **Workspace → Home**, crie a pasta `demo-streaming`.
2. Na pasta, escolha **Import** e envie os seis arquivos de [azure/notebooks](../../azure/notebooks).
3. Confirme que aparecem `00_config` a `05_resetar`, todos como notebooks.

Se você criou o namespace com outro nome, ajuste `NAMESPACE` em `00_config`.

### 7. Testar a infraestrutura

Crie um notebook temporário ligado ao cluster e rode:

```python
import socket

socket.create_connection(("evh-streaming-demo.servicebus.windows.net", 9093), timeout=5).close()
print("Rede OK")

segredo = dbutils.secrets.get("streaming-demo", "eventhub-connection")
print("Segredo OK", len(segredo) > 0)

display(spark.sql("SHOW VOLUMES IN streaming_demo.pedidos"))
```

Devem aparecer `Rede OK`, `Segredo OK True` e os dois Volumes. Se a conexão falhar, a saída da VNet para a porta `9093` está bloqueada.

Depois, faça um ensaio completo com [02-executar-demo.md](02-executar-demo.md).

## Parte 2: resetar entre ensaios

1. Interrompa os notebooks do produtor, do consumidor e do painel.
2. No Cloud Shell, recrie o hub. O primeiro comando apaga todas as mensagens do tópico:

   ```powershell
   az eventhubs eventhub delete --resource-group rsg-streaming-demo --namespace-name evh-streaming-demo --name pedidos
   ```

   ```powershell
   az eventhubs eventhub create --resource-group rsg-streaming-demo --namespace-name evh-streaming-demo --name pedidos --partition-count 3 --cleanup-policy Delete --retention-time-in-hours 24
   ```

3. Execute o notebook `05_resetar`.

## Parte 3: desligar depois da demo

O Event Hubs cobra por hora mesmo parado. Para interromper os custos e manter o resto:

1. Interrompa os notebooks.
2. Termine o cluster em **Compute**.
3. Apague o namespace:

   ```powershell
   az eventhubs namespace delete --resource-group rsg-streaming-demo --name evh-streaming-demo
   ```

**Para a próxima demo:**
1. Repita a etapa 2 deste guia.
2. Rode o comando `$c = ...` da etapa 3, porque a chave será nova. O scope já existe.
3. Execute `05_resetar`.

## Parte 4: excluir tudo

> **Atenção:** apagar o resource group remove todos os recursos dele, inclusive o workspace e o storage. Isso não tem volta.

1. Confira os recursos do grupo:

   ```powershell
   az resource list --resource-group rsg-streaming-demo --query "[].{nome:name,tipo:type}" -o table
   ```

2. No SQL Editor, apague o catálogo, que pode continuar no metastore depois que o workspace for removido:

   ```sql
   DROP CATALOG streaming_demo CASCADE;
   ```

3. Se você criou uma External Location, apague-a em **Catalog → External Data**.
4. Apague o grupo:

   ```powershell
   az group delete --name rsg-streaming-demo
   ```

5. Confirme que o comando abaixo retorna `false`:

   ```powershell
   az group exists --name rsg-streaming-demo
   ```

Se você usou Key Vault, ele fica em exclusão reversível por até 90 dias, com o nome reservado. Para liberar o nome, use **Key vaults → Manage deleted vaults**.

## Alternativa: secret scope com Key Vault

Use este caminho se o `az rest` falhar. Ele substitui a etapa 3.

1. No portal, crie um **Key Vault** chamado `kvt-streaming-demo` no grupo `rsg-streaming-demo`, em East US, no plano Standard.
2. Em **Access configuration**, escolha **Vault access policy**. Em **Networking**, deixe o acesso público ligado.
3. No namespace do Event Hubs, abra **Settings → Shared access policies → demo** e copie **Connection string–primary key**.
4. No Key Vault, vá em **Objects → Secrets → Generate/Import** e crie o segredo `eventhub-connection` com esse valor.
5. Em **Settings → Properties**, copie **Vault URI** e **Resource ID**.
6. Abra `https://URL_DO_WORKSPACE#secrets/createScope`, com o `S` maiúsculo.
7. Preencha **Scope Name** com `streaming-demo`, **Manage Principal** com `Creator`, **DNS Name** com o Vault URI e **Resource ID** com o valor copiado.

Quando o namespace for recriado, atualize o segredo no Key Vault.
