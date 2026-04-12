# Design: Channel por Partição Kafka

**Data:** 2026-04-12
**Contexto:** Creditbus.Facade — feature CardsIngestion
**Objetivo:** Desacoplar consumo de processamento usando `Channel<T>` por partição Kafka, garantindo roteamento por chave, workers resilientes e commit de offsets sem perda.

---

## Problema

O `KafkaConsumerBackgroundService` atual processa mensagens de forma estritamente sequencial: lê uma mensagem, processa completamente, lê a próxima. Para uma volumetria de até 100 mil mensagens, isso cria um gargalo direto — enquanto a UseCase executa (ex: escrita em banco), o broker fica sem receber novos `Fetch` do consumer.

Requisitos adicionais que o design deve atender:

- Mensagens do mesmo `TradingAccount` **não podem ser processadas concorrentemente** (risco de race condition em atualizações de limite de crédito, status do cartão, etc.)
- Workers não devem morrer silenciosamente
- Offsets não devem avançar antes do processamento ser confirmado

---

## Decisão de Arquitetura

**Abordagem escolhida: Channel por partição Kafka.**

Um `PartitionWorker` é criado para cada partição atribuída ao consumer. Cada worker possui um `Channel<ConsumeResult>` bounded e processa mensagens sequencialmente. O Kafka roteia mensagens de mesmo `TradingAccount` para a mesma partição via hash da key — garantindo exclusividade por titular sem nenhuma lógica adicional no app.

**Pré-requisito crítico:** o producer deve definir o `key` da mensagem Kafka como o valor do `TradingAccount`. Sem isso, o roteamento por partição não garante exclusividade.

---

## Componentes

### Novos

#### `PartitionWorker`
`Shared/Infrastructure/Kafka/PartitionWorker.cs`

Responsabilidade: processar todas as mensagens de uma partição Kafka específica.

- Possui um `Channel<ConsumeResult<string, string>>` com capacidade configurável (`BoundedChannelOptions`)
- Loop interno com `ReadAllAsync` → Polly pipeline → handler → `StoreOffset`
- O `try/catch` envolve **todo o loop** — qualquer exceção não tratada é logada como `Critical` e o worker aguarda 1 segundo antes de reiniciar, nunca encerrando silenciosamente
- `StopAsync()` completa o channel (`Writer.Complete()`) e aguarda o dreno antes de retornar — garante que mensagens em buffer sejam processadas antes do rebalanceamento

**Interface pública:**
```csharp
Task StartAsync(CancellationToken cancellationToken);
Task StopAsync();
ValueTask EnqueueAsync(ConsumeResult<string, string> result, CancellationToken cancellationToken);
```

#### `PartitionWorkerRegistry`
`Shared/Infrastructure/Kafka/PartitionWorkerRegistry.cs`

Dicionário thread-safe de `TopicPartition → PartitionWorker`. Usado pelo `KafkaConsumerBackgroundService` para criar, recuperar e remover workers dinamicamente durante rebalanceamento.

**Interface pública:**
```csharp
void Add(TopicPartition partition, PartitionWorker worker);
PartitionWorker Remove(TopicPartition partition);
bool TryGet(TopicPartition partition, out PartitionWorker worker);
```

---

### Modificados

#### `KafkaConsumerBackgroundService`
- O `IConsumer<string, string>` é construído com `SetPartitionsAssignedHandler` e `SetPartitionsRevokedHandler`
- **OnAssigned:** cria um `PartitionWorker` para cada nova partição, inicia a task de processamento, registra no `PartitionWorkerRegistry`
- **OnRevoked:** para cada partição revogada, chama `worker.StopAsync()` (drena o channel) e remove do registry
- O loop principal passa a fazer apenas: `Consume()` → `registry.TryGet(partition)` → `worker.EnqueueAsync(message)`
- Remove toda lógica de processamento direto (`ProcessMessageAsync` é movida para `PartitionWorker`)

#### `KafkaOptions`
Adiciona a propriedade:
```csharp
public int ChannelCapacity { get; init; } = 1000;
```
Controla o tamanho do buffer por worker. Quando o channel está cheio, o reader é pausado automaticamente (back-pressure nativo do `BoundedChannel`).

#### `KafkaServiceExtensions`
Atualiza o `ConsumerConfig`:
```csharp
EnableAutoOffsetStore = false,  // nunca avança offset sozinho
EnableAutoCommit = true,        // commit periódico do que foi stored
AutoCommitIntervalMs = 5000
```
Registra `PartitionWorkerRegistry` como singleton no DI.

---

### Intactos

Os seguintes componentes **não são alterados**:

| Componente | Motivo |
|---|---|
| `IKafkaMessageHandler` | Interface pública estável |
| `KafkaMessageHandler<T>` | Deserialização continua funcionando |
| `KafkaHandlerRegistry` | Roteamento por `MessageType` inalterado |
| `KafkaDlqPublisher` | DLQ inalterado |
| `CardsIngestionKafkaConsumer` | Handler concreto inalterado |
| `KafkaOptions.Retry` | Configuração de retry inalterada |

---

## Fluxo de Dados

```
Kafka Broker (4 partições, key = TradingAccount)
     │
     ▼
KafkaConsumerBackgroundService
     ├── OnPartitionsAssigned → cria PartitionWorker + inicia task
     ├── OnPartitionsRevoked  → StopAsync (drena) + remove do registry
     │
     └── Loop: Consume() → registry.TryGet(partition) → worker.EnqueueAsync()

PartitionWorker (1 por partição)
     └── Channel<ConsumeResult> (bounded, capacidade via ChannelCapacity)
           └── ReadAllAsync → Polly → Handler → StoreOffset
                                                      │
                                              EnableAutoCommit periódico
```

---

## Tratamento de Erros

### Worker resiliente
O loop de processamento do `PartitionWorker` nunca termina por exceção:

```
try {
    await foreach msg → processa
    break  // parada limpa via channel.Complete()
}
catch (OperationCanceledException) { break }  // shutdown normal
catch (Exception ex) {
    LogCritical(ex)       // visível nos logs
    await Delay(1s)       // pausa antes de reiniciar
}                         // loop continua — worker nunca morre silenciosamente
```

### Offsets sem perda por fora de ordem
- `EnableAutoOffsetStore = false` — o offset nunca avança automaticamente
- `StoreOffset` é chamado após cada mensagem processada com sucesso ou encaminhada à DLQ
- Como cada worker processa sequencialmente sua partição, o offset avança sempre em ordem — sem risco de saltar mensagens em restart

### Rebalanceamento
- **OnRevoked:** `Writer.Complete()` + await no dreno — mensagens já lidas do Kafka têm seus offsets stored antes da partição ser entregue a outro consumer
- Não há risco de perda de mensagem durante rebalanceamento

---

## Configuração (`appsettings.json`)

```json
"Kafka": {
  "BootstrapServers": "localhost:9092",
  "GroupId": "creditbus-facade",
  "Topic": "creditbus.ingestion",
  "DlqTopic": "creditbus.ingestion.dlq",
  "ChannelCapacity": 1000,
  "Retry": {
    "MaxAttempts": 3,
    "InitialDelayMs": 1000,
    "MaxDelayMs": 30000,
    "JitterFactor": 0.2
  }
}
```

---

## Impacto em Performance

| Métrica | Antes | Depois |
|---|---|---|
| Paralelismo | 0 (sequencial) | 1 worker por partição (4 com compose atual) |
| Leitura do broker | Pausada durante processamento | Contínua |
| Back-pressure | Nenhum | BoundedChannel pausa reader se worker sobrecarregado |
| Commit | Por mensagem (individual) | StoreOffset + AutoCommit periódico |

---

## Restrições e Decisões Adiadas

- O número de workers é determinado pelo número de partições Kafka atribuídas — não é configurado separadamente no `appsettings.json`. Para aumentar o paralelismo, aumentar as partições no `compose.yaml` (já configurado com 4).
- Monitoramento de lag por partição (ex: métricas Prometheus) está fora do escopo deste design.
- A implementação de múltiplas instâncias do serviço (consumer group com várias réplicas) é compatível com este design mas não é escopo desta iteração.
