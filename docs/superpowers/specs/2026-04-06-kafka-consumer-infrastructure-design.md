# Kafka Consumer Infrastructure Design

**Date:** 2026-04-06
**Feature:** Shared Kafka consumer infrastructure for multi-contract topic ingestion
**Status:** Approved

---

## Overview

Implement a shared Kafka consumer infrastructure in `Shared/Infrastructure/Kafka/` that:

- Consumes the multi-contract topic `creditbus.ingestion`
- Routes messages to feature-specific handlers based on the `message-type` Kafka header
- Retries failed messages using Polly with exponential backoff + jitter (configurable)
- Publishes exhausted messages to the DLQ topic `creditbus.ingestion.dlq`
- Commits offsets manually only after success or DLQ delivery

---

## Libraries

| Package | Purpose |
|---|---|
| `Confluent.Kafka` | Kafka consumer and producer client |
| `Polly` | Retry policy (exponential backoff + jitter) |

---

## Folder Structure

```
Creditbus.Facade/
├── Shared/
│   └── Infrastructure/
│       └── Kafka/
│           ├── KafkaOptions.cs                   # Typed configuration
│           ├── IKafkaMessageHandler.cs            # Handler interface
│           ├── KafkaHandlerRegistry.cs            # message-type → handler resolution
│           ├── KafkaDlqPublisher.cs               # DLQ producer
│           ├── KafkaConsumerBackgroundService.cs  # Poll loop (BackgroundService)
│           └── KafkaServiceExtensions.cs          # AddKafkaInfrastructure() DI extension
│
└── Features/
    └── CardsIngestion/
        └── Infrastructure/
            └── CardsIngestionKafkaHandler.cs      # Concrete handler for CardsIngestion feature
```

---

## Configuration

**appsettings.json:**
```json
{
  "Kafka": {
    "BootstrapServers": "localhost:9092",
    "GroupId": "creditbus-facade",
    "Topic": "creditbus.ingestion",
    "DlqTopic": "creditbus.ingestion.dlq",
    "Retry": {
      "MaxAttempts": 3,
      "InitialDelayMs": 1000,
      "MaxDelayMs": 30000,
      "JitterFactor": 0.2
    }
  }
}
```

**KafkaOptions.cs** maps this structure via `IOptions<KafkaOptions>`. `RetryOptions` is a nested class.

---

## Message Routing

Messages are routed by the Kafka header `message-type`.

**IKafkaMessageHandler interface:**
```csharp
public interface IKafkaMessageHandler
{
    string MessageType { get; }
    Task HandleAsync(string payload, CancellationToken cancellationToken);
}
```

**KafkaHandlerRegistry:**
- Receives all `IKafkaMessageHandler` registrations via `IEnumerable<IKafkaMessageHandler>` from DI
- Builds a `Dictionary<string, IKafkaMessageHandler>` indexed by `MessageType`
- Exposes `Resolve(string messageType)` — throws a descriptive exception if the type is unrecognized

**Adding a new feature handler:**
Implement `IKafkaMessageHandler` in the feature's `Infrastructure/` folder and register it in DI. No changes to shared infrastructure required.

**Example (CardsIngestion):**
```csharp
public class CardsIngestionKafkaHandler : IKafkaMessageHandler
{
    public string MessageType => "CardsIngestionEvent";
    public Task HandleAsync(string payload, CancellationToken ct) { ... }
}
```

---

## Retry Policy (Polly)

Policy: `WaitAndRetryAsync` — handles all exceptions.

**Delay formula per attempt:**
```
baseDelay  = Min(InitialDelayMs * 2^(attempt - 1), MaxDelayMs)
jitter     = Random(-baseDelay * JitterFactor, +baseDelay * JitterFactor)
finalDelay = baseDelay + jitter
```

**Example with defaults (InitialDelayMs=1000, MaxDelayMs=30000, JitterFactor=0.2, MaxAttempts=3):**

| Attempt | Base delay | Jitter range | Approx final delay |
|---------|-----------|--------------|-------------------|
| 1       | 1000ms    | ±200ms       | 800ms – 1200ms    |
| 2       | 2000ms    | ±400ms       | 1600ms – 2400ms   |
| 3       | 4000ms    | ±800ms       | 3200ms – 4800ms   |

After `MaxAttempts` failures, the message is sent to the DLQ. `MaxAttempts` is the total number of execution attempts, including the first. With `MaxAttempts: 3`, the handler runs up to 3 times (2 retries after the initial attempt) before the message is dead-lettered.

---

## DLQ (Dead Letter Queue)

**Topic:** `creditbus.ingestion.dlq`

When all retry attempts are exhausted, `KafkaDlqPublisher`:
1. Publishes the original payload to `creditbus.ingestion.dlq`
2. Preserves all original Kafka headers (including `message-type`) for traceability
3. Adds a `dlq-reason` header containing the last exception message

---

## Poll Loop Flow

```
Poll message
  └─ Read message-type header
       └─ Resolve handler from KafkaHandlerRegistry
            └─ Execute handler with Polly retry policy
                 ├─ Success → commit offset
                 └─ All retries exhausted → KafkaDlqPublisher → commit offset
```

**Offset commit strategy:** manual commit (`EnableAutoCommit = false`). The offset is committed only after successful processing **or** after confirmed DLQ delivery. It is never committed mid-retry.

---

## DI Registration (Program.cs)

```csharp
builder.Services.AddKafkaInfrastructure(builder.Configuration);
```

`KafkaServiceExtensions.AddKafkaInfrastructure()` registers:
- `KafkaOptions` via `IOptions<>`
- `KafkaHandlerRegistry` as singleton
- `KafkaDlqPublisher` as singleton
- `KafkaConsumerBackgroundService` as hosted service
- All `IKafkaMessageHandler` implementations from all features

---

## Extension Contract

To add a new feature consumer:
1. Create `Features/<FeatureName>/Infrastructure/<FeatureName>KafkaHandler.cs` implementing `IKafkaMessageHandler`
2. Register the handler in `KafkaServiceExtensions` (or via the feature's own DI extension)
3. Define the `MessageType` string constant — must match exactly the value sent in the `message-type` header by the producer
