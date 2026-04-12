# Channel por Partição Kafka — Plano de Implementação

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Desacoplar leitura e processamento de mensagens Kafka usando um `PartitionWorker` por partição, garantindo roteamento por chave, workers resilientes e commit de offsets sem perda.

**Architecture:** Um loop de leitura leve faz `Consume()` e enfileira em `Channel<ConsumeResult>` do worker correspondente à partição. Cada `PartitionWorker` processa sequencialmente — garantindo que o mesmo `TradingAccount` (que Kafka roteia para a mesma partição via message key) nunca seja processado concorrentemente. Workers se reiniciam em caso de exceção inesperada e nunca morrem silenciosamente. `StoreOffset` substitui `Commit` para garantir ordering correto de offsets.

**Tech Stack:** .NET 10, Confluent.Kafka 2.14, Polly 8, System.Threading.Channels, xUnit, NSubstitute, FluentAssertions.

---

## Mapa de arquivos

| Ação | Arquivo |
|---|---|
| Criar | `Creditbus.Facade/Shared/Infrastructure/Kafka/IPartitionWorker.cs` |
| Criar | `Creditbus.Facade/Shared/Infrastructure/Kafka/PartitionWorker.cs` |
| Criar | `Creditbus.Facade/Shared/Infrastructure/Kafka/PartitionWorkerRegistry.cs` |
| Modificar | `Creditbus.Facade/Shared/Infrastructure/Kafka/KafkaOptions.cs` |
| Modificar | `Creditbus.Facade/Shared/Infrastructure/Kafka/KafkaConsumerBackgroundService.cs` |
| Modificar | `Creditbus.Facade/Shared/Infrastructure/Kafka/KafkaServiceExtensions.cs` |
| Modificar | `Creditbus.Facade/appsettings.json` |
| Criar | `Creditbus.Facade.Tests/Shared/Infrastructure/Kafka/PartitionWorkerRegistryTests.cs` |
| Criar | `Creditbus.Facade.Tests/Shared/Infrastructure/Kafka/PartitionWorkerTests.cs` |
| Modificar | `Creditbus.Facade.Tests/Shared/Infrastructure/Kafka/KafkaConsumerBackgroundServiceTests.cs` |

---

## Task 1: KafkaOptions — adicionar ChannelCapacity

**Files:**
- Modify: `Creditbus.Facade/Shared/Infrastructure/Kafka/KafkaOptions.cs`
- Modify: `Creditbus.Facade/appsettings.json`

- [ ] **Step 1: Adicionar propriedade ChannelCapacity em KafkaOptions**

Substituir o conteúdo de `Creditbus.Facade/Shared/Infrastructure/Kafka/KafkaOptions.cs`:

```csharp
namespace Creditbus.Facade.Shared.Infrastructure.Kafka;

public sealed class KafkaOptions
{
    public const string SectionName = "Kafka";

    public string BootstrapServers { get; init; } = string.Empty;
    public string GroupId { get; init; } = string.Empty;
    public string Topic { get; init; } = string.Empty;
    public string DlqTopic { get; init; } = string.Empty;
    public int ChannelCapacity { get; init; } = 1000;
    public RetryOptions Retry { get; init; } = new();
}

public sealed class RetryOptions
{
    public int MaxAttempts { get; init; } = 3;
    public int InitialDelayMs { get; init; } = 1000;
    public int MaxDelayMs { get; init; } = 30000;
    public double JitterFactor { get; init; } = 0.2;
}
```

- [ ] **Step 2: Adicionar ChannelCapacity no appsettings.json**

Substituir o conteúdo de `Creditbus.Facade/appsettings.json`:

```json
{
  "Logging": {
    "LogLevel": {
      "Default": "Information",
      "Microsoft.AspNetCore": "Warning"
    }
  },
  "AllowedHosts": "*",
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
}
```

- [ ] **Step 3: Verificar build**

```bash
dotnet build Creditbus.Facade.sln
```

Esperado: `Build succeeded.`

- [ ] **Step 4: Commit**

```bash
git add Creditbus.Facade/Shared/Infrastructure/Kafka/KafkaOptions.cs
git add Creditbus.Facade/appsettings.json
git commit -m "feat: adiciona ChannelCapacity em KafkaOptions"
```

---

## Task 2: IPartitionWorker + PartitionWorkerRegistry

**Files:**
- Create: `Creditbus.Facade/Shared/Infrastructure/Kafka/IPartitionWorker.cs`
- Create: `Creditbus.Facade/Shared/Infrastructure/Kafka/PartitionWorkerRegistry.cs`
- Create: `Creditbus.Facade.Tests/Shared/Infrastructure/Kafka/PartitionWorkerRegistryTests.cs`

- [ ] **Step 1: Criar IPartitionWorker**

Criar `Creditbus.Facade/Shared/Infrastructure/Kafka/IPartitionWorker.cs`:

```csharp
using Confluent.Kafka;

namespace Creditbus.Facade.Shared.Infrastructure.Kafka;

public interface IPartitionWorker
{
    void Start();
    Task StopAsync();
    ValueTask EnqueueAsync(ConsumeResult<string, string> result, CancellationToken cancellationToken);
}
```

- [ ] **Step 2: Escrever os testes de PartitionWorkerRegistry (falham — classe não existe)**

Criar `Creditbus.Facade.Tests/Shared/Infrastructure/Kafka/PartitionWorkerRegistryTests.cs`:

```csharp
using Confluent.Kafka;
using Creditbus.Facade.Shared.Infrastructure.Kafka;
using FluentAssertions;
using NSubstitute;

namespace Creditbus.Facade.Tests.Shared.Infrastructure.Kafka;

public class PartitionWorkerRegistryTests
{
    private static TopicPartition Partition(int id) =>
        new("test-topic", new Partition(id));

    private static IPartitionWorker FakeWorker() =>
        Substitute.For<IPartitionWorker>();

    private readonly PartitionWorkerRegistry _sut = new();

    [Fact]
    public void TryGet_ReturnsWorker_AfterAdd()
    {
        var partition = Partition(0);
        var worker = FakeWorker();

        _sut.Add(partition, worker);

        _sut.TryGet(partition, out var result).Should().BeTrue();
        result.Should().BeSameAs(worker);
    }

    [Fact]
    public void TryGet_ReturnsFalse_WhenPartitionNotRegistered()
    {
        _sut.TryGet(Partition(99), out var result).Should().BeFalse();
        result.Should().BeNull();
    }

    [Fact]
    public void Remove_ReturnsAndRemovesWorker()
    {
        var partition = Partition(1);
        var worker = FakeWorker();
        _sut.Add(partition, worker);

        var removed = _sut.Remove(partition);

        removed.Should().BeSameAs(worker);
        _sut.TryGet(partition, out _).Should().BeFalse();
    }

    [Fact]
    public void Remove_ReturnsNull_WhenPartitionNotRegistered()
    {
        _sut.Remove(Partition(42)).Should().BeNull();
    }

    [Fact]
    public void Add_ThrowsInvalidOperationException_OnDuplicatePartition()
    {
        var partition = Partition(0);
        _sut.Add(partition, FakeWorker());

        var act = () => _sut.Add(partition, FakeWorker());

        act.Should().Throw<InvalidOperationException>()
            .WithMessage("*already registered*");
    }

    [Fact]
    public void GetAll_ReturnsAllRegisteredWorkers()
    {
        var w0 = FakeWorker();
        var w1 = FakeWorker();
        _sut.Add(Partition(0), w0);
        _sut.Add(Partition(1), w1);

        _sut.GetAll().Should().BeEquivalentTo(new[] { w0, w1 });
    }
}
```

- [ ] **Step 3: Rodar os testes — devem falhar**

```bash
dotnet test Creditbus.Facade.Tests/Creditbus.Facade.Tests.csproj --filter "PartitionWorkerRegistryTests" -v minimal
```

Esperado: `FAILED` — `PartitionWorkerRegistry` não existe.

- [ ] **Step 4: Implementar PartitionWorkerRegistry**

Criar `Creditbus.Facade/Shared/Infrastructure/Kafka/PartitionWorkerRegistry.cs`:

```csharp
using Confluent.Kafka;

namespace Creditbus.Facade.Shared.Infrastructure.Kafka;

public sealed class PartitionWorkerRegistry
{
    private readonly Dictionary<TopicPartition, IPartitionWorker> _workers = new();
    private readonly Lock _lock = new();

    public void Add(TopicPartition partition, IPartitionWorker worker)
    {
        lock (_lock)
        {
            if (!_workers.TryAdd(partition, worker))
                throw new InvalidOperationException(
                    $"Worker already registered for partition '{partition}'.");
        }
    }

    public IPartitionWorker? Remove(TopicPartition partition)
    {
        lock (_lock)
        {
            _workers.Remove(partition, out var worker);
            return worker;
        }
    }

    public bool TryGet(TopicPartition partition, out IPartitionWorker? worker)
    {
        lock (_lock)
        {
            return _workers.TryGetValue(partition, out worker);
        }
    }

    public IReadOnlyCollection<IPartitionWorker> GetAll()
    {
        lock (_lock)
        {
            return _workers.Values.ToList();
        }
    }
}
```

- [ ] **Step 5: Rodar os testes — devem passar**

```bash
dotnet test Creditbus.Facade.Tests/Creditbus.Facade.Tests.csproj --filter "PartitionWorkerRegistryTests" -v minimal
```

Esperado: `Passed: 6`

- [ ] **Step 6: Commit**

```bash
git add Creditbus.Facade/Shared/Infrastructure/Kafka/IPartitionWorker.cs
git add Creditbus.Facade/Shared/Infrastructure/Kafka/PartitionWorkerRegistry.cs
git add Creditbus.Facade.Tests/Shared/Infrastructure/Kafka/PartitionWorkerRegistryTests.cs
git commit -m "feat: adiciona IPartitionWorker e PartitionWorkerRegistry"
```

---

## Task 3: PartitionWorker

**Files:**
- Create: `Creditbus.Facade/Shared/Infrastructure/Kafka/PartitionWorker.cs`
- Create: `Creditbus.Facade.Tests/Shared/Infrastructure/Kafka/PartitionWorkerTests.cs`

- [ ] **Step 1: Escrever os testes (falham — classe não existe)**

Criar `Creditbus.Facade.Tests/Shared/Infrastructure/Kafka/PartitionWorkerTests.cs`:

```csharp
using System.Text;
using Confluent.Kafka;
using Creditbus.Facade.Shared.Infrastructure.Kafka;
using FluentAssertions;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using NSubstitute;

namespace Creditbus.Facade.Tests.Shared.Infrastructure.Kafka;

public class PartitionWorkerTests
{
    private readonly IConsumer<string, string> _consumer;
    private readonly IProducer<string, string> _producer;
    private readonly KafkaOptions _options;

    public PartitionWorkerTests()
    {
        _consumer = Substitute.For<IConsumer<string, string>>();
        _producer = Substitute.For<IProducer<string, string>>();
        _producer
            .ProduceAsync(Arg.Any<string>(), Arg.Any<Message<string, string>>(), Arg.Any<CancellationToken>())
            .Returns(new DeliveryResult<string, string>());

        _options = new KafkaOptions
        {
            DlqTopic = "test-dlq",
            ChannelCapacity = 100,
            Retry = new RetryOptions
            {
                MaxAttempts = 1,
                InitialDelayMs = 0,
                MaxDelayMs = 0,
                JitterFactor = 0
            }
        };
    }

    private PartitionWorker BuildSut(params IKafkaMessageHandler[] handlers)
    {
        var handlerRegistry = new KafkaHandlerRegistry(handlers);
        var dlqPublisher = new KafkaDlqPublisher(_producer, Options.Create(_options));
        var pipeline = KafkaConsumerBackgroundService.BuildPipeline(_options.Retry);
        return new PartitionWorker(
            new TopicPartition("test-topic", new Partition(0)),
            _consumer,
            _options.ChannelCapacity,
            handlerRegistry,
            dlqPublisher,
            pipeline,
            NullLogger<PartitionWorker>.Instance);
    }

    private static ConsumeResult<string, string> MakeConsumeResult(string messageType, string payload) =>
        new()
        {
            TopicPartitionOffset = new TopicPartitionOffset("test-topic", new Partition(0), new Offset(1)),
            Message = new Message<string, string>
            {
                Value = payload,
                Headers = new Headers { { "message-type", Encoding.UTF8.GetBytes(messageType) } }
            }
        };

    private static ConsumeResult<string, string> MakeConsumeResultNoHeader(string payload) =>
        new()
        {
            TopicPartitionOffset = new TopicPartitionOffset("test-topic", new Partition(0), new Offset(1)),
            Message = new Message<string, string> { Value = payload, Headers = new Headers() }
        };

    [Fact]
    public async Task ProcessMessage_StoresOffset_WhenHandlerSucceeds()
    {
        var handler = Substitute.For<IKafkaMessageHandler>();
        handler.MessageType.Returns("CardsIngestionEvent");
        handler.HandleAsync(Arg.Any<string>(), Arg.Any<CancellationToken>())
            .Returns(Task.CompletedTask);
        var sut = BuildSut(handler);
        sut.Start();

        var result = MakeConsumeResult("CardsIngestionEvent", "payload");
        await sut.EnqueueAsync(result, CancellationToken.None);
        await sut.StopAsync();

        _consumer.Received(1).StoreOffset(result);
        await _producer.DidNotReceive().ProduceAsync(
            Arg.Any<string>(), Arg.Any<Message<string, string>>(), Arg.Any<CancellationToken>());
    }

    [Fact]
    public async Task ProcessMessage_SendsToDlqAndStoresOffset_WhenHandlerFailsAllRetries()
    {
        var handler = Substitute.For<IKafkaMessageHandler>();
        handler.MessageType.Returns("CardsIngestionEvent");
        handler.HandleAsync(Arg.Any<string>(), Arg.Any<CancellationToken>())
            .Returns(Task.FromException(new Exception("handler failed")));
        var sut = BuildSut(handler);
        sut.Start();

        var result = MakeConsumeResult("CardsIngestionEvent", "payload");
        await sut.EnqueueAsync(result, CancellationToken.None);
        await sut.StopAsync();

        await _producer.Received(1).ProduceAsync(
            _options.DlqTopic,
            Arg.Is<Message<string, string>>(m => m.Value == "payload"),
            Arg.Any<CancellationToken>());
        _consumer.Received(1).StoreOffset(result);
    }

    [Fact]
    public async Task ProcessMessage_SendsToDlqAndStoresOffset_WhenMessageTypeHeaderIsMissing()
    {
        var sut = BuildSut();
        sut.Start();

        var result = MakeConsumeResultNoHeader("payload");
        await sut.EnqueueAsync(result, CancellationToken.None);
        await sut.StopAsync();

        await _producer.Received(1).ProduceAsync(
            _options.DlqTopic,
            Arg.Is<Message<string, string>>(m =>
                m.Headers.Any(h =>
                    h.Key == "dlq-reason" &&
                    Encoding.UTF8.GetString(h.GetValueBytes()).Contains("message-type"))),
            Arg.Any<CancellationToken>());
        _consumer.Received(1).StoreOffset(result);
    }

    [Fact]
    public async Task ProcessMessage_SendsToDlqAndStoresOffset_WhenMessageTypeIsUnrecognized()
    {
        var sut = BuildSut(); // no handlers registered
        sut.Start();

        var result = MakeConsumeResult("UnknownType", "payload");
        await sut.EnqueueAsync(result, CancellationToken.None);
        await sut.StopAsync();

        await _producer.Received(1).ProduceAsync(
            _options.DlqTopic,
            Arg.Any<Message<string, string>>(),
            Arg.Any<CancellationToken>());
        _consumer.Received(1).StoreOffset(result);
    }

    [Fact]
    public async Task ProcessMessage_DoesNotSendToDlq_WhenHandlerSucceedsOnRetry()
    {
        var options = new KafkaOptions
        {
            DlqTopic = "test-dlq",
            ChannelCapacity = 100,
            Retry = new RetryOptions { MaxAttempts = 2, InitialDelayMs = 0, MaxDelayMs = 0, JitterFactor = 0 }
        };
        var handler = Substitute.For<IKafkaMessageHandler>();
        handler.MessageType.Returns("CardsIngestionEvent");
        handler.HandleAsync(Arg.Any<string>(), Arg.Any<CancellationToken>())
            .Returns(
                _ => Task.FromException(new Exception("first attempt fails")),
                _ => Task.CompletedTask);

        var handlerRegistry = new KafkaHandlerRegistry([handler]);
        var dlqPublisher = new KafkaDlqPublisher(_producer, Options.Create(options));
        var pipeline = KafkaConsumerBackgroundService.BuildPipeline(options.Retry);
        var sut = new PartitionWorker(
            new TopicPartition("test-topic", new Partition(0)),
            _consumer, options.ChannelCapacity,
            handlerRegistry, dlqPublisher, pipeline,
            NullLogger<PartitionWorker>.Instance);
        sut.Start();

        var result = MakeConsumeResult("CardsIngestionEvent", "payload");
        await sut.EnqueueAsync(result, CancellationToken.None);
        await sut.StopAsync();

        await _producer.DidNotReceive().ProduceAsync(
            Arg.Any<string>(), Arg.Any<Message<string, string>>(), Arg.Any<CancellationToken>());
        _consumer.Received(1).StoreOffset(result);
    }

    [Fact]
    public async Task StopAsync_DrainsChannelBeforeReturning()
    {
        var processedCount = 0;
        var handler = Substitute.For<IKafkaMessageHandler>();
        handler.MessageType.Returns("CardsIngestionEvent");
        handler.HandleAsync(Arg.Any<string>(), Arg.Any<CancellationToken>())
            .Returns(async _ =>
            {
                await Task.Delay(10);
                processedCount++;
            });
        var sut = BuildSut(handler);
        sut.Start();

        for (var i = 0; i < 5; i++)
            await sut.EnqueueAsync(MakeConsumeResult("CardsIngestionEvent", $"p{i}"), CancellationToken.None);

        await sut.StopAsync();

        processedCount.Should().Be(5);
    }
}
```

- [ ] **Step 2: Rodar os testes — devem falhar**

```bash
dotnet test Creditbus.Facade.Tests/Creditbus.Facade.Tests.csproj --filter "PartitionWorkerTests" -v minimal
```

Esperado: `FAILED` — `PartitionWorker` não existe.

- [ ] **Step 3: Implementar PartitionWorker**

Criar `Creditbus.Facade/Shared/Infrastructure/Kafka/PartitionWorker.cs`:

```csharp
using System.Text;
using System.Threading.Channels;
using Confluent.Kafka;
using Microsoft.Extensions.Logging;
using Polly;

namespace Creditbus.Facade.Shared.Infrastructure.Kafka;

public sealed class PartitionWorker : IPartitionWorker
{
    private readonly TopicPartition _partition;
    private readonly IConsumer<string, string> _consumer;
    private readonly Channel<ConsumeResult<string, string>> _channel;
    private readonly KafkaHandlerRegistry _handlerRegistry;
    private readonly KafkaDlqPublisher _dlqPublisher;
    private readonly ResiliencePipeline _pipeline;
    private readonly ILogger<PartitionWorker> _logger;
    private Task _loopTask = Task.CompletedTask;

    public PartitionWorker(
        TopicPartition partition,
        IConsumer<string, string> consumer,
        int channelCapacity,
        KafkaHandlerRegistry handlerRegistry,
        KafkaDlqPublisher dlqPublisher,
        ResiliencePipeline pipeline,
        ILogger<PartitionWorker> logger)
    {
        _partition = partition;
        _consumer = consumer;
        _handlerRegistry = handlerRegistry;
        _dlqPublisher = dlqPublisher;
        _pipeline = pipeline;
        _logger = logger;
        _channel = Channel.CreateBounded<ConsumeResult<string, string>>(
            new BoundedChannelOptions(channelCapacity)
            {
                FullMode = BoundedChannelFullMode.Wait,
                SingleWriter = true,
                SingleReader = true
            });
    }

    public void Start() =>
        _loopTask = ProcessLoopAsync();

    public async Task StopAsync()
    {
        _channel.Writer.TryComplete();
        await _loopTask;
    }

    public ValueTask EnqueueAsync(ConsumeResult<string, string> result, CancellationToken cancellationToken) =>
        _channel.Writer.WriteAsync(result, cancellationToken);

    private async Task ProcessLoopAsync()
    {
        while (true)
        {
            try
            {
                await foreach (var result in _channel.Reader.ReadAllAsync())
                    await ProcessMessageAsync(result);

                break; // channel completado — parada limpa
            }
            catch (Exception ex)
            {
                _logger.LogCritical(ex,
                    "PartitionWorker for {Partition} crashed unexpectedly. Restarting in 1s.", _partition);
                await Task.Delay(1000);
            }
        }
    }

    internal async Task ProcessMessageAsync(
        ConsumeResult<string, string> result,
        CancellationToken cancellationToken = default)
    {
        var messageTypeBytes = result.Message.Headers
            .FirstOrDefault(h => h.Key == "message-type")
            ?.GetValueBytes();

        if (messageTypeBytes is null)
        {
            _logger.LogWarning("Received message without 'message-type' header. Routing to DLQ.");
            await _dlqPublisher.PublishAsync(
                result.Message.Value,
                result.Message.Headers,
                "Missing message-type header",
                cancellationToken);
            _consumer.StoreOffset(result);
            return;
        }

        var messageType = Encoding.UTF8.GetString(messageTypeBytes);

        IKafkaMessageHandler handler;
        try
        {
            handler = _handlerRegistry.Resolve(messageType);
        }
        catch (InvalidOperationException ex)
        {
            _logger.LogWarning(ex, "Unrecognized message type '{MessageType}'. Routing to DLQ.", messageType);
            await _dlqPublisher.PublishAsync(
                result.Message.Value,
                result.Message.Headers,
                ex.Message,
                cancellationToken);
            _consumer.StoreOffset(result);
            return;
        }

        try
        {
            await _pipeline.ExecuteAsync(
                async ct => await handler.HandleAsync(result.Message.Value, ct),
                cancellationToken);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex,
                "Handler for '{MessageType}' failed after all retry attempts. Routing to DLQ.", messageType);
            await _dlqPublisher.PublishAsync(
                result.Message.Value,
                result.Message.Headers,
                ex.Message,
                cancellationToken);
        }

        _consumer.StoreOffset(result);
    }
}
```

- [ ] **Step 4: Rodar os testes — devem passar**

```bash
dotnet test Creditbus.Facade.Tests/Creditbus.Facade.Tests.csproj --filter "PartitionWorkerTests" -v minimal
```

Esperado: `Passed: 6`

- [ ] **Step 5: Commit**

```bash
git add Creditbus.Facade/Shared/Infrastructure/Kafka/PartitionWorker.cs
git add Creditbus.Facade.Tests/Shared/Infrastructure/Kafka/PartitionWorkerTests.cs
git commit -m "feat: adiciona PartitionWorker com channel bounded e resiliência"
```

---

## Task 4: Refatorar KafkaConsumerBackgroundService

**Files:**
- Modify: `Creditbus.Facade/Shared/Infrastructure/Kafka/KafkaConsumerBackgroundService.cs`
- Modify: `Creditbus.Facade.Tests/Shared/Infrastructure/Kafka/KafkaConsumerBackgroundServiceTests.cs`

- [ ] **Step 1: Substituir os testes de KafkaConsumerBackgroundService**

Substituir o conteúdo de `Creditbus.Facade.Tests/Shared/Infrastructure/Kafka/KafkaConsumerBackgroundServiceTests.cs`:

```csharp
using System.Text;
using Confluent.Kafka;
using Creditbus.Facade.Shared.Infrastructure.Kafka;
using FluentAssertions;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using NSubstitute;

namespace Creditbus.Facade.Tests.Shared.Infrastructure.Kafka;

public class KafkaConsumerBackgroundServiceTests
{
    private readonly IConsumer<string, string> _consumer;
    private readonly PartitionWorkerRegistry _workerRegistry;
    private readonly KafkaOptions _options;

    public KafkaConsumerBackgroundServiceTests()
    {
        _consumer = Substitute.For<IConsumer<string, string>>();
        _workerRegistry = new PartitionWorkerRegistry();
        _options = new KafkaOptions
        {
            Topic = "test-topic",
            Retry = new RetryOptions { MaxAttempts = 1, InitialDelayMs = 0, MaxDelayMs = 0, JitterFactor = 0 }
        };
    }

    private KafkaConsumerBackgroundService BuildSut() =>
        new(_consumer, _workerRegistry, Options.Create(_options),
            NullLogger<KafkaConsumerBackgroundService>.Instance);

    private static TopicPartition Partition(int id) =>
        new("test-topic", new Partition(id));

    private static ConsumeResult<string, string> MakeResult(int partition, string payload) =>
        new()
        {
            TopicPartitionOffset = new TopicPartitionOffset("test-topic", new Partition(partition), new Offset(1)),
            Message = new Message<string, string>
            {
                Value = payload,
                Headers = new Headers { { "message-type", Encoding.UTF8.GetBytes("CardsIngestionEvent") } }
            }
        };

    [Fact]
    public async Task ExecuteAsync_EnqueuesMessage_OnCorrectWorker()
    {
        var worker = Substitute.For<IPartitionWorker>();
        _workerRegistry.Add(Partition(0), worker);

        var result = MakeResult(0, "payload");

        using var cts = new CancellationTokenSource();
        _consumer.Consume(Arg.Any<CancellationToken>())
            .Returns(
                _ => result,
                _ => { cts.Cancel(); throw new OperationCanceledException(); });

        var sut = BuildSut();
        await sut.StartAsync(cts.Token);

        try { await sut.ExecuteTask!; } catch (OperationCanceledException) { }

        await worker.Received(1).EnqueueAsync(result, Arg.Any<CancellationToken>());
    }

    [Fact]
    public async Task ExecuteAsync_DoesNotThrow_WhenNoWorkerRegisteredForPartition()
    {
        // Nenhum worker registrado — deve logar erro e continuar sem explodir
        var result = MakeResult(0, "payload");

        using var cts = new CancellationTokenSource();
        _consumer.Consume(Arg.Any<CancellationToken>())
            .Returns(
                _ => result,
                _ => { cts.Cancel(); throw new OperationCanceledException(); });

        var sut = BuildSut();
        await sut.StartAsync(cts.Token);

        var act = async () =>
        {
            try { await sut.ExecuteTask!; } catch (OperationCanceledException) { }
        };

        await act.Should().NotThrowAsync();
    }
}
```

- [ ] **Step 2: Rodar os testes — devem falhar**

```bash
dotnet test Creditbus.Facade.Tests/Creditbus.Facade.Tests.csproj --filter "KafkaConsumerBackgroundServiceTests" -v minimal
```

Esperado: `FAILED` — assinatura do construtor incompatível com a implementação atual.

- [ ] **Step 3: Substituir KafkaConsumerBackgroundService**

Substituir o conteúdo de `Creditbus.Facade/Shared/Infrastructure/Kafka/KafkaConsumerBackgroundService.cs`:

```csharp
using Confluent.Kafka;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Polly;
using Polly.Retry;

namespace Creditbus.Facade.Shared.Infrastructure.Kafka;

public sealed class KafkaConsumerBackgroundService : BackgroundService
{
    private readonly IConsumer<string, string> _consumer;
    private readonly PartitionWorkerRegistry _workerRegistry;
    private readonly string _topic;
    private readonly ILogger<KafkaConsumerBackgroundService> _logger;

    public KafkaConsumerBackgroundService(
        IConsumer<string, string> consumer,
        PartitionWorkerRegistry workerRegistry,
        IOptions<KafkaOptions> options,
        ILogger<KafkaConsumerBackgroundService> logger)
    {
        _consumer = consumer;
        _workerRegistry = workerRegistry;
        _topic = options.Value.Topic;
        _logger = logger;
    }

    internal static ResiliencePipeline BuildPipeline(RetryOptions options)
    {
        var builder = new ResiliencePipelineBuilder();

        if (options.MaxAttempts > 1)
        {
            builder.AddRetry(new RetryStrategyOptions
            {
                MaxRetryAttempts = options.MaxAttempts - 1,
                ShouldHandle = new PredicateBuilder().Handle<Exception>(),
                DelayGenerator = args =>
                {
                    double baseMs = Math.Min(
                        options.InitialDelayMs * Math.Pow(2, args.AttemptNumber),
                        options.MaxDelayMs);
                    double jitter = (Random.Shared.NextDouble() * 2 - 1) * baseMs * options.JitterFactor;
                    return ValueTask.FromResult<TimeSpan?>(TimeSpan.FromMilliseconds(baseMs + jitter));
                }
            });
        }

        return builder.Build();
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        _consumer.Subscribe(_topic);
        try
        {
            while (!stoppingToken.IsCancellationRequested)
            {
                var result = _consumer.Consume(stoppingToken);

                if (!_workerRegistry.TryGet(result.TopicPartition, out var worker))
                {
                    _logger.LogError(
                        "No worker registered for partition {Partition}. Message at offset {Offset} will be skipped.",
                        result.TopicPartition, result.Offset);
                    continue;
                }

                await worker!.EnqueueAsync(result, stoppingToken);
            }
        }
        finally
        {
            _consumer.Close();
        }
    }
}
```

- [ ] **Step 4: Rodar os testes da suite completa**

```bash
dotnet test Creditbus.Facade.sln -v minimal
```

Esperado: todos os testes passando.

- [ ] **Step 5: Commit**

```bash
git add Creditbus.Facade/Shared/Infrastructure/Kafka/KafkaConsumerBackgroundService.cs
git add Creditbus.Facade.Tests/Shared/Infrastructure/Kafka/KafkaConsumerBackgroundServiceTests.cs
git commit -m "refactor: simplifica KafkaConsumerBackgroundService para roteamento por partição"
```

---

## Task 5: KafkaServiceExtensions — ConsumerConfig + PartitionWorker setup

**Files:**
- Modify: `Creditbus.Facade/Shared/Infrastructure/Kafka/KafkaServiceExtensions.cs`

- [ ] **Step 1: Substituir KafkaServiceExtensions**

Substituir o conteúdo de `Creditbus.Facade/Shared/Infrastructure/Kafka/KafkaServiceExtensions.cs`:

```csharp
using Confluent.Kafka;
using Creditbus.Facade.Features.CardsIngestion.Application.ProcessCardEvent;
using Creditbus.Facade.Features.CardsIngestion.Infrastructure;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace Creditbus.Facade.Shared.Infrastructure.Kafka;

public static class KafkaServiceExtensions
{
    public static IServiceCollection AddKafkaInfrastructure(
        this IServiceCollection services,
        IConfiguration configuration)
    {
        services.Configure<KafkaOptions>(configuration.GetSection(KafkaOptions.SectionName));

        services.AddSingleton<IProducer<string, string>>(sp =>
        {
            var options = sp.GetRequiredService<IOptions<KafkaOptions>>().Value;
            var config = new ProducerConfig { BootstrapServers = options.BootstrapServers };
            return new ProducerBuilder<string, string>(config).Build();
        });

        services.AddSingleton<PartitionWorkerRegistry>();

        services.AddSingleton<IConsumer<string, string>>(sp =>
        {
            var options = sp.GetRequiredService<IOptions<KafkaOptions>>().Value;
            var workerRegistry = sp.GetRequiredService<PartitionWorkerRegistry>();
            var handlerRegistry = sp.GetRequiredService<KafkaHandlerRegistry>();
            var dlqPublisher = sp.GetRequiredService<KafkaDlqPublisher>();
            var loggerFactory = sp.GetRequiredService<ILoggerFactory>();
            var pipeline = KafkaConsumerBackgroundService.BuildPipeline(options.Retry);

            var config = new ConsumerConfig
            {
                BootstrapServers = options.BootstrapServers,
                GroupId = options.GroupId,
                AutoOffsetReset = AutoOffsetReset.Earliest,
                EnableAutoCommit = true,
                AutoCommitIntervalMs = 5000,
                EnableAutoOffsetStore = false
            };

            return new ConsumerBuilder<string, string>(config)
                .SetPartitionsAssignedHandler((consumer, partitions) =>
                {
                    foreach (var tp in partitions)
                    {
                        var worker = new PartitionWorker(
                            tp,
                            consumer,
                            options.ChannelCapacity,
                            handlerRegistry,
                            dlqPublisher,
                            pipeline,
                            loggerFactory.CreateLogger<PartitionWorker>());
                        workerRegistry.Add(tp, worker);
                        worker.Start();
                    }
                })
                .SetPartitionsRevokedHandler((_, partitions) =>
                {
                    foreach (var tp in partitions)
                    {
                        var worker = workerRegistry.Remove(tp);
                        worker?.StopAsync().GetAwaiter().GetResult();
                    }
                })
                .Build();
        });

        services.AddScoped<IProcessCardEventUseCase, ProcessCardEventUseCase>();
        services.AddSingleton<IKafkaMessageHandler, CardsIngestionKafkaConsumer>();
        services.AddSingleton<KafkaHandlerRegistry>();
        services.AddSingleton<KafkaDlqPublisher>();
        services.AddHostedService<KafkaConsumerBackgroundService>();

        return services;
    }
}
```

- [ ] **Step 2: Build completo**

```bash
dotnet build Creditbus.Facade.sln
```

Esperado: `Build succeeded. 0 Warning(s) 0 Error(s)`

- [ ] **Step 3: Todos os testes verdes**

```bash
dotnet test Creditbus.Facade.sln -v minimal
```

Esperado: todos os testes passando.

- [ ] **Step 4: Commit final**

```bash
git add Creditbus.Facade/Shared/Infrastructure/Kafka/KafkaServiceExtensions.cs
git commit -m "feat: configura consumer com StoreOffset, AutoCommit e callbacks de partição"
```

---

## Verificação final

- [ ] **Build limpo**

```bash
dotnet build Creditbus.Facade.sln
```

Esperado: `Build succeeded. 0 Warning(s) 0 Error(s)`

- [ ] **Todos os testes verdes**

```bash
dotnet test Creditbus.Facade.sln -v normal
```

Esperado: todos os testes passando — `PartitionWorkerTests` (6), `PartitionWorkerRegistryTests` (6), `KafkaConsumerBackgroundServiceTests` (2) e todos os demais existentes.

- [ ] **Commit final**

```bash
git add -A
git commit -m "feat: implementa consumer channel por partição Kafka com workers resilientes"
```
