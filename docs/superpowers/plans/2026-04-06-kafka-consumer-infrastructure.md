# Kafka Consumer Infrastructure Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Implement a shared Kafka consumer infrastructure that consumes the multi-contract topic `creditbus.ingestion`, routes messages by `message-type` header to feature handlers, retries failures with Polly exponential backoff + jitter, and dead-letters exhausted messages to `creditbus.ingestion.dlq`.

**Architecture:** A `BackgroundService` polls Kafka with manual offset commit (`EnableAutoCommit = false`). Each message is dispatched to an `IKafkaMessageHandler` resolved by `message-type` header via `KafkaHandlerRegistry`. Polly's `WaitAndRetryAsync` wraps the handler call; on exhaustion, `KafkaDlqPublisher` forwards the message (with original headers + `dlq-reason`) to the DLQ topic before committing the offset.

**Tech Stack:** .NET 10, Confluent.Kafka, Polly v8, xUnit, NSubstitute, FluentAssertions

---

## File Map

| Action | Path | Responsibility |
|--------|------|----------------|
| Create | `Creditbus.Facade/Shared/Infrastructure/Kafka/KafkaOptions.cs` | Typed configuration bound from appsettings |
| Create | `Creditbus.Facade/Shared/Infrastructure/Kafka/IKafkaMessageHandler.cs` | Handler contract |
| Create | `Creditbus.Facade/Shared/Infrastructure/Kafka/KafkaHandlerRegistry.cs` | Routes message-type → handler |
| Create | `Creditbus.Facade/Shared/Infrastructure/Kafka/KafkaDlqPublisher.cs` | Publishes to DLQ topic |
| Create | `Creditbus.Facade/Shared/Infrastructure/Kafka/KafkaConsumerBackgroundService.cs` | Poll loop + processing |
| Create | `Creditbus.Facade/Shared/Infrastructure/Kafka/KafkaServiceExtensions.cs` | DI registration |
| Create | `Creditbus.Facade/Features/CardsIngestion/Infrastructure/CardsIngestionKafkaHandler.cs` | Stub handler for CardsIngestion |
| Create | `Creditbus.Facade/appsettings.json` | Kafka configuration |
| Modify | `Creditbus.Facade/Creditbus.Facade.csproj` | Add NuGet packages + InternalsVisibleTo |
| Modify | `Creditbus.Facade/Program.cs` | Wire AddKafkaInfrastructure |
| Create | `Creditbus.Facade.Tests/Creditbus.Facade.Tests.csproj` | Test project |
| Create | `Creditbus.Facade.Tests/Shared/Infrastructure/Kafka/KafkaHandlerRegistryTests.cs` | Unit tests |
| Create | `Creditbus.Facade.Tests/Shared/Infrastructure/Kafka/KafkaDlqPublisherTests.cs` | Unit tests |
| Create | `Creditbus.Facade.Tests/Shared/Infrastructure/Kafka/KafkaConsumerBackgroundServiceTests.cs` | Unit tests |
| Modify | `Creditbus.Facade.sln` | Add test project to solution |

---

## Task 1: Add NuGet packages, test project, and solution wiring

**Files:**
- Modify: `Creditbus.Facade/Creditbus.Facade.csproj`
- Create: `Creditbus.Facade.Tests/Creditbus.Facade.Tests.csproj`
- Modify: `Creditbus.Facade.sln`

- [ ] **Step 1: Add packages to main project**

```bash
cd Creditbus.Facade
dotnet add package Confluent.Kafka
dotnet add package Polly
```

- [ ] **Step 2: Add InternalsVisibleTo to main project csproj**

Open `Creditbus.Facade/Creditbus.Facade.csproj` and add inside `<Project>`:

```xml
<ItemGroup>
  <AssemblyAttribute Include="System.Runtime.CompilerServices.InternalsVisibleTo">
    <_Parameter1>Creditbus.Facade.Tests</_Parameter1>
  </AssemblyAttribute>
</ItemGroup>
```

- [ ] **Step 3: Create test project**

From the solution root (`C:\repos\BarramentoV3\Facade`):

```bash
dotnet new xunit -n Creditbus.Facade.Tests -o Creditbus.Facade.Tests
```

- [ ] **Step 4: Add test dependencies**

```bash
cd Creditbus.Facade.Tests
dotnet add package NSubstitute
dotnet add package FluentAssertions
dotnet add package Confluent.Kafka
dotnet add reference ../Creditbus.Facade/Creditbus.Facade.csproj
```

- [ ] **Step 5: Add test project to solution**

```bash
cd ..
dotnet sln add Creditbus.Facade.Tests/Creditbus.Facade.Tests.csproj
```

- [ ] **Step 6: Verify build**

```bash
dotnet build Creditbus.Facade.sln
```

Expected: Build succeeded with 0 errors.

- [ ] **Step 7: Commit**

```bash
git add Creditbus.Facade/Creditbus.Facade.csproj Creditbus.Facade.Tests/ Creditbus.Facade.sln
git commit -m "chore: add Confluent.Kafka, Polly packages and test project"
```

---

## Task 2: KafkaOptions and appsettings.json

**Files:**
- Create: `Creditbus.Facade/Shared/Infrastructure/Kafka/KafkaOptions.cs`
- Create: `Creditbus.Facade/appsettings.json`

- [ ] **Step 1: Create KafkaOptions.cs**

Create `Creditbus.Facade/Shared/Infrastructure/Kafka/KafkaOptions.cs`:

```csharp
namespace Creditbus.Facade.Shared.Infrastructure.Kafka;

public sealed class KafkaOptions
{
    public const string SectionName = "Kafka";

    public string BootstrapServers { get; init; } = string.Empty;
    public string GroupId { get; init; } = string.Empty;
    public string Topic { get; init; } = string.Empty;
    public string DlqTopic { get; init; } = string.Empty;
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

- [ ] **Step 2: Create appsettings.json**

Create `Creditbus.Facade/appsettings.json`:

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
    "Retry": {
      "MaxAttempts": 3,
      "InitialDelayMs": 1000,
      "MaxDelayMs": 30000,
      "JitterFactor": 0.2
    }
  }
}
```

- [ ] **Step 3: Verify build**

```bash
dotnet build Creditbus.Facade.sln
```

Expected: Build succeeded with 0 errors.

- [ ] **Step 4: Commit**

```bash
git add Creditbus.Facade/Shared/Infrastructure/Kafka/KafkaOptions.cs Creditbus.Facade/appsettings.json
git commit -m "feat: add KafkaOptions typed configuration"
```

---

## Task 3: IKafkaMessageHandler interface

**Files:**
- Create: `Creditbus.Facade/Shared/Infrastructure/Kafka/IKafkaMessageHandler.cs`

- [ ] **Step 1: Create IKafkaMessageHandler.cs**

Create `Creditbus.Facade/Shared/Infrastructure/Kafka/IKafkaMessageHandler.cs`:

```csharp
namespace Creditbus.Facade.Shared.Infrastructure.Kafka;

public interface IKafkaMessageHandler
{
    string MessageType { get; }
    Task HandleAsync(string payload, CancellationToken cancellationToken);
}
```

- [ ] **Step 2: Verify build**

```bash
dotnet build Creditbus.Facade.sln
```

Expected: Build succeeded with 0 errors.

- [ ] **Step 3: Commit**

```bash
git add Creditbus.Facade/Shared/Infrastructure/Kafka/IKafkaMessageHandler.cs
git commit -m "feat: add IKafkaMessageHandler interface"
```

---

## Task 4: KafkaHandlerRegistry (TDD)

**Files:**
- Create: `Creditbus.Facade/Shared/Infrastructure/Kafka/KafkaHandlerRegistry.cs`
- Create: `Creditbus.Facade.Tests/Shared/Infrastructure/Kafka/KafkaHandlerRegistryTests.cs`

- [ ] **Step 1: Write the failing tests**

Create `Creditbus.Facade.Tests/Shared/Infrastructure/Kafka/KafkaHandlerRegistryTests.cs`:

```csharp
using Creditbus.Facade.Shared.Infrastructure.Kafka;
using FluentAssertions;
using NSubstitute;

namespace Creditbus.Facade.Tests.Shared.Infrastructure.Kafka;

public class KafkaHandlerRegistryTests
{
    [Fact]
    public void Resolve_ReturnsCorrectHandler_WhenMessageTypeIsRegistered()
    {
        var handler = Substitute.For<IKafkaMessageHandler>();
        handler.MessageType.Returns("CardsIngestionEvent");
        var registry = new KafkaHandlerRegistry([handler]);

        var result = registry.Resolve("CardsIngestionEvent");

        result.Should().Be(handler);
    }

    [Fact]
    public void Resolve_IsCaseInsensitive()
    {
        var handler = Substitute.For<IKafkaMessageHandler>();
        handler.MessageType.Returns("CardsIngestionEvent");
        var registry = new KafkaHandlerRegistry([handler]);

        var result = registry.Resolve("cardsingestionevent");

        result.Should().Be(handler);
    }

    [Fact]
    public void Resolve_Throws_WhenMessageTypeIsNotRegistered()
    {
        var registry = new KafkaHandlerRegistry([]);

        var act = () => registry.Resolve("UnknownType");

        act.Should().Throw<InvalidOperationException>()
            .WithMessage("*UnknownType*");
    }

    [Fact]
    public void Constructor_Throws_WhenDuplicateMessageTypesAreRegistered()
    {
        var handler1 = Substitute.For<IKafkaMessageHandler>();
        handler1.MessageType.Returns("CardsIngestionEvent");
        var handler2 = Substitute.For<IKafkaMessageHandler>();
        handler2.MessageType.Returns("CardsIngestionEvent");

        var act = () => new KafkaHandlerRegistry([handler1, handler2]);

        act.Should().Throw<InvalidOperationException>()
            .WithMessage("*CardsIngestionEvent*");
    }
}
```

- [ ] **Step 2: Run tests to confirm they fail**

```bash
dotnet test Creditbus.Facade.sln --filter "FullyQualifiedName~KafkaHandlerRegistryTests"
```

Expected: Compilation error — `KafkaHandlerRegistry` does not exist yet.

- [ ] **Step 3: Implement KafkaHandlerRegistry**

Create `Creditbus.Facade/Shared/Infrastructure/Kafka/KafkaHandlerRegistry.cs`:

```csharp
namespace Creditbus.Facade.Shared.Infrastructure.Kafka;

public sealed class KafkaHandlerRegistry
{
    private readonly Dictionary<string, IKafkaMessageHandler> _handlers;

    public KafkaHandlerRegistry(IEnumerable<IKafkaMessageHandler> handlers)
    {
        _handlers = new Dictionary<string, IKafkaMessageHandler>(StringComparer.OrdinalIgnoreCase);

        foreach (var handler in handlers)
        {
            if (!_handlers.TryAdd(handler.MessageType, handler))
                throw new InvalidOperationException(
                    $"Duplicate handler registered for message type '{handler.MessageType}'.");
        }
    }

    public IKafkaMessageHandler Resolve(string messageType)
    {
        if (_handlers.TryGetValue(messageType, out var handler))
            return handler;

        throw new InvalidOperationException(
            $"No handler registered for message type '{messageType}'. " +
            $"Registered types: [{string.Join(", ", _handlers.Keys)}]");
    }
}
```

- [ ] **Step 4: Run tests to confirm they pass**

```bash
dotnet test Creditbus.Facade.sln --filter "FullyQualifiedName~KafkaHandlerRegistryTests"
```

Expected: 4 tests passed.

- [ ] **Step 5: Commit**

```bash
git add Creditbus.Facade/Shared/Infrastructure/Kafka/KafkaHandlerRegistry.cs \
        Creditbus.Facade.Tests/Shared/Infrastructure/Kafka/KafkaHandlerRegistryTests.cs
git commit -m "feat: add KafkaHandlerRegistry with message-type routing"
```

---

## Task 5: KafkaDlqPublisher (TDD)

**Files:**
- Create: `Creditbus.Facade/Shared/Infrastructure/Kafka/KafkaDlqPublisher.cs`
- Create: `Creditbus.Facade.Tests/Shared/Infrastructure/Kafka/KafkaDlqPublisherTests.cs`

- [ ] **Step 1: Write the failing tests**

Create `Creditbus.Facade.Tests/Shared/Infrastructure/Kafka/KafkaDlqPublisherTests.cs`:

```csharp
using System.Text;
using Confluent.Kafka;
using Creditbus.Facade.Shared.Infrastructure.Kafka;
using FluentAssertions;
using Microsoft.Extensions.Options;
using NSubstitute;

namespace Creditbus.Facade.Tests.Shared.Infrastructure.Kafka;

public class KafkaDlqPublisherTests
{
    private readonly IProducer<string, string> _producer;
    private readonly KafkaDlqPublisher _sut;
    private const string DlqTopic = "creditbus.ingestion.dlq";

    public KafkaDlqPublisherTests()
    {
        _producer = Substitute.For<IProducer<string, string>>();
        _producer
            .ProduceAsync(Arg.Any<string>(), Arg.Any<Message<string, string>>(), Arg.Any<CancellationToken>())
            .Returns(new DeliveryResult<string, string>());

        var options = Options.Create(new KafkaOptions { DlqTopic = DlqTopic });
        _sut = new KafkaDlqPublisher(_producer, options);
    }

    [Fact]
    public async Task PublishAsync_PublishesToDlqTopic()
    {
        var headers = new Headers();
        headers.Add("message-type", Encoding.UTF8.GetBytes("CardsIngestionEvent"));

        await _sut.PublishAsync("payload", headers, "some error", CancellationToken.None);

        await _producer.Received(1).ProduceAsync(
            DlqTopic,
            Arg.Any<Message<string, string>>(),
            Arg.Any<CancellationToken>());
    }

    [Fact]
    public async Task PublishAsync_PreservesOriginalPayload()
    {
        var headers = new Headers();

        await _sut.PublishAsync("original-payload", headers, "error", CancellationToken.None);

        await _producer.Received(1).ProduceAsync(
            Arg.Any<string>(),
            Arg.Is<Message<string, string>>(m => m.Value == "original-payload"),
            Arg.Any<CancellationToken>());
    }

    [Fact]
    public async Task PublishAsync_PreservesOriginalHeaders()
    {
        var headers = new Headers();
        headers.Add("message-type", Encoding.UTF8.GetBytes("CardsIngestionEvent"));
        headers.Add("correlation-id", Encoding.UTF8.GetBytes("abc-123"));

        await _sut.PublishAsync("payload", headers, "error", CancellationToken.None);

        await _producer.Received(1).ProduceAsync(
            Arg.Any<string>(),
            Arg.Is<Message<string, string>>(m =>
                m.Headers.Any(h => h.Key == "message-type") &&
                m.Headers.Any(h => h.Key == "correlation-id")),
            Arg.Any<CancellationToken>());
    }

    [Fact]
    public async Task PublishAsync_AddsDlqReasonHeader()
    {
        var headers = new Headers();

        await _sut.PublishAsync("payload", headers, "handler blew up", CancellationToken.None);

        await _producer.Received(1).ProduceAsync(
            Arg.Any<string>(),
            Arg.Is<Message<string, string>>(m =>
                m.Headers.Any(h =>
                    h.Key == "dlq-reason" &&
                    Encoding.UTF8.GetString(h.GetValueBytes()) == "handler blew up")),
            Arg.Any<CancellationToken>());
    }
}
```

- [ ] **Step 2: Run tests to confirm they fail**

```bash
dotnet test Creditbus.Facade.sln --filter "FullyQualifiedName~KafkaDlqPublisherTests"
```

Expected: Compilation error — `KafkaDlqPublisher` does not exist yet.

- [ ] **Step 3: Implement KafkaDlqPublisher**

Create `Creditbus.Facade/Shared/Infrastructure/Kafka/KafkaDlqPublisher.cs`:

```csharp
using System.Text;
using Confluent.Kafka;
using Microsoft.Extensions.Options;

namespace Creditbus.Facade.Shared.Infrastructure.Kafka;

public sealed class KafkaDlqPublisher
{
    private readonly IProducer<string, string> _producer;
    private readonly string _dlqTopic;

    public KafkaDlqPublisher(IProducer<string, string> producer, IOptions<KafkaOptions> options)
    {
        _producer = producer;
        _dlqTopic = options.Value.DlqTopic;
    }

    public async Task PublishAsync(
        string payload,
        Headers originalHeaders,
        string lastErrorMessage,
        CancellationToken cancellationToken)
    {
        var headers = new Headers();
        foreach (var header in originalHeaders)
            headers.Add(header.Key, header.GetValueBytes());
        headers.Add("dlq-reason", Encoding.UTF8.GetBytes(lastErrorMessage));

        var message = new Message<string, string>
        {
            Value = payload,
            Headers = headers
        };

        await _producer.ProduceAsync(_dlqTopic, message, cancellationToken);
    }
}
```

- [ ] **Step 4: Run tests to confirm they pass**

```bash
dotnet test Creditbus.Facade.sln --filter "FullyQualifiedName~KafkaDlqPublisherTests"
```

Expected: 4 tests passed.

- [ ] **Step 5: Commit**

```bash
git add Creditbus.Facade/Shared/Infrastructure/Kafka/KafkaDlqPublisher.cs \
        Creditbus.Facade.Tests/Shared/Infrastructure/Kafka/KafkaDlqPublisherTests.cs
git commit -m "feat: add KafkaDlqPublisher"
```

---

## Task 6: KafkaConsumerBackgroundService (TDD)

**Files:**
- Create: `Creditbus.Facade/Shared/Infrastructure/Kafka/KafkaConsumerBackgroundService.cs`
- Create: `Creditbus.Facade.Tests/Shared/Infrastructure/Kafka/KafkaConsumerBackgroundServiceTests.cs`

- [ ] **Step 1: Write the failing tests**

Create `Creditbus.Facade.Tests/Shared/Infrastructure/Kafka/KafkaConsumerBackgroundServiceTests.cs`:

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
    private readonly IProducer<string, string> _producer;
    private readonly KafkaOptions _options;

    public KafkaConsumerBackgroundServiceTests()
    {
        _consumer = Substitute.For<IConsumer<string, string>>();
        _producer = Substitute.For<IProducer<string, string>>();
        _producer
            .ProduceAsync(Arg.Any<string>(), Arg.Any<Message<string, string>>(), Arg.Any<CancellationToken>())
            .Returns(new DeliveryResult<string, string>());

        _options = new KafkaOptions
        {
            BootstrapServers = "localhost:9092",
            GroupId = "test",
            Topic = "test-topic",
            DlqTopic = "test-dlq",
            Retry = new RetryOptions
            {
                MaxAttempts = 1,   // 1 attempt = no retries, avoids delays in tests
                InitialDelayMs = 0,
                MaxDelayMs = 0,
                JitterFactor = 0
            }
        };
    }

    private KafkaConsumerBackgroundService BuildSut(params IKafkaMessageHandler[] handlers)
    {
        var registry = new KafkaHandlerRegistry(handlers);
        var dlqPublisher = new KafkaDlqPublisher(_producer, Options.Create(_options));
        return new KafkaConsumerBackgroundService(
            _consumer, registry, dlqPublisher,
            Options.Create(_options),
            NullLogger<KafkaConsumerBackgroundService>.Instance);
    }

    [Fact]
    public async Task ProcessMessageAsync_CommitsOffset_WhenHandlerSucceeds()
    {
        var handler = Substitute.For<IKafkaMessageHandler>();
        handler.MessageType.Returns("CardsIngestionEvent");
        handler.HandleAsync(Arg.Any<string>(), Arg.Any<CancellationToken>())
            .Returns(Task.CompletedTask);
        var sut = BuildSut(handler);

        var result = MakeConsumeResult("CardsIngestionEvent", "payload");
        await sut.ProcessMessageAsync(result, CancellationToken.None);

        _consumer.Received(1).Commit(result);
        await _producer.DidNotReceive().ProduceAsync(
            Arg.Any<string>(), Arg.Any<Message<string, string>>(), Arg.Any<CancellationToken>());
    }

    [Fact]
    public async Task ProcessMessageAsync_SendsToDlqAndCommits_WhenHandlerFailsAllRetries()
    {
        var handler = Substitute.For<IKafkaMessageHandler>();
        handler.MessageType.Returns("CardsIngestionEvent");
        handler.HandleAsync(Arg.Any<string>(), Arg.Any<CancellationToken>())
            .Returns(Task.FromException(new Exception("handler failed")));
        var sut = BuildSut(handler);

        var result = MakeConsumeResult("CardsIngestionEvent", "payload");
        await sut.ProcessMessageAsync(result, CancellationToken.None);

        await _producer.Received(1).ProduceAsync(
            _options.DlqTopic,
            Arg.Is<Message<string, string>>(m => m.Value == "payload"),
            Arg.Any<CancellationToken>());
        _consumer.Received(1).Commit(result);
    }

    [Fact]
    public async Task ProcessMessageAsync_SendsToDlqAndCommits_WhenMessageTypeHeaderIsMissing()
    {
        var sut = BuildSut();

        var result = MakeConsumeResultNoHeader("payload");
        await sut.ProcessMessageAsync(result, CancellationToken.None);

        await _producer.Received(1).ProduceAsync(
            _options.DlqTopic,
            Arg.Is<Message<string, string>>(m =>
                m.Headers.Any(h =>
                    h.Key == "dlq-reason" &&
                    Encoding.UTF8.GetString(h.GetValueBytes()).Contains("message-type"))),
            Arg.Any<CancellationToken>());
        _consumer.Received(1).Commit(result);
    }

    [Fact]
    public async Task ProcessMessageAsync_SendsToDlqAndCommits_WhenMessageTypeIsUnrecognized()
    {
        var sut = BuildSut(); // no handlers registered

        var result = MakeConsumeResult("UnknownType", "payload");
        await sut.ProcessMessageAsync(result, CancellationToken.None);

        await _producer.Received(1).ProduceAsync(
            _options.DlqTopic,
            Arg.Any<Message<string, string>>(),
            Arg.Any<CancellationToken>());
        _consumer.Received(1).Commit(result);
    }

    [Fact]
    public async Task ProcessMessageAsync_DoesNotSendToDlq_WhenHandlerSucceedsOnRetry()
    {
        // 2 attempts: first fails, second succeeds — message must NOT go to DLQ
        var options = new KafkaOptions
        {
            BootstrapServers = "localhost:9092",
            GroupId = "test",
            Topic = "test-topic",
            DlqTopic = "test-dlq",
            Retry = new RetryOptions { MaxAttempts = 2, InitialDelayMs = 0, MaxDelayMs = 0, JitterFactor = 0 }
        };

        var handler = Substitute.For<IKafkaMessageHandler>();
        handler.MessageType.Returns("CardsIngestionEvent");
        handler.HandleAsync(Arg.Any<string>(), Arg.Any<CancellationToken>())
            .Returns(
                _ => Task.FromException(new Exception("first attempt fails")),
                _ => Task.CompletedTask);

        var registry = new KafkaHandlerRegistry([handler]);
        var dlqPublisher = new KafkaDlqPublisher(_producer, Options.Create(options));
        var sut = new KafkaConsumerBackgroundService(
            _consumer, registry, dlqPublisher,
            Options.Create(options),
            NullLogger<KafkaConsumerBackgroundService>.Instance);

        var result = MakeConsumeResult("CardsIngestionEvent", "payload");
        await sut.ProcessMessageAsync(result, CancellationToken.None);

        await _producer.DidNotReceive().ProduceAsync(
            Arg.Any<string>(), Arg.Any<Message<string, string>>(), Arg.Any<CancellationToken>());
        _consumer.Received(1).Commit(result);
    }

    private static ConsumeResult<string, string> MakeConsumeResult(string messageType, string payload)
    {
        var headers = new Headers();
        headers.Add("message-type", Encoding.UTF8.GetBytes(messageType));
        return new ConsumeResult<string, string>
        {
            Message = new Message<string, string> { Value = payload, Headers = headers }
        };
    }

    private static ConsumeResult<string, string> MakeConsumeResultNoHeader(string payload) =>
        new()
        {
            Message = new Message<string, string> { Value = payload, Headers = new Headers() }
        };
}
```

- [ ] **Step 2: Run tests to confirm they fail**

```bash
dotnet test Creditbus.Facade.sln --filter "FullyQualifiedName~KafkaConsumerBackgroundServiceTests"
```

Expected: Compilation error — `KafkaConsumerBackgroundService` does not exist yet.

- [ ] **Step 3: Implement KafkaConsumerBackgroundService**

Create `Creditbus.Facade/Shared/Infrastructure/Kafka/KafkaConsumerBackgroundService.cs`:

```csharp
using System.Text;
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
    private readonly KafkaHandlerRegistry _registry;
    private readonly KafkaDlqPublisher _dlqPublisher;
    private readonly string _topic;
    private readonly ResiliencePipeline _pipeline;
    private readonly ILogger<KafkaConsumerBackgroundService> _logger;

    public KafkaConsumerBackgroundService(
        IConsumer<string, string> consumer,
        KafkaHandlerRegistry registry,
        KafkaDlqPublisher dlqPublisher,
        IOptions<KafkaOptions> options,
        ILogger<KafkaConsumerBackgroundService> logger)
    {
        _consumer = consumer;
        _registry = registry;
        _dlqPublisher = dlqPublisher;
        _topic = options.Value.Topic;
        _logger = logger;
        _pipeline = BuildPipeline(options.Value.Retry);
    }

    private static ResiliencePipeline BuildPipeline(RetryOptions options) =>
        new ResiliencePipelineBuilder()
            .AddRetry(new RetryStrategyOptions
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
            })
            .Build();

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        _consumer.Subscribe(_topic);
        try
        {
            while (!stoppingToken.IsCancellationRequested)
            {
                var result = _consumer.Consume(stoppingToken);
                await ProcessMessageAsync(result, stoppingToken);
            }
        }
        finally
        {
            _consumer.Close();
        }
    }

    internal async Task ProcessMessageAsync(
        ConsumeResult<string, string> result,
        CancellationToken cancellationToken)
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
            _consumer.Commit(result);
            return;
        }

        var messageType = Encoding.UTF8.GetString(messageTypeBytes);

        IKafkaMessageHandler handler;
        try
        {
            handler = _registry.Resolve(messageType);
        }
        catch (InvalidOperationException ex)
        {
            _logger.LogWarning(ex, "Unrecognized message type '{MessageType}'. Routing to DLQ.", messageType);
            await _dlqPublisher.PublishAsync(
                result.Message.Value,
                result.Message.Headers,
                ex.Message,
                cancellationToken);
            _consumer.Commit(result);
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

        _consumer.Commit(result);
    }
}
```

- [ ] **Step 4: Run tests to confirm they pass**

```bash
dotnet test Creditbus.Facade.sln --filter "FullyQualifiedName~KafkaConsumerBackgroundServiceTests"
```

Expected: 5 tests passed.

- [ ] **Step 5: Run all tests**

```bash
dotnet test Creditbus.Facade.sln
```

Expected: All tests pass.

- [ ] **Step 6: Commit**

```bash
git add Creditbus.Facade/Shared/Infrastructure/Kafka/KafkaConsumerBackgroundService.cs \
        Creditbus.Facade.Tests/Shared/Infrastructure/Kafka/KafkaConsumerBackgroundServiceTests.cs
git commit -m "feat: add KafkaConsumerBackgroundService with Polly retry and DLQ routing"
```

---

## Task 7: CardsIngestionKafkaHandler

**Files:**
- Create: `Creditbus.Facade/Features/CardsIngestion/Infrastructure/CardsIngestionKafkaHandler.cs`

- [ ] **Step 1: Create the handler stub**

Create `Creditbus.Facade/Features/CardsIngestion/Infrastructure/CardsIngestionKafkaHandler.cs`:

```csharp
using Creditbus.Facade.Shared.Infrastructure.Kafka;
using Microsoft.Extensions.Logging;

namespace Creditbus.Facade.Features.CardsIngestion.Infrastructure;

public sealed class CardsIngestionKafkaHandler : IKafkaMessageHandler
{
    private readonly ILogger<CardsIngestionKafkaHandler> _logger;

    public CardsIngestionKafkaHandler(ILogger<CardsIngestionKafkaHandler> logger)
    {
        _logger = logger;
    }

    public string MessageType => "CardsIngestionEvent";

    public Task HandleAsync(string payload, CancellationToken cancellationToken)
    {
        _logger.LogInformation("Received CardsIngestionEvent: {Payload}", payload);
        return Task.CompletedTask;
    }
}
```

- [ ] **Step 2: Verify build**

```bash
dotnet build Creditbus.Facade.sln
```

Expected: Build succeeded with 0 errors.

- [ ] **Step 3: Commit**

```bash
git add Creditbus.Facade/Features/CardsIngestion/Infrastructure/CardsIngestionKafkaHandler.cs
git commit -m "feat: add CardsIngestionKafkaHandler stub"
```

---

## Task 8: KafkaServiceExtensions and Program.cs wiring

**Files:**
- Create: `Creditbus.Facade/Shared/Infrastructure/Kafka/KafkaServiceExtensions.cs`
- Modify: `Creditbus.Facade/Program.cs`

- [ ] **Step 1: Create KafkaServiceExtensions**

Create `Creditbus.Facade/Shared/Infrastructure/Kafka/KafkaServiceExtensions.cs`:

```csharp
using Confluent.Kafka;
using Creditbus.Facade.Features.CardsIngestion.Infrastructure;
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

        services.AddSingleton<IConsumer<string, string>>(sp =>
        {
            var options = sp.GetRequiredService<IOptions<KafkaOptions>>().Value;
            var config = new ConsumerConfig
            {
                BootstrapServers = options.BootstrapServers,
                GroupId = options.GroupId,
                AutoOffsetReset = AutoOffsetReset.Earliest,
                EnableAutoCommit = false
            };
            return new ConsumerBuilder<string, string>(config).Build();
        });

        services.AddSingleton<IKafkaMessageHandler, CardsIngestionKafkaHandler>();
        services.AddSingleton<KafkaHandlerRegistry>();
        services.AddSingleton<KafkaDlqPublisher>();
        services.AddHostedService<KafkaConsumerBackgroundService>();

        return services;
    }
}
```

- [ ] **Step 2: Update Program.cs**

Replace the full content of `Creditbus.Facade/Program.cs` with:

```csharp
using Creditbus.Facade.Shared.Infrastructure.Kafka;

var builder = WebApplication.CreateBuilder(args);

builder.Services.AddEndpointsApiExplorer();
builder.Services.AddSwaggerGen();
builder.Services.AddKafkaInfrastructure(builder.Configuration);

var app = builder.Build();

if (app.Environment.IsDevelopment())
{
    app.UseSwagger();
    app.UseSwaggerUI();
}

app.UseHttpsRedirection();

app.Run();
```

- [ ] **Step 3: Build and run all tests**

```bash
dotnet build Creditbus.Facade.sln
dotnet test Creditbus.Facade.sln
```

Expected: Build succeeded. All tests pass.

- [ ] **Step 4: Commit**

```bash
git add Creditbus.Facade/Shared/Infrastructure/Kafka/KafkaServiceExtensions.cs \
        Creditbus.Facade/Program.cs
git commit -m "feat: wire Kafka consumer infrastructure into DI"
```
