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