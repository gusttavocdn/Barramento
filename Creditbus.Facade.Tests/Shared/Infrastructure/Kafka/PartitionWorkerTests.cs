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