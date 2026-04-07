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
