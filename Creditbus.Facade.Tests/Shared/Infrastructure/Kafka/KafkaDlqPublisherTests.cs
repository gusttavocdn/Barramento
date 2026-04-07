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
