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
