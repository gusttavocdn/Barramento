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
