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

    // Constrói o pipeline de retry com backoff exponencial + jitter.
    // Jitter = variação aleatória no delay para evitar que múltiplas instâncias
    // do serviço tentem novamente ao mesmo tempo e sobrecarreguem o sistema.
    internal static ResiliencePipeline BuildPipeline(RetryOptions options)
    {
        var builder = new ResiliencePipelineBuilder();

        if (options.MaxAttempts > 1)
        {
            builder.AddRetry(new RetryStrategyOptions
            {
                // MaxRetryAttempts = tentativas extras após a primeira. Ex: MaxAttempts=3 → 1 + 2 retries.
                MaxRetryAttempts = options.MaxAttempts - 1,
                ShouldHandle = new PredicateBuilder().Handle<Exception>(),
                DelayGenerator = args =>
                {
                    // Backoff exponencial: cada tentativa dobra o tempo de espera (2^tentativa).
                    // Limitado por MaxDelayMs para não crescer indefinidamente.
                    double baseMs = Math.Min(
                        options.InitialDelayMs * Math.Pow(2, args.AttemptNumber),
                        options.MaxDelayMs);

                    // Jitter: adiciona/remove até JitterFactor% do delay base aleatoriamente.
                    double jitter = (Random.Shared.NextDouble() * 2 - 1) * baseMs * options.JitterFactor;
                    return ValueTask.FromResult<TimeSpan?>(TimeSpan.FromMilliseconds(baseMs + jitter));
                }
            });
        }

        return builder.Build();
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        // Registra interesse no tópico. Os callbacks de partição (OnAssigned/OnRevoked)
        // configurados em KafkaServiceExtensions são disparados a partir daqui.
        _consumer.Subscribe(_topic);
        try
        {
            while (!stoppingToken.IsCancellationRequested)
            {
                // Bloqueante até chegar uma mensagem. Leve — não processa nada aqui,
                // só recebe e repassa para o worker da partição correspondente.
                var result = _consumer.Consume(stoppingToken);

                // Situação anormal: partição atribuída mas sem worker no registry.
                // Pode ocorrer em race condition durante rebalanceamento.
                if (!_workerRegistry.TryGet(result.TopicPartition, out var worker))
                {
                    _logger.LogError(
                        "No worker registered for partition {Partition}. Message at offset {Offset} will be skipped.",
                        result.TopicPartition, result.Offset);
                    continue;
                }

                // Enfileira no channel do worker. Se o channel estiver cheio (back-pressure),
                // aguarda aqui até abrir espaço — sem perder mensagens.
                await worker!.EnqueueAsync(result, stoppingToken);
            }
        }
        finally
        {
            // Garante que o consumer se desregistre do grupo ao encerrar,
            // liberando as partições para outros consumers do mesmo GroupId.
            _consumer.Close();
        }
    }
}
