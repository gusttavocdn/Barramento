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

        // Producer: usado apenas para publicar mensagens na DLQ em caso de falha.
        services.AddSingleton<IProducer<string, string>>(sp =>
        {
            var options = sp.GetRequiredService<IOptions<KafkaOptions>>().Value;
            var config = new ProducerConfig { BootstrapServers = options.BootstrapServers };
            return new ProducerBuilder<string, string>(config).Build();
        });

        // Registry criado antes do consumer pois os callbacks de partição abaixo precisam dele.
        services.AddSingleton<PartitionWorkerRegistry>();

        services.AddSingleton<IConsumer<string, string>>(sp =>
        {
            var options = sp.GetRequiredService<IOptions<KafkaOptions>>().Value;
            var workerRegistry = sp.GetRequiredService<PartitionWorkerRegistry>();
            var handlerRegistry = sp.GetRequiredService<KafkaConsumerRegistry>();
            var dlqPublisher = sp.GetRequiredService<KafkaDlqPublisher>();
            var loggerFactory = sp.GetRequiredService<ILoggerFactory>();

            // Pipeline compartilhado entre todos os workers — é stateless, pode ser reutilizado.
            var pipeline = KafkaConsumerBackgroundService.BuildPipeline(options.Retry);

            var config = new ConsumerConfig
            {
                BootstrapServers = options.BootstrapServers,
                GroupId = options.GroupId,
                AutoOffsetReset = AutoOffsetReset.Earliest,  // Earliest: ao entrar no grupo pela primeira vez, começa do início do tópico.
                // AutoCommit: o client envia o offset ao broker a cada AutoCommitIntervalMs.
                // Só commita o que foi explicitamente marcado via StoreOffset — nunca avança sozinho.
                EnableAutoCommit = true,
                AutoCommitIntervalMs = 5000,
                // Desabilita o avanço automático de offset. StoreOffset() no PartitionWorker
                // é quem controla o que pode ser commitado.
                EnableAutoOffsetStore = false
            };

            return new ConsumerBuilder<string, string>(config)
                // Disparado pelo Kafka quando partições são atribuídas a este consumer
                // (na primeira conexão ou após rebalanceamento do grupo).
                // Para cada partição recebida: cria um worker dedicado e o inicia.
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
                // Disparado quando partições são revogadas (ex: novo consumer entrou no grupo).
                // StopAsync() drena o channel antes de retornar — garante que nenhuma mensagem
                // em buffer seja perdida antes de a partição ser entregue a outro consumer.
                // GetAwaiter().GetResult() porque este callback é síncrono na API do Kafka.
                .SetPartitionsRevokedHandler((_, partitions) =>
                {
                    foreach (var tpo in partitions)
                    {
                        var worker = workerRegistry.Remove(tpo.TopicPartition);
                        worker?.StopAsync().GetAwaiter().GetResult();
                    }
                })
                .Build();
        });

        services.AddScoped<IProcessCardEventUseCase, ProcessCardEventUseCase>();
        // Cada novo IKafkaMessageHandler registrado aqui é detectado automaticamente
        // pelo KafkaHandlerRegistry via injeção de IEnumerable<IKafkaMessageHandler>.
        services.AddSingleton<IKafkaMessageConsumer, CardsIngestionKafkaConsumer>();
        services.AddSingleton<KafkaConsumerRegistry>();
        services.AddSingleton<KafkaDlqPublisher>();
        services.AddHostedService<KafkaConsumerBackgroundService>();

        return services;
    }
}
