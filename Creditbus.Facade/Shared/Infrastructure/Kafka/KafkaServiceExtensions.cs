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