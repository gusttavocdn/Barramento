using Confluent.Kafka;

namespace Creditbus.Facade.Shared.Infrastructure.Kafka;

// Mapeia cada partição Kafka atribuída a este consumer para seu worker dedicado.
// Usado pelo BackgroundService para rotear mensagens e pelo KafkaServiceExtensions
// para criar/remover workers durante rebalanceamento.
public sealed class PartitionWorkerRegistry
{
    private readonly Dictionary<TopicPartition, IPartitionWorker> _workers = new();

    // lock garante que Add/Remove/TryGet sejam thread-safe:
    // o BackgroundService (reader) e os callbacks de rebalanceamento (writer)
    // podem acessar o dicionário em threads diferentes simultaneamente.
    private readonly object _lock = new();

    public void Add(TopicPartition partition, IPartitionWorker worker)
    {
        lock (_lock)
        {
            if (!_workers.TryAdd(partition, worker))
                throw new InvalidOperationException($"Worker already registered for partition {partition}");
        }
    }

    public IPartitionWorker? Remove(TopicPartition partition)
    {
        lock (_lock)
        {
            _workers.Remove(partition, out var worker);
            return worker;
        }
    }

    public bool TryGet(TopicPartition partition, out IPartitionWorker? worker)
    {
        lock (_lock)
        {
            return _workers.TryGetValue(partition, out worker);
        }
    }

    public IReadOnlyCollection<IPartitionWorker> GetAll()
    {
        lock (_lock)
        {
            // ToList() cria uma cópia para que o caller não segure o lock
            // enquanto itera sobre a coleção.
            return _workers.Values.ToList();
        }
    }
}
