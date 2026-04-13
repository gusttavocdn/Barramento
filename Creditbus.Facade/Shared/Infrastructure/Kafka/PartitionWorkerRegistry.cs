using Confluent.Kafka;

namespace Creditbus.Facade.Shared.Infrastructure.Kafka
{
	public sealed class PartitionWorkerRegistry
	{
		private readonly Dictionary<TopicPartition, IPartitionWorker> _workers = new();
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
				return _workers.Values.ToList();
			}
		}
	}
}