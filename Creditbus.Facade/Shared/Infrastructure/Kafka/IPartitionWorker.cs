using Confluent.Kafka;

namespace Creditbus.Facade.Shared.Infrastructure.Kafka
{
	public interface IPartitionWorker
	{
		void Start();
		Task StopAsync();
		ValueTask EnqueueAsync(ConsumeResult<string, string> result, CancellationToken cancellationToken);
	}
}