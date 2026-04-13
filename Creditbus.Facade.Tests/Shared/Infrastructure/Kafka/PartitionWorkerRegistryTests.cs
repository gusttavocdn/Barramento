using Confluent.Kafka;
using Creditbus.Facade.Shared.Infrastructure.Kafka;
using FluentAssertions;
using NSubstitute;

namespace Creditbus.Facade.Tests.Shared.Infrastructure.Kafka
{
	public class PartitionWorkerRegistryTests
	{
		private static TopicPartition Partition(int id) => new("test-topic", new Partition(id));
		private static IPartitionWorker FakeWorker() => Substitute.For<IPartitionWorker>();
		private readonly PartitionWorkerRegistry _sut = new();

		[Fact]
		public void TryGet_ReturnsWorker_AfterAdd()
		{
			var partition = Partition(0);
			var worker = FakeWorker();

			_sut.Add(partition, worker);
			_sut.TryGet(partition, out var result).Should().BeTrue();
			result.Should().BeSameAs(worker);
		}

		[Fact]
		public void Try_GetReturnsFalse_WhenPartitionNotRegistered()
		{
			_sut.TryGet(Partition(99), out var result).Should().BeFalse();
			result.Should().BeNull();
		}

		[Fact]
		public void Remove_ReturnsNull_WhenPartititionNotRegistered()
		{
			_sut.Remove(Partition(99)).Should().BeNull();
		}

		[Fact]
		public void Add_ThrownInvalidOperationException_OnDuplicatePartition()
		{
			var partition = Partition(0);
			_sut.Add(partition, FakeWorker());

			var act = () => _sut.Add(partition, FakeWorker());

			act.Should().Throw<InvalidOperationException>().WithMessage("*already registered*");
		}

		[Fact]
		public void GetAll_ReturnsAllRegestedWorkers()
		{
			var w0 = FakeWorker();
			var w1 = FakeWorker();
			_sut.Add(Partition(0), w0);
			_sut.Add(Partition(1), w1);

			_sut.GetAll().Should().BeEquivalentTo(new[] { w0, w1 });
		}
	}
}