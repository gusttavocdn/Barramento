using Creditbus.Facade.Shared.Infrastructure.Kafka;
using FluentAssertions;
using NSubstitute;

namespace Creditbus.Facade.Tests.Shared.Infrastructure.Kafka;

public class KafkaConsumerRegistryTests
{
    [Fact]
    public void Resolve_ReturnsCorrectHandler_WhenMessageTypeIsRegistered()
    {
        var handler = Substitute.For<IKafkaMessageConsumer>();
        handler.MessageType.Returns("CardsIngestionEvent");
        var registry = new KafkaConsumerRegistry([handler]);

        var result = registry.Resolve("CardsIngestionEvent");

        result.Should().Be(handler);
    }

    [Fact]
    public void Resolve_IsCaseInsensitive()
    {
        var handler = Substitute.For<IKafkaMessageConsumer>();
        handler.MessageType.Returns("CardsIngestionEvent");
        var registry = new KafkaConsumerRegistry([handler]);

        var result = registry.Resolve("cardsingestionevent");

        result.Should().Be(handler);
    }

    [Fact]
    public void Resolve_Throws_WhenMessageTypeIsNotRegistered()
    {
        var registry = new KafkaConsumerRegistry([]);

        var act = () => registry.Resolve("UnknownType");

        act.Should().Throw<InvalidOperationException>()
            .WithMessage("*UnknownType*");
    }

    [Fact]
    public void Constructor_Throws_WhenDuplicateMessageTypesAreRegistered()
    {
        var handler1 = Substitute.For<IKafkaMessageConsumer>();
        handler1.MessageType.Returns("CardsIngestionEvent");
        var handler2 = Substitute.For<IKafkaMessageConsumer>();
        handler2.MessageType.Returns("CardsIngestionEvent");

        var act = () => new KafkaConsumerRegistry([handler1, handler2]);

        act.Should().Throw<InvalidOperationException>()
            .WithMessage("*CardsIngestionEvent*");
    }
}
