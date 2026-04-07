using Creditbus.Facade.Shared.Infrastructure.Kafka;
using FluentAssertions;
using NSubstitute;

namespace Creditbus.Facade.Tests.Shared.Infrastructure.Kafka;

public class KafkaHandlerRegistryTests
{
    [Fact]
    public void Resolve_ReturnsCorrectHandler_WhenMessageTypeIsRegistered()
    {
        var handler = Substitute.For<IKafkaMessageHandler>();
        handler.MessageType.Returns("CardsIngestionEvent");
        var registry = new KafkaHandlerRegistry([handler]);

        var result = registry.Resolve("CardsIngestionEvent");

        result.Should().Be(handler);
    }

    [Fact]
    public void Resolve_IsCaseInsensitive()
    {
        var handler = Substitute.For<IKafkaMessageHandler>();
        handler.MessageType.Returns("CardsIngestionEvent");
        var registry = new KafkaHandlerRegistry([handler]);

        var result = registry.Resolve("cardsingestionevent");

        result.Should().Be(handler);
    }

    [Fact]
    public void Resolve_Throws_WhenMessageTypeIsNotRegistered()
    {
        var registry = new KafkaHandlerRegistry([]);

        var act = () => registry.Resolve("UnknownType");

        act.Should().Throw<InvalidOperationException>()
            .WithMessage("*UnknownType*");
    }

    [Fact]
    public void Constructor_Throws_WhenDuplicateMessageTypesAreRegistered()
    {
        var handler1 = Substitute.For<IKafkaMessageHandler>();
        handler1.MessageType.Returns("CardsIngestionEvent");
        var handler2 = Substitute.For<IKafkaMessageHandler>();
        handler2.MessageType.Returns("CardsIngestionEvent");

        var act = () => new KafkaHandlerRegistry([handler1, handler2]);

        act.Should().Throw<InvalidOperationException>()
            .WithMessage("*CardsIngestionEvent*");
    }
}
