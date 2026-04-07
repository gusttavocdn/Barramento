using System.Text.Json;
using System.Text.Json.Serialization;
using Creditbus.Facade.Features.CardsIngestion.Application.Contracts;
using FluentAssertions;

namespace Creditbus.Facade.Tests.Features.CardsIngestion.Application;

public class CardHolderStatusConverterTests
{
    private static readonly JsonSerializerOptions Options = new() { PropertyNameCaseInsensitive = true };

    private record StatusWrapper(
        [property: JsonConverter(typeof(CardHolderStatusConverter))] string Status);

    private static string Deserialize(string json)
        => JsonSerializer.Deserialize<StatusWrapper>(json, Options)!.Status;

    [Fact]
    public void Read_ReturnsEnumName_WhenValueIsValidInt()
    {
        var result = Deserialize("""{"status": 0}""");
        result.Should().Be("Eligible");
    }

    [Fact]
    public void Read_ReturnsEnumName_WhenValueIsNonZeroInt()
    {
        var result = Deserialize("""{"status": 1}""");
        result.Should().Be("Active");
    }

    [Fact]
    public void Read_ReturnsEnumName_WhenValueIsValidString()
    {
        var result = Deserialize("""{"status": "Active"}""");
        result.Should().Be("Active");
    }

    [Fact]
    public void Read_IsCaseInsensitive_WhenValueIsString()
    {
        var result = Deserialize("""{"status": "active"}""");
        result.Should().Be("Active");
    }

    [Fact]
    public void Read_ThrowsJsonException_WhenIntValueIsOutOfRange()
    {
        var act = () => Deserialize("""{"status": 9999}""");
        act.Should().Throw<JsonException>().WithMessage("*9999*");
    }

    [Fact]
    public void Read_ThrowsJsonException_WhenStringValueIsInvalid()
    {
        var act = () => Deserialize("""{"status": "InvalidStatus"}""");
        act.Should().Throw<JsonException>().WithMessage("*InvalidStatus*");
    }
}
