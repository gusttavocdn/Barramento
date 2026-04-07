using System.Text.Json;
using System.Text.Json.Serialization;
using Creditbus.Facade.Features.CardsIngestion.Domain;

namespace Creditbus.Facade.Features.CardsIngestion.Application.Contracts;

public sealed class CardHolderStatusConverter : JsonConverter<string>
{
    public override string Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options)
    {
        if (reader.TokenType == JsonTokenType.Number)
        {
            var value = reader.GetInt32();
            if (!Enum.IsDefined(typeof(CardHolderStatus), value))
                throw new JsonException($"Invalid CardHolderStatus integer value: {value}.");
            return ((CardHolderStatus)value).ToString();
        }

        if (reader.TokenType == JsonTokenType.String)
        {
            var value = reader.GetString()!;
            if (!Enum.TryParse<CardHolderStatus>(value, ignoreCase: true, out var result))
                throw new JsonException($"Invalid CardHolderStatus string value: '{value}'.");
            return result.ToString();
        }

        throw new JsonException($"Unexpected token type for CardHolderStatus: {reader.TokenType}.");
    }

    public override void Write(Utf8JsonWriter writer, string value, JsonSerializerOptions options)
        => writer.WriteStringValue(value);
}
