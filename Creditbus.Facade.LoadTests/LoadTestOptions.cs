namespace Creditbus.Facade.LoadTests;

public record LoadTestOptions(
    int Rate,
    int Workers,
    TimeSpan? Duration,
    string Broker,
    string Topic
)
{
    private const int DefaultRate = 100;
    private const int DefaultWorkers = 4;
    private const string DefaultBroker = "localhost:9092";
    private const string DefaultTopic = "creditbus.ingestion";

    public static LoadTestOptions Parse(string[] args)
    {
        var rate = DefaultRate;
        var workers = DefaultWorkers;
        TimeSpan? duration = null;
        var broker = DefaultBroker;
        var topic = DefaultTopic;

        for (var i = 0; i < args.Length; i++)
        {
            switch (args[i])
            {
                case "--rate":
                    RequireValue(args, i);
                    if (!int.TryParse(args[i + 1], out rate) || rate <= 0)
                        throw new ArgumentException($"--rate deve ser um inteiro positivo. Recebido: '{args[i + 1]}'");
                    i++;
                    break;
                case "--workers":
                    RequireValue(args, i);
                    if (!int.TryParse(args[i + 1], out workers) || workers <= 0)
                        throw new ArgumentException($"--workers deve ser um inteiro positivo. Recebido: '{args[i + 1]}'");
                    i++;
                    break;
                case "--duration":
                    RequireValue(args, i);
                    duration = ParseDuration(args[i + 1]);
                    i++;
                    break;
                case "--broker":
                    RequireValue(args, i);
                    broker = args[i + 1];
                    i++;
                    break;
                case "--topic":
                    RequireValue(args, i);
                    topic = args[i + 1];
                    i++;
                    break;
                default:
                    if (args[i].StartsWith("--"))
                        throw new ArgumentException($"Argumento desconhecido: '{args[i]}'");
                    break;
            }
        }

        return new LoadTestOptions(rate, workers, duration, broker, topic);
    }

    private static void RequireValue(string[] args, int flagIndex)
    {
        if (flagIndex + 1 >= args.Length || args[flagIndex + 1].StartsWith("--"))
            throw new ArgumentException($"O argumento '{args[flagIndex]}' requer um valor.");
    }

    private static TimeSpan ParseDuration(string value)
    {
        if (value.EndsWith('s') && int.TryParse(value[..^1], out var seconds))
            return TimeSpan.FromSeconds(seconds);
        if (value.EndsWith('m') && int.TryParse(value[..^1], out var minutes))
            return TimeSpan.FromMinutes(minutes);
        if (value.EndsWith('h') && int.TryParse(value[..^1], out var hours))
            return TimeSpan.FromHours(hours);
        throw new ArgumentException($"Formato de duração inválido: '{value}'. Use ex: 30s, 2m, 1h");
    }

    public static void PrintUsage()
    {
        Console.WriteLine("""
            Uso: dotnet run -- [opções]

            Opções:
              --rate     <int>     Mensagens por segundo        (padrão: 100)
              --workers  <int>     Workers paralelos            (padrão: 4)
              --duration <string>  Duração ex: 30s, 2m, 1h     (padrão: roda até Ctrl+C)
              --broker   <string>  Bootstrap server Kafka       (padrão: localhost:9092)
              --topic    <string>  Tópico Kafka                 (padrão: creditbus.ingestion)

            Exemplos:
              dotnet run -- --rate 500 --workers 8 --duration 2m
              dotnet run -- --rate 1000 --broker kafka:9092 --topic meu-topico
            """);
    }
}
