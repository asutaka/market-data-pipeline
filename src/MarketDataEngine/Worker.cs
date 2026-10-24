using Confluent.Kafka;
using ClickHouse.Client.ADO;
using System.Text.Json;

namespace MarketDataEngine;

public class MarketWorker : BackgroundService
{
    private readonly ILogger<MarketWorker> _logger;
    private readonly string _kafkaBootstrap;
    private readonly string _clickhouseConnStr;
    private const string Topic = "market-ticks";

    public MarketWorker(ILogger<MarketWorker> logger, IConfiguration config)
    {
        _logger = logger;
        _kafkaBootstrap = config["KAFKA_BOOTSTRAP"] ?? "my-cluster-kafka-bootstrap.kafka.svc.cluster.local:9092";
        _clickhouseConnStr = config["CLICKHOUSE_CONN"] ?? "Host=chi-clickhouse-cluster-cluster-0-0.clickhouse.svc.cluster.local;Port=8123;User=admin;Password=ClickHouse@123;Database=default";
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        _logger.LogInformation("🚀 MarketDataEngine .NET 8 Started!");
        _logger.LogInformation("📍 Kafka Bootstrap: {Bootstrap}", _kafkaBootstrap);

        var producerTask = RunProducerAsync(stoppingToken);
        var consumerTask = RunConsumerAsync(stoppingToken);

        await Task.WhenAll(producerTask, consumerTask);
    }

    private async Task RunProducerAsync(CancellationToken ct)
    {
        var config = new ProducerConfig
        {
            BootstrapServers = _kafkaBootstrap,
            Acks = Acks.Leader,
            LingerMs = 5
        };

        using var producer = new ProducerBuilder<string, string>(config).Build();
        var symbols = new[] { "FPT", "HPG", "VCB", "SSI", "MWG" };
        var random = new Random();

        while (!ct.IsCancellationRequested)
        {
            try
            {
                var symbol = symbols[random.Next(symbols.Length)];
                var basePrice = symbol switch { "FPT" => 135.0m, "HPG" => 28.5m, "VCB" => 92.0m, "SSI" => 36.0m, _ => 50.0m };
                var tick = new MarketTick(
                    symbol,
                    Math.Round(basePrice + (decimal)(random.NextDouble() * 2 - 1), 2),
                    random.Next(100, 5000),
                    DateTime.UtcNow
                );

                var json = JsonSerializer.Serialize(tick);
                await producer.ProduceAsync(Topic, new Message<string, string> { Key = symbol, Value = json }, ct);
                _logger.LogInformation("📤 [Kafka Producer] Sent: {Symbol} at {Price}k VND | Vol: {Vol}", tick.Symbol, tick.Price, tick.Volume);
            }
            catch (Exception ex)
            {
                _logger.LogWarning("Producer warning: {Msg}", ex.Message);
            }

            await Task.Delay(1000, ct);
        }
    }

    private async Task RunConsumerAsync(CancellationToken ct)
    {
        // Cho producer chay truoc 2 giay de topic co san du lieu
        await Task.Delay(2000, ct);

        var config = new ConsumerConfig
        {
            BootstrapServers = _kafkaBootstrap,
            GroupId = "candle-aggregator-group",
            AutoOffsetReset = AutoOffsetReset.Earliest,
            EnableAutoCommit = true
        };

        using var consumer = new ConsumerBuilder<string, string>(config).Build();
        consumer.Subscribe(Topic);

        using var connection = new ClickHouseConnection(_clickhouseConnStr);
        await connection.OpenAsync(ct);

        while (!ct.IsCancellationRequested)
        {
            try
            {
                var cr = consumer.Consume(TimeSpan.FromSeconds(2));
                if (cr?.Message?.Value != null)
                {
                    var tick = JsonSerializer.Deserialize<MarketTick>(cr.Message.Value);
                    if (tick != null)
                    {
                        using var cmd = connection.CreateCommand();
                        cmd.CommandText = $"INSERT INTO stock_candles (symbol, time, open, high, low, close, volume) VALUES ('{tick.Symbol}', now(), {tick.Price}, {tick.Price}, {tick.Price}, {tick.Price}, {tick.Volume})";
                        await cmd.ExecuteNonQueryAsync(ct);

                        _logger.LogInformation("💾 [ClickHousezzz] Saved Candle: {Symbol} Price={Price} Vol={Vol}", tick.Symbol, tick.Price, tick.Volume);
                    }
                }
            }
            catch (Exception ex)
            {
                _logger.LogWarning("Consumer warning: {Msg}", ex.Message);
                await Task.Delay(2000, ct);
            }
        }
    }
}
