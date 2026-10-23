namespace MarketDataEngine;

public record MarketTick(
    string Symbol,
    decimal Price,
    long Volume,
    DateTime Timestamp
);

public class Candle
{
    public string Symbol { get; set; } = string.Empty;
    public DateTime Time { get; set; }
    public decimal Open { get; set; }
    public decimal High { get; set; }
    public decimal Low { get; set; }
    public decimal Close { get; set; }
    public long Volume { get; set; }
}
