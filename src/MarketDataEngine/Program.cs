using MarketDataEngine;

var builder = Host.CreateApplicationBuilder(args);
builder.Services.AddHostedService<MarketWorker>();

var host = builder.Build();
host.Run();
