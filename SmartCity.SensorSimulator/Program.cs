using SmartCity.SensorSimulator;
using SmartCity.SensorSimulator.Configuration;
using SmartCity.SensorSimulator.Services;

var builder = Host.CreateApplicationBuilder(args);

// Connect to Aspire's OpenTelemetry pipeline
builder.AddServiceDefaults();

builder.Services.Configure<ProducerOptions>(builder.Configuration.GetSection("producer"));

builder.Services.AddHostedService<KafkaTopicInitializer>();

builder.Services.AddSingleton<TrafficDataGenerator>();
builder.Services.AddSingleton<KafkaProducerService>();
builder.Services.AddHostedService<Worker>();

var host = builder.Build();
host.Run();
