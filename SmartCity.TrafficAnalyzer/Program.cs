using MassTransit;
using SmartCity.TrafficAnalyzer;
using SmartCity.TrafficAnalyzer.Services;

var builder = Host.CreateApplicationBuilder(args);

// Connect to Aspire's OpenTelemetry pipeline
builder.AddServiceDefaults();

builder.Services.AddMassTransit(x =>
{
    x.UsingRabbitMq((context, cfg) =>
    {
        // Aspire injects the connection string via the name "messaging"
        var connectionString = builder.Configuration.GetConnectionString("messaging")
            ?? throw new InvalidOperationException("RabbitMQ connection string 'messaging' not found.");

        cfg.Host(new Uri(connectionString));

        // Configure outgoing message topology
        cfg.Message<SmartCity.Contracts.Commands.ChangeTrafficLightCommand>(m =>
        {
            m.SetEntityName("traffic-light-commands");
        });
    });
});

builder.Services.AddSingleton<TrafficAnalysisService>();
builder.Services.AddHostedService<KafkaConsumerServiceWorker>();

var host = builder.Build();
host.Run();
