using MassTransit;
using SmartCity.DashboardService.Consumers;
using SmartCity.DashboardService.Hubs;

var builder = WebApplication.CreateBuilder(args);

// Add services to the container.
// Learn more about configuring OpenAPI at https://aka.ms/aspnet/openapi

// Connect to Aspire's OpenTelemetry pipeline
builder.AddServiceDefaults();

builder.Services.AddSignalR();

builder.Services.AddMassTransit(x =>
{
    x.AddConsumer<TrafficStateChangedConsumer>();

    x.UsingRabbitMq((context, cfg) =>
    {
        var connectionString = builder.Configuration.GetConnectionString("messaging")
            ?? throw new InvalidOperationException("RabbitMQ connection string not found.");

        cfg.Host(new Uri(connectionString));

        cfg.ReceiveEndpoint("dashboard-state-changes", e =>
        {
            e.PrefetchCount = 100;  // dashboard updates are lightweight
            e.ConfigureConsumer<TrafficStateChangedConsumer>(context);
        });
    });
});

// CORS so the browser can connect to the SignalR hub
builder.Services.AddCors(options =>
{
    options.AddDefaultPolicy(policy =>
    {
        policy.AllowAnyHeader()
              .AllowAnyMethod()
              .SetIsOriginAllowed(_ => true)  // dev only — restrict in production
              .AllowCredentials();             // required for SignalR
    });
});

var app = builder.Build();

app.MapDefaultEndpoints();

app.UseCors();

// Serve the static HTML dashboard
app.UseDefaultFiles();
app.UseStaticFiles();

app.MapHub<TrafficHub>("/hubs/traffic");

app.Run();
