using MassTransit;
using Microsoft.EntityFrameworkCore;
using SmartCity.TrafficLightController;
using SmartCity.TrafficLightController.Consumers;
using SmartCity.TrafficLightController.Data;
using SmartCity.TrafficLightController.Services;
using StackExchange.Redis;

var builder = Host.CreateApplicationBuilder(args);

builder.AddServiceDefaults();

builder.Services.AddDbContext<TrafficDbContext>(options =>
{
    var connectionString = builder.Configuration.GetConnectionString("TrafficDb")
        ?? throw new InvalidOperationException("PostgreSQL connection string 'TrafficDb' not found.");

    options.UseNpgsql(connectionString, npgsqlOptions =>
    {
        // Retry on transient database errors
        npgsqlOptions.EnableRetryOnFailure(
            maxRetryCount: 3,
            maxRetryDelay: TimeSpan.FromSeconds(5),
            errorCodesToAdd: null);
    });
});

builder.Services.AddHostedService<DatabaseInitializer>();

builder.Services.AddSingleton<IntersectionStateStore>();

builder.AddRedisClient("cache");
builder.Services.AddSingleton<RedisLockService>();
builder.Services.AddSingleton<RedisDelayedScheduler>();
builder.Services.AddHostedService<RestoreSchedulerWorker>();

builder.Services.AddMassTransit(x =>
{
    x.AddConsumer<ChangeTrafficLightConsumer>();
    x.AddConsumer<EmergencyRouteConsumer>();

    x.UsingRabbitMq((context, cfg) =>
    {
        var connectionString = builder.Configuration.GetConnectionString("messaging")
            ?? throw new InvalidOperationException("RabbitMQ connection string 'messaging' not found.");

        cfg.Host(new Uri(connectionString));

        cfg.ReceiveEndpoint("light-controller", e =>
        {
            e.UseMessageRetry(r =>
            {
                r.Incremental(
                    retryLimit: 3,
                    initialInterval: TimeSpan.FromMilliseconds(500),
                    intervalIncrement: TimeSpan.FromMilliseconds(500));

                // Only retry on transient errors, not business logic errors
                r.Ignore<ArgumentException>();
            });

            e.ConcurrentMessageLimit = 5;
            e.PrefetchCount = 50;

            // CIRCUIT BREAKER (Advanced Fault Tolerance)
            // The circuit evaluates failures in a rolling 1-minute window (TrackingPeriod).
            // It will only "trip" (stops the consumer from receiving or processing any more messages from the RabbitMQ queue)
            // if both of these conditions are met:
            // Condition 1:  totalProcessed >= ActiveThreshold
            // Condition 2:  (totalFailed / totalProcessed) * 100 >= TripThreshold
            e.UseCircuitBreaker(cb =>
            {
                cb.TrackingPeriod = TimeSpan.FromMinutes(1);
                cb.TripThreshold = 15; 
                cb.ActiveThreshold = 10;
                cb.ResetInterval = TimeSpan.FromMinutes(5); // pause for 5 minutes
            });

            // Register the consumer
            e.ConfigureConsumer<ChangeTrafficLightConsumer>(context);
        });

        cfg.ReceiveEndpoint("light-controller-emergency", e =>
        {
            // Enable RabbitMQ priority queue (0-10 priority levels)
            e.SetQueueArgument("x-max-priority", 10);

            // CRITICAL: Prefetch = 1 ensures emergencies are never stuck
            // behind locally-buffered messages
            e.PrefetchCount = 1;
            e.ConcurrentMessageLimit = 10; // Process multiple emergencies in parallel

            // Emergencies should retry aggressively but quickly
            e.UseMessageRetry(r =>
            {
                r.Immediate(2); // 2 instant retries (no delay for emergencies)
            });

            e.ConfigureConsumer<EmergencyRouteConsumer>(context);
        });

        // Configure the message topology to match the publisher
        cfg.Message<SmartCity.Contracts.Commands.ChangeTrafficLightCommand>(m =>
        {
            m.SetEntityName("traffic-light-commands");
        });

        cfg.Message<SmartCity.Contracts.Commands.EmergencyRouteCommand>(m =>
        {
            m.SetEntityName("emergency-route-commands");
        });
    });
});

var host = builder.Build();
host.Run();
