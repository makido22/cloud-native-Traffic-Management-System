var builder = DistributedApplication.CreateBuilder(args);

// 1. PostgreSQL - Persistent storage for intersection states
var postgres = builder.AddPostgres("postgres-server")
    .WithPgAdmin()                    // Adds a web UI to inspect the DB
    .AddDatabase("TrafficDb");        // Creates a logical database

// 2. Redis - High-speed state cache (TTL locks, rate limiting)
// Adds a web UI to inspect Redis keys
var redis = builder.AddRedis("cache")
    .WithRedisCommander();            

// 3. RabbitMQ - Command & Control message broker
// Adds the RabbitMQ Management UI
var rabbitmq = builder.AddRabbitMQ("messaging")
    .WithManagementPlugin();

// 4. Kafka - High-throughput telemetry stream broker
// Adds a web UI to inspect topics and messages
var kafka = builder.AddKafka("streaming")
    .WithKafkaUI()
    .WithDataVolume(); // <-- attach a persistent named Docker volume             

builder.AddProject<Projects.SmartCity_SensorSimulator>("sensor-simulator")
    .WithReference(kafka);

builder.AddProject<Projects.SmartCity_TrafficAnalyzer>("traffic-analyzer")
    .WithReference(kafka)
    .WithReference(rabbitmq)
    .WithReference(redis)
    .WithReplicas(4);

builder.AddProject<Projects.SmartCity_TrafficLightController>("light-controller")
    .WithReference(rabbitmq)
    .WithReference(postgres)
    .WaitFor(postgres)
    .WithReference(redis);

var emergencyApi = builder.AddProject<Projects.SmartCity_EmergencyAPI>("emergency-api")
    .WithReference(rabbitmq)
    .WaitFor(rabbitmq)
    .WithReference(redis);

var dashboard = builder.AddProject<Projects.SmartCity_DashboardService>("dashboard")
    .WithReference(rabbitmq)
    .WaitFor(rabbitmq);

builder.AddProject<Projects.SmartCity_Gateway>("gateway")
    .WithReference(redis)            // for distributed rate limiting
    .WithReference(emergencyApi)     // enables service discovery for routing
    .WithReference(dashboard)
    .WaitFor(emergencyApi)
    .WithReplicas(3)
    .WithExternalHttpEndpoints();    // ONLY the gateway is exposed externally

builder.Build().Run();