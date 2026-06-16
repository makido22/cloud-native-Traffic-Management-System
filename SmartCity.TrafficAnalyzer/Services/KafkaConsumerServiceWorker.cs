using Confluent.Kafka;
using SmartCity.Contracts.Events;
using System.Text.Json;
using static System.Runtime.InteropServices.JavaScript.JSType;

namespace SmartCity.TrafficAnalyzer.Services;

public class KafkaConsumerServiceWorker : BackgroundService
{
    private readonly ILogger<KafkaConsumerServiceWorker> _logger;
    private readonly IConfiguration _configuration;
    private readonly TrafficAnalysisService _analyzer;
    private const string TopicName = "traffic-telemetry";
    private const string ConsumerGroupId = "traffic-analyzer-group";

    public KafkaConsumerServiceWorker(
        ILogger<KafkaConsumerServiceWorker> logger,
        IConfiguration configuration,
        TrafficAnalysisService analyzer)
    {
        _logger = logger;
        _configuration = configuration;
        _analyzer = analyzer;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var bootstrapServers = _configuration.GetConnectionString("streaming")
            ?? throw new InvalidOperationException("Kafka connection string not found.");

        var config = new ConsumerConfig
        {
            BootstrapServers = bootstrapServers,
            GroupId = ConsumerGroupId,

            // CRITICAL: Start from the EARLIEST message if no offset exists.
            // This proves "zero data loss" - if the consumer was offline,
            // it picks up exactly where it left off.
            AutoOffsetReset = AutoOffsetReset.Earliest,

            // 1. librdkafka handles network commits on a background thread
            EnableAutoCommit = true,
            AutoCommitIntervalMs = 5000,
            // 2. BUT, we manually control WHEN a message is marked as "done"
            EnableAutoOffsetStore = false,

            FetchMinBytes = 1024 * 1024, // 1 MB
            FetchWaitMaxMs = 50,

            // Heartbeat
            // The maximum time the Kafka broker waits to hear from a consumer before
            // assuming it is dead and kicking it out of the group.
            SessionTimeoutMs = 10000,
            // session timeouts
            // How frequently the consumer sends a background "ping" (heartbeat)
            // to the broker to prove it is still alive
            HeartbeatIntervalMs = 3000
        };

        using var consumer = new ConsumerBuilder<byte[], byte[]>(config)
            .SetErrorHandler((_, e) => _logger.LogError("Kafka error: {Reason}", e.Reason))
            .SetPartitionsAssignedHandler((_, partitions) =>
            {
                _logger.LogInformation(
                    "Assigned partitions: [{Partitions}]",
                    string.Join(", ", partitions.Select(p => p.Partition.Value)));
            })
            .Build();

        consumer.Subscribe(TopicName);
        _logger.LogInformation("Kafka Consumer subscribed to {Topic}", TopicName);

        // Run the consume loop on a background task to avoid (*)blocking
        await Task.Run(async () =>
        {
            try
            {
                while (!stoppingToken.IsCancellationRequested)
                {
                    try
                    {
                        var consumeResult = consumer.Consume(TimeSpan.FromMilliseconds(500));
                        // (*)blocking happens here if no message.
                        if (consumeResult is null) continue;

                        await ProcessMessage(consumeResult.Message.Value, stoppingToken);

                        // OPTIMIZATION: Store the offset in local memory.
                        // The background thread will commit this to the broker later.
                        // This is ~10,000x faster than consumer.Commit()
                        consumer.StoreOffset(consumeResult);
                    }
                    catch (ConsumeException ex)
                    {
                        _logger.LogError(ex, "Consume error: {Reason}", ex.Error.Reason);
                    }
                    catch (Exception ex)
                    {
                        _logger.LogError(ex, "Unexpected error processing message");
                    }
                }
            }
            finally
            {
                // Close() ensures any stored offsets are committed before shutting down
                consumer.Close();
                _logger.LogInformation("Kafka Consumer closed cleanly");
            }
        }, stoppingToken);
    }

    private async Task ProcessMessage(byte[] payload, CancellationToken cancellationToken)
    {
        try
        {
            // Deserialize directly from UTF-8 bytes. 
            // This skips the expensive string allocation entirely.
            // JsonSerializer implicitly wraps it in a ReadOnlySpan<byte>
            // TrafficDataReceived object instance is allocated
            // JSON text block itself never touches the managed heap
            var data = JsonSerializer.Deserialize<TrafficDataReceived>(payload);
            if (data is null)
            {
                _logger.LogWarning("Received null/invalid message: {Json}", data);
                return;
            }

            await _analyzer.AnalyzeAsync(data, cancellationToken);
        }
        catch (JsonException ex)
        {
            _logger.LogError(ex, "Failed to deserialize message");
        }
    }
}