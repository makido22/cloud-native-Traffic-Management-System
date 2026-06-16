using SmartCity.Contracts.Events;
using System.Text.Json;
using Confluent.Kafka;
using System.Threading.Channels;
using System.Text;

namespace SmartCity.SensorSimulator.Services;

public class KafkaProducerService : IDisposable
{
    // Byte serialization is faster and avoids the string→bytes (<string, string>)
    // double-conversion librdkafka would otherwise do.
    private readonly IProducer<byte[], byte[]> _producer;

    // Pre-compute all 20 key byte arrays ONCE at startup
    private readonly Dictionary<int, byte[]> _keyCache;

    private readonly ILogger<KafkaProducerService> _logger;

    private readonly Channel<Message<byte[], byte[]>> _retryChannel;
    private readonly Channel<string> _statsChannel;
    
    private const string TopicName = "traffic-telemetry";
    private const string DeadLetterTopic = "traffic-telemetry-dlq";
    private const int MaxRetryAttempts = 3;

    public KafkaProducerService(IConfiguration configuration, ILogger<KafkaProducerService> logger)
    {
        _logger = logger;
        _retryChannel = Channel.CreateBounded<Message<byte[], byte[]>>(10_000);
        _statsChannel = Channel.CreateBounded<string>(5);

        // Build the cache: intersection 101-120 → their UTF-8 key bytes
        _keyCache = Enumerable.Range(101, 20)
            .ToDictionary(id => id, id => Encoding.UTF8.GetBytes(id.ToString()));

        var bootstrapServers = configuration.GetConnectionString("streaming")
            ?? throw new InvalidOperationException("Kafka connection string not found.");

        var config = new ProducerConfig
        {
            BootstrapServers = bootstrapServers,
            ClientId = "sensor-simulator",
            Acks = Acks.All,

            // The broker discards the duplicate message so it isn't written to the log twice 
            // When a broker receives a sequence number, it evaluates it against the last recorded sequence number
            // for that Producer ID:If Incoming Sequence == Last Sequence + 1: Perfect.
            // The broker accepts and saves the message [1].If Incoming Sequence <= Last Sequence: Duplicate detected.
            // The broker drops the message but sends a success acknowledgement [1].
            // If Incoming Sequence > Last Sequence + 1: Gap detected. This means a message was lost in transit.
            // The broker rejects the request with an out-of-order sequence exception [1].
            EnableIdempotence = true,

            // max.in.flight.requests.per.connection = 5 batches = 5 requests
            // idempotence is on, so It attaches sequence numbers to batches,
            // automatically discarding duplicates and rearranging out-of-order batches
            // for strict exactly-once semantics,
            // if batch 1 fails, batch 2 and 3,... will be rejected by broker
            // broker recognizes them as out-of-sequence and rejects them with an error
            // producer buffers Batches 2 and 3,... internally in memory
            MaxInFlight = 5,
            
            MessageSendMaxRetries = 3,
            RetryBackoffMs = 100,
            CompressionType = CompressionType.Lz4, // generally the fastest compression algorithm supported by Apache Kafka
            LingerMs = 50,
            BatchSize = 1024 * 1024, // 1MB batches
            
            // Total memory the producer can buffer before Produce() blocks
            QueueBufferingMaxKbytes = 1024 * 1024,   // 1GB buffer
            QueueBufferingMaxMessages = 2_000_000,   // allow huge in-memory queue

            StatisticsIntervalMs = 10000
        };

        _producer = new ProducerBuilder<byte[], byte[]>(config)
            .SetStatisticsHandler((_, json) =>
            {
                _statsChannel.Writer.TryWrite(json);
            })
            .Build();

        // Start the background Librdkafka internal statistics processor
        _ = ProcessStats();
        logger.LogInformation("ProcessStatsChannel is running");

        // Start the background retry processor
        _ = ProcessRetryChannel();
        logger.LogInformation("ProcessRetryChannel is running");
    }

    public void Publish(TrafficDataReceived data)
    {
        var valuebytes = JsonSerializer.SerializeToUtf8Bytes(data);

        var message = new Message<byte[], byte[]>
        {
            Key = GetKeyBytes(data.IntersectionId),
            Value = valuebytes,
            Timestamp = new Timestamp(data.Timestamp)
        };

        ProduceWithRetryTracking(message, attempt: 0);
    }

    private void ProduceWithRetryTracking(Message<byte[], byte[]> message, int attempt)
    {
        _producer.Produce(TopicName, message, report =>
        {
            if (report.Error.Code == ErrorCode.NoError)
            {
                _logger.LogDebug(
                    "Delivered to {Topic}[{Partition}@{Offset}]",
                    report.Topic, report.Partition.Value, report.Offset.Value);
                return;
            }

            // FAILURE: librdkafka exhausted its internal retries.
            _logger.LogWarning(
                "Delivery failed (attempt {Attempt}): {Reason}",
                attempt, report.Error.Reason);

            if (IsRetryable(report.Error, attempt) && attempt < MaxRetryAttempts)
            {
                // Push to the retry channel (don't retry on the librdkafka thread, which is this callback)
                _retryChannel.Writer.TryWrite(message);
            }
            else
            {
                // All retries exhausted. Send to Dead Letter Queue.
                SendToDeadLetterQueue(message, report.Error.Reason);
            }
        });
    }

    /// <summary>
    /// Background loop that processes failed messages from the retry channel.
    /// Runs on a separate thread, NOT on the librdkafka callback thread.
    /// </summary>
    private async Task ProcessRetryChannel()
    {
        await foreach (var message in _retryChannel.Reader.ReadAllAsync())
        {
            // Extract current attempt from headers (or increment a counter)
            var currentAttempt = GetRetryAttempt(message) + 1;

            // Wait before retrying (exponential backoff)
            var delay = TimeSpan.FromMilliseconds(500 * Math.Pow(2, currentAttempt - 1));
            await Task.Delay(delay);

            SetRetryAttempt(message, currentAttempt);

            _logger.LogInformation("Retrying message (attempt {Attempt})", currentAttempt);
            ProduceWithRetryTracking(message, currentAttempt);
        }
    }

    /// <summary>
    /// Messages that permanently fail go here.
    /// They can be inspected, debugged, and replayed later.
    /// </summary>
    private void SendToDeadLetterQueue(Message<byte[], byte[]> message, string errorReason)
    {
        _logger.LogError(
            "Message permanently failed. Sending to DLQ. Reason: {Reason}",
            errorReason);

        // Add error metadata to the message headers
        message.Headers ??= new Headers();
        message.Headers.Add("error-reason", Encoding.UTF8.GetBytes(errorReason));
        message.Headers.Add("failed-at", Encoding.UTF8.GetBytes(DateTime.UtcNow.ToString("O")));

        _producer.Produce(DeadLetterTopic, message, dlqReport =>
        {
            if (dlqReport.Error.Code != ErrorCode.NoError)
            {
                // If even the DLQ fails, log critically. Human must intervene.
                _logger.LogCritical(
                    "CRITICAL: Failed to write to DLQ! Message lost. Reason: {Reason}",
                    dlqReport.Error.Reason);
            }
        });
    }

    private int GetRetryAttempt(Message<byte[], byte[]> message)
    {
        var header = message.Headers?.FirstOrDefault(h => h.Key == "retry-attempt");
        if (header is null) return 0;
        return int.Parse(Encoding.UTF8.GetString(header.GetValueBytes()));
    }

    private void SetRetryAttempt(Message<byte[], byte[]> message, int attempt)
    {
        message.Headers ??= new Headers();
        message.Headers.Remove("retry-attempt");
        message.Headers.Add("retry-attempt", Encoding.UTF8.GetBytes(attempt.ToString()));
    }

    private static bool IsRetryable(Error error, int attempt)
    {
        return error.Code switch
        {
            ErrorCode.LeaderNotAvailable => true,
            ErrorCode.RequestTimedOut => true,
            ErrorCode.NotEnoughReplicas => true,
            ErrorCode.NetworkException => true,

            // These will NEVER succeed no matter how many times we retry
            ErrorCode.TopicException => false,
            ErrorCode.MsgSizeTooLarge => false,
            ErrorCode.TopicAuthorizationFailed => false,
            ErrorCode.InvalidMsg => false,

            // Unknown errors: retry once, then give up
            _ => attempt < 1
        };
    }

    private async Task ProcessStats()
    {
        await foreach (var json in _statsChannel.Reader.ReadAllAsync())
        {
            try
            {
                var stats = JsonDocument.Parse(json);
                var root = stats.RootElement;

                // Total messages produced by application
                var totalMessages = root.GetProperty("txmsgs").GetInt64();

                // Total produce REQUESTS sent to Kafka (network round trips)
                var totalRequests = root.GetProperty("tx").GetInt64();

                // Batch size stats (per broker)
                var brokers = root.GetProperty("brokers");
                foreach (var broker in brokers.EnumerateObject())
                {
                    if (!broker.Value.TryGetProperty("batchsize", out var batchSize) ||
                        !broker.Value.TryGetProperty("batchcnt", out var batchCount))
                    {
                        continue;
                    }
                    var avgBatchBytes = batchSize.GetProperty("avg").GetInt64();
                    var maxBatchBytes = batchSize.GetProperty("max").GetInt64();

                    var avgMsgsPerBatch = batchCount.GetProperty("avg").GetInt64();
                    var maxMsgsPerBatch = batchCount.GetProperty("max").GetInt64();

                    _logger.LogInformation(
                        "BATCH STATS [{Broker}]: " +
                        "Avg msgs/batch: {AvgCount} | Max msgs/batch: {MaxCount} | " +
                        "Avg batch size: {AvgBytes} bytes | Max batch size: {MaxBytes} bytes",
                        broker.Name,
                        avgMsgsPerBatch, maxMsgsPerBatch,
                        avgBatchBytes, maxBatchBytes);
                }

                _logger.LogInformation(
                    "📊 NETWORK EFFICIENCY: {Messages} messages sent in {Requests} requests " +
                    "(ratio: {Ratio:F1} msgs/request)",
                    totalMessages, totalRequests,
                    totalRequests > 0 ? (double)totalMessages / totalRequests : 0);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Failed to parse Kafka stats");
            }
        }
    }

    private byte[] GetKeyBytes(int intersectionId) => _keyCache[intersectionId];

    public void Dispose()
    {
        _retryChannel.Writer.Complete();
        _statsChannel.Writer.Complete();
        _producer?.Flush(TimeSpan.FromSeconds(10));
        _producer?.Dispose();
    }
}