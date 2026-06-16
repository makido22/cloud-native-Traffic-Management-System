using Microsoft.Extensions.Options;
using SmartCity.SensorSimulator.Configuration;
using SmartCity.SensorSimulator.Services;

namespace SmartCity.SensorSimulator;

public class Worker(
    KafkaProducerService producer,
    TrafficDataGenerator generator,
    ILogger<Worker> logger,
    IOptions<ProducerOptions> config) : BackgroundService
{
    private readonly KafkaProducerService _producer = producer;
    private readonly TrafficDataGenerator _generator = generator;
    private readonly ILogger<Worker> _logger = logger;
    private readonly IOptions<ProducerOptions> _config = config;

    protected override async Task ExecuteAsync(CancellationToken ct)
    {
        var tasks = Enumerable.Range(0, _config.Value.ProducerThreads)
            .Select(workerId => RunProducerLoop(workerId, ct));

        await Task.WhenAll(tasks);
    }

    private async Task RunProducerLoop(int workerId, CancellationToken ct)
    {
        var perWorkerRate = _config.Value.TargetMessagesPerSecond / _config.Value.ProducerThreads;
        var delayMs = TimeSpan.FromSeconds(1.0 / perWorkerRate);

        _logger.LogInformation(
            "Sensor Simulator Thread {threadId} started. Generating {Rate} messages/sec",
            Environment.CurrentManagedThreadId, _config.Value.TargetMessagesPerSecond);

        var totalSent = 0L;
        var startTime = DateTime.UtcNow;

        while (!ct.IsCancellationRequested)
        {
            var data = _generator.Generate();
            _producer.Publish(data);
            totalSent++;

            // Log progress every 1000 messages
            if (totalSent % 5000 == 0)
            {
                var elapsed = (DateTime.UtcNow - startTime).TotalSeconds;
                var rate = totalSent / elapsed;
                _logger.LogInformation(
                    "Thread: {threadId}. Total sent: {Total} | Avg rate: {Rate:F0} msg/sec",
                    Environment.CurrentManagedThreadId, totalSent, rate);
            }

            await Task.Delay(delayMs, ct);
        }

        _logger.LogInformation("Sensor Simulator stopped. Total messages sent: {Total}", totalSent);
    }
}