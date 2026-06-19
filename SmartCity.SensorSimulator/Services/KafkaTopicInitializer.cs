using Confluent.Kafka;
using Confluent.Kafka.Admin;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SmartCity.SensorSimulator.Services
{
    /// <summary>
    /// BackgroundService is not suited, because it is executed asynchronously, TOpic must be created and configured before application runs.
    /// BackGroundService runs ExecuteAsync inside StartAsync. code below. 
    /// public virtual Task StartAsync(CancellationToken cancellationToken)
    ///  {
    ///  Create linked token to allow cancelling executing task from provided token
    ///    _stoppingCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);

    ///  Execute all of ExecuteAsync asynchronously, and store the task we're executing so that we can wait for it later.
    ///    _executeTask = Task.Run(() => ExecuteAsync(_stoppingCts.Token), _stoppingCts.Token);

    ///  Always return a completed task.  Any result from ExecuteAsync will be handled by the Host.
    ///    return Task.CompletedTask;
    ///  }
    /// </summary>
    public class KafkaTopicInitializer(IConfiguration config, ILogger<KafkaTopicInitializer> logger) : IHostedService
    {
        private readonly IConfiguration _config = config;
        private readonly ILogger<KafkaTopicInitializer> _logger = logger;

        public async Task StartAsync(CancellationToken cancellationToken)
        {
            var bootstrapServers = _config.GetConnectionString("streaming")
                ?? throw new InvalidOperationException("Kafka connection string not found.");

            using var adminClient = new AdminClientBuilder(
                new AdminClientConfig { BootstrapServers = bootstrapServers }).Build();

            var topics = new[]
            {
                new TopicSpecification
                {
                    Name = "traffic-telemetry",
                    NumPartitions = 4,          // <-- partition count
                    ReplicationFactor = 1        // <-- single broker in dev
                },
                new TopicSpecification
                {
                    Name = "traffic-telemetry-dlq",
                    NumPartitions = 1,
                    ReplicationFactor = 1
                }
            };

            try
            {
                _logger.LogInformation("EXECUTING Create traffic-telemetry Topic.");
                await adminClient.CreateTopicsAsync(topics);
                _logger.LogInformation("traffic-telemetry Topic is created.");
            }
            catch (CreateTopicsException ex)
            {
                // Topic already exists - that's fine on restart
                foreach (var result in ex.Results)
                {
                    if (result.Error.Code == ErrorCode.TopicAlreadyExists)
                        _logger.LogInformation("Topic {Topic} already exists.", result.Topic);
                    else
                        _logger.LogError("Failed to create {Topic}: {Reason}",
                            result.Topic, result.Error.Reason);
                }
            }
        }

        public Task StopAsync(CancellationToken cancellationToken) => Task.CompletedTask;
    }
}
