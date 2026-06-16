using Confluent.Kafka;
using Confluent.Kafka.Admin;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SmartCity.SensorSimulator.Services
{
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
