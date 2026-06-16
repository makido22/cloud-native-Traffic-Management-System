using MassTransit;
using SmartCity.Contracts.Commands;
using SmartCity.ServiceDefaults;

namespace SmartCity.TrafficLightController.Consumers;

/// <summary>
/// Processes ChangeTrafficLightCommand messages from RabbitMQ.
/// Updates the intersection state and simulates hardware switching time.
/// </summary>
public class ChangeTrafficLightConsumer : IConsumer<ChangeTrafficLightCommand>
{
    private readonly ILogger<ChangeTrafficLightConsumer> _logger;
    private readonly IntersectionStateStore _stateStore;

    public ChangeTrafficLightConsumer(
        ILogger<ChangeTrafficLightConsumer> logger,
        IntersectionStateStore stateStore)
    {
        _logger = logger;
        _stateStore = stateStore;
    }

    public async Task Consume(ConsumeContext<ChangeTrafficLightCommand> context)
    {
        var cmd = context.Message;

        _logger.LogInformation(
            "📥 COMMAND RECEIVED: Intersection {Id} ({Direction}) → {Color} | Reason: {Reason}",
            cmd.IntersectionId, cmd.Direction, cmd.TargetColor, cmd.Reason);

        if (await _stateStore.IsLockedAsync(cmd.IntersectionId, context.CancellationToken))
        {
            _logger.LogWarning(
                "🔒 REJECTED: Intersection {Id} is locked by emergency override.",
                cmd.IntersectionId);
            return;
        }

        // Simulate hardware switching delay
        await Task.Delay(300, context.CancellationToken);

        // Persist to PostgreSQL (with optimistic concurrency)
        var success = await _stateStore.UpdateStateAsync(
            cmd.IntersectionId,
            cmd.Direction,
            cmd.TargetColor,
            cmd.Reason,
            false,
            context.CancellationToken);

        if (success)
        {
            var latency = DateTime.UtcNow - cmd.IssuedAt;
            _logger.LogInformation(
                "✅ LIGHT CHANGED: Intersection {Id} → {Color} | Latency: {Latency}ms",
                cmd.IntersectionId, cmd.TargetColor, latency.TotalMilliseconds);
        }
        else
        {
            _logger.LogError(
                "FAILED to update Intersection {Id}. State store rejected the change.",
                cmd.IntersectionId);

            // Throw to trigger MassTransit retry
            throw new InvalidOperationException(
                $"Failed to persist state change for intersection {cmd.IntersectionId}");
        }
    }
}