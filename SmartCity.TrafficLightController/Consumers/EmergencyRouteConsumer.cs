using MassTransit;
using SmartCity.Contracts.Commands;
using SmartCity.ServiceDefaults;
using SmartCity.TrafficLightController.Services;
using System.Diagnostics;

namespace SmartCity.TrafficLightController.Consumers;

public class EmergencyRouteConsumer(
    ILogger<EmergencyRouteConsumer> logger,
    IntersectionStateStore stateStore,
    RedisLockService lockService,
    RedisDelayedScheduler scheduler) : IConsumer<EmergencyRouteCommand>
{
    private readonly ILogger<EmergencyRouteConsumer> _logger = logger;
    private readonly IntersectionStateStore _stateStore = stateStore;
    private readonly RedisLockService _lockService = lockService;
    private readonly RedisDelayedScheduler _scheduler = scheduler;
    private static readonly TimeSpan EmergencyLockDuration = TimeSpan.FromSeconds(60);

    public async Task Consume(ConsumeContext<EmergencyRouteCommand> context)
    {
        var cmd = context.Message;

        using var activity = Telemetry.ActivitySource.StartActivity("ProcessEmergency");
        activity?.SetTag("intersection.id", cmd.IntersectionId);
        activity?.SetTag("vehicle.id", cmd.VehicleId);
        activity?.SetTag("priority", cmd.RoutePriority);
        Telemetry.EmergencyRequests.Add(1,
            new KeyValuePair<string, object?>("intersection", cmd.IntersectionId));

        _logger.LogWarning(
            "🚨 EMERGENCY: {Vehicle} requesting GREEN at Intersection {Id}",
            cmd.VehicleId, cmd.IntersectionId);

        using (var lockSpan = Telemetry.ActivitySource.StartActivity("AcquireRedisLock"))
        {
            var lockAcquired = await _lockService.TryAcquireLockAsync(
                cmd.IntersectionId,
                cmd.VehicleId,
                EmergencyLockDuration);

            lockSpan?.SetTag("lock.acquired", lockAcquired);

            if (!lockAcquired)
            {
                activity?.SetTag("result", "already_locked");

                var holder = await _lockService.GetLockHolderAsync(cmd.IntersectionId);                
                _logger.LogWarning(
                    "🚦 Intersection {Id} already locked by {Holder}. {Vehicle} can proceed (already GREEN).",
                    cmd.IntersectionId, holder, cmd.VehicleId);
                return;
            }
        } 

        var success = await _stateStore.UpdateStateAsync(
            intersectionId: cmd.IntersectionId,
            direction: cmd.Direction,
            targetColor: "GREEN",
            reason: $"Emergency override for {cmd.VehicleId}",
            isEmergency: true,
            cancellationToken: context.CancellationToken);

        if (!success)
        {
            await _lockService.ReleaseLockAsync(cmd.IntersectionId, cmd.VehicleId);

            activity?.SetStatus(ActivityStatusCode.Error, "Persistence failed");

            // Framework Defaults (MassTransit):
            // unhandled exceptions trigger automatic message retries, and if all retries fail,
            // in this case 2 instant retries,
            // it creates a Fault<T> message or sends the message to a Error/Dead-Letter Queue.
            throw new InvalidOperationException(
                $"Failed to persist emergency state for intersection {cmd.IntersectionId}");
        }

        await _scheduler.ScheduleRestoreAsync(
            cmd.IntersectionId,
            cmd.VehicleId,
            EmergencyLockDuration);

        var endToEndLatency = (DateTime.UtcNow - cmd.IssuedAt).TotalMilliseconds;

        Telemetry.EmergencyLatency.Record(endToEndLatency,
            new KeyValuePair<string, object?>("intersection", cmd.IntersectionId));

        activity?.SetTag("latency.ms", endToEndLatency);
        activity?.SetStatus(ActivityStatusCode.Ok);

        _logger.LogWarning(
            "EMERGENCY CLEARED: Intersection {Id} → GREEN for {Vehicle} | " +
            "Latency: {Latency:F1}ms | Auto-restore in {Seconds}s",
            cmd.IntersectionId, cmd.VehicleId, endToEndLatency, EmergencyLockDuration.TotalSeconds);
    }
}