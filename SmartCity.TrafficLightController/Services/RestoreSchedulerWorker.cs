using SmartCity.TrafficLightController.Models;

namespace SmartCity.TrafficLightController.Services;

/// <summary>
/// Background service that polls the Redis Sorted Set every second for due
/// restoration jobs. When a job is due, it releases the Redis lock and
/// restores the intersection to Normal mode in PostgreSQL.
/// </summary>
public class RestoreSchedulerWorker(
    RedisDelayedScheduler scheduler,
    RedisLockService lockService,
    IntersectionStateStore stateStore,
    ILogger<RestoreSchedulerWorker> logger) : BackgroundService
{
    private readonly RedisDelayedScheduler _scheduler = scheduler;
    private readonly RedisLockService _lockService = lockService;
    private readonly ILogger<RestoreSchedulerWorker> _logger = logger;
    private readonly IntersectionStateStore _stateStore = stateStore;
    private static readonly TimeSpan PollInterval = TimeSpan.FromSeconds(1);

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        _logger.LogInformation("RestoreSchedulerWorker started. Polling every {Sec}s.",
            PollInterval.TotalSeconds);

        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                var dueJobs = await _scheduler.ClaimDueJobsAsync(batchSize: 100);

                foreach (var job in dueJobs)
                {
                    await ProcessRestoreJob(job, stoppingToken);
                }
            }
            catch (Exception ex)
            {
                // Never let the worker crash — log and continue polling
                _logger.LogError(ex, "Error during restore polling cycle.");
            }

            await Task.Delay(PollInterval, stoppingToken);
        }

        _logger.LogInformation("RestoreSchedulerWorker stopping.");
    }

    private async Task ProcessRestoreJob(RestoreJob job, CancellationToken cancellationToken)
    {
        _logger.LogInformation(
            "RESTORE FIRING: Intersection {Id} (was locked by {Vehicle})",
            job.IntersectionId, job.VehicleId);

        // Release the Redis lock (Lua compare-and-delete — only if still ours)
        await _lockService.ReleaseLockAsync(job.IntersectionId, job.VehicleId);

        await _stateStore.RestoreNormalModeAsync(job.IntersectionId, cancellationToken);

        _logger.LogInformation(
            "RESTORED: Intersection {Id} returned to Normal mode.",
            job.IntersectionId);
    }
}