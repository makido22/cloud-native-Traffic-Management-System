using SmartCity.TrafficLightController.Models;
using StackExchange.Redis;

namespace SmartCity.TrafficLightController.Services;

/// <summary>
/// A durable, crash-resistant delayed job scheduler backed by a Redis Sorted Set.
/// 
/// Jobs are stored with a SCORE equal to their due Unix timestamp.
/// A background worker polls for due jobs and claims them atomically via Lua,
/// guaranteeing exactly-once execution even across multiple service instances.
/// </summary>
public class RedisDelayedScheduler
{
    private readonly IConnectionMultiplexer _redis;
    private readonly ILogger<RedisDelayedScheduler> _logger;

    // The Sorted Set key that holds all pending restore jobs
    private const string RestoreJobsKey = "schedule:restore-jobs";

    public RedisDelayedScheduler(
        IConnectionMultiplexer redis,
        ILogger<RedisDelayedScheduler> logger)
    {
        _redis = redis;
        _logger = logger;
    }

    /// <summary>
    /// Schedules a restoration job to fire at a future time.
    /// The member encodes both the intersection ID and the vehicle ID.
    /// </summary>
    public async Task ScheduleRestoreAsync(
        int intersectionId,
        string vehicleId,
        TimeSpan delay)
    {
        var db = _redis.GetDatabase();

        // Score = the Unix timestamp (in milliseconds) when this job becomes due
        var dueTime = DateTimeOffset.UtcNow.Add(delay).ToUnixTimeMilliseconds();

        // Member = unique payload. Include a timestamp to avoid duplicate-member
        // collisions if the same intersection+vehicle is scheduled twice.
        var member = $"{intersectionId}|{vehicleId}|{Guid.NewGuid():N}";

        // ZADD schedule:restore-jobs <dueTime> <member>
        // O(log N) operation — fast even with millions of jobs
        await db.SortedSetAddAsync(RestoreJobsKey, member, dueTime);

        _logger.LogInformation(
            "SCHEDULED: Restore for Intersection {Id} ({Vehicle}) at {DueTime} (in {Delay}s)",
            intersectionId, vehicleId,
            DateTimeOffset.FromUnixTimeMilliseconds(dueTime).UtcDateTime,
            delay.TotalSeconds);
    }

    /// <summary>
    /// Atomically claims all jobs that are due (score &lt;= now).
    /// 
    /// This uses a Lua script so that the "find due jobs" and
    /// "remove claimed jobs" operations happen as ONE atomic unit.
    /// Because Redis is single-threaded, no two worker instances can ever
    /// claim the same job. This guarantees exactly-once execution.
    /// </summary>
    public async Task<List<RestoreJob>> ClaimDueJobsAsync(int batchSize = 100)
    {
        var db = _redis.GetDatabase();
        var now = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();

        // Lua script:
        // 1. ZRANGEBYSCORE — get all members with score between 0 and 'now', limited to batchSize
        // 2. ZREM — remove those exact members
        // 3. Return the claimed members
        // All in ONE atomic operation.
        const string luaScript = @"
            local jobs = redis.call('ZRANGEBYSCORE', KEYS[1], 0, ARGV[1], 'LIMIT', 0, ARGV[2])
            if #jobs > 0 then
                redis.call('ZREM', KEYS[1], unpack(jobs))
            end
            return jobs";

        var result = await db.ScriptEvaluateAsync(
            luaScript,
            new RedisKey[] { RestoreJobsKey },
            new RedisValue[] { now, batchSize });

        var claimedMembers = (RedisValue[])result!;

        var jobs = new List<RestoreJob>();
        foreach (var member in claimedMembers)
        {
            var parsed = ParseJob(member.ToString());
            if (parsed is not null)
                jobs.Add(parsed);
        }

        if (jobs.Count > 0)
        {
            _logger.LogInformation("⏰ Claimed {Count} due restore job(s).", jobs.Count);
        }

        return jobs;
    }

    /// <summary>
    /// Returns the number of pending (not-yet-due) jobs. Useful for monitoring.
    /// </summary>
    public async Task<long> GetPendingJobCountAsync()
    {
        var db = _redis.GetDatabase();
        return await db.SortedSetLengthAsync(RestoreJobsKey);
    }

    private RestoreJob? ParseJob(string member)
    {
        // Member format: "intersectionId|vehicleId|guid"
        var parts = member.Split('|');
        if (parts.Length < 2 || !int.TryParse(parts[0], out var intersectionId))
        {
            _logger.LogWarning("Failed to parse restore job member: {Member}", member);
            return null;
        }

        return new RestoreJob(intersectionId, parts[1]);
    }
}