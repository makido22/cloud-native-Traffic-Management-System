using StackExchange.Redis;

namespace SmartCity.TrafficLightController.Services;

/// <summary>
/// Manages distributed intersection locks using Redis.
/// Provides atomic lock acquisition with automatic TTL expiry.
/// </summary>
public class RedisLockService
{
    private readonly IConnectionMultiplexer _redis;
    private readonly ILogger<RedisLockService> _logger;

    private static readonly TimeSpan DefaultLockDuration = TimeSpan.FromSeconds(60);

    public RedisLockService(
        IConnectionMultiplexer redis,
        ILogger<RedisLockService> logger)
    {
        _redis = redis;
        _logger = logger;
    }

    private static string LockKey(int intersectionId) => $"Intersection:{intersectionId}:Lock";

    /// <summary>
    /// Attempts to acquire an emergency lock on an intersection.
    /// Returns true if the lock was acquired, false if already locked.
    /// This is ATOMIC — only one caller can ever win.
    /// </summary>
    public async Task<bool> TryAcquireLockAsync(
        int intersectionId,
        string vehicleId,
        TimeSpan? duration = null)
    {
        var db = _redis.GetDatabase();
        var key = LockKey(intersectionId);
        var ttl = duration ?? DefaultLockDuration;

        // SET key value NX EX <seconds>
        // When = When.NotExists is the "NX" flag (atomic acquire)
        var acquired = await db.StringSetAsync(
            key: key,
            value: vehicleId,
            expiry: ttl,
            when: When.NotExists);

        if (acquired)
        {
            _logger.LogInformation(
                "LOCK ACQUIRED: Intersection {Id} locked by {Vehicle} for {Seconds}s",
                intersectionId, vehicleId, ttl.TotalSeconds);
        }
        else
        {
            var currentHolder = await db.StringGetAsync(key);
            _logger.LogWarning(
                "LOCK DENIED: Intersection {Id} already locked by {Holder}",
                intersectionId, currentHolder);
        }

        return acquired;
    }

    /// <summary>
    /// Checks if an intersection is currently locked.
    /// </summary>
    public async Task<bool> IsLockedAsync(int intersectionId)
    {
        var db = _redis.GetDatabase();
        return await db.KeyExistsAsync(LockKey(intersectionId));
    }

    /// <summary>
    /// Returns the vehicle ID currently holding the lock, or null if unlocked.
    /// </summary>
    public async Task<string?> GetLockHolderAsync(int intersectionId)
    {
        var db = _redis.GetDatabase();
        var value = await db.StringGetAsync(LockKey(intersectionId));
        return value.HasValue ? value.ToString() : null;
    }

    /// <summary>
    /// Returns remaining lock time in seconds, or null if unlocked.
    /// </summary>
    public async Task<double?> GetRemainingLockTimeAsync(int intersectionId)
    {
        var db = _redis.GetDatabase();
        var ttl = await db.KeyTimeToLiveAsync(LockKey(intersectionId));
        return ttl?.TotalSeconds;
    }

    /// <summary>
    /// Manually releases a lock. Used by RestoreNormalTrafficCommand.
    /// Uses a Lua script to ensure we only delete OUR lock (not someone else's).
    /// </summary>
    public async Task<bool> ReleaseLockAsync(int intersectionId, string vehicleId)
    {
        var db = _redis.GetDatabase();
        var key = LockKey(intersectionId);

        // Lua script: only delete the key if its value matches our vehicleId.
        // This prevents accidentally releasing a lock that was re-acquired by
        // a DIFFERENT vehicle after ours expired.
        const string luaScript = @"
            if redis.call('GET', KEYS[1]) == ARGV[1] then
                return redis.call('DEL', KEYS[1])
            else
                return 0
            end";

        var result = (int)await db.ScriptEvaluateAsync(
            luaScript,
            new RedisKey[] { key },
            new RedisValue[] { vehicleId });

        if (result == 1)
        {
            _logger.LogInformation(
                "LOCK RELEASED: Intersection {Id} released by {Vehicle}",
                intersectionId, vehicleId);
            return true;
        }

        _logger.LogWarning(
            "LOCK NOT RELEASED: Intersection {Id} lock no longer held by {Vehicle} (expired or reassigned)",
            intersectionId, vehicleId);
        return false;
    }
}