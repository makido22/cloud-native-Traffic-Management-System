using StackExchange.Redis;

namespace SmartCity.Gateway.RateLimiting;

/// <summary>
/// A distributed rate limiter backed by Redis.
/// Enforces limits GLOBALLY across all gateway instances by using a
/// shared Redis counter, solving the problem that in-memory limiters
/// cannot enforce limits when scaled horizontally.
/// 
/// Algorithm: Fixed-window counter with atomic INCR + EXPIRE via Lua.
/// </summary>
public class RedisRateLimiter
{
    private readonly IConnectionMultiplexer _redis;
    private readonly ILogger<RedisRateLimiter> _logger;

    public RedisRateLimiter(
        IConnectionMultiplexer redis,
        ILogger<RedisRateLimiter> logger)
    {
        _redis = redis;
        _logger = logger;
    }

    /// <summary>
    /// Atomically checks and increments the rate limit counter for a client.
    /// Returns a result indicating whether the request is allowed.
    /// </summary>
    /// <param name="clientKey">Unique identifier (e.g., client IP).</param>
    /// <param name="limit">Max requests allowed in the window.</param>
    /// <param name="windowSeconds">The window duration in seconds.</param>
    public async Task<RateLimitResult> CheckAsync(
        string clientKey,
        int limit,
        int windowSeconds)
    {
        var db = _redis.GetDatabase();
        var redisKey = $"ratelimit:{clientKey}";

        // Lua script (atomic):
        // 1. INCR the counter, key is deleted automatically after it expires, then it is treated as new key
        // 2. If this is the FIRST request (count == 1), set the expiry window
        // 3. Get the remaining TTL
        // 4. Return [current_count, ttl_seconds]
        //
        // Doing INCR + EXPIRE atomically prevents a race where a request
        // increments but the key never gets a TTL (leading to a permanent counter).
        const string luaScript = @"
            local current = redis.call('INCR', KEYS[1])
            if current == 1 then
                redis.call('EXPIRE', KEYS[1], ARGV[1])
            end
            local ttl = redis.call('TTL', KEYS[1])
            return {current, ttl}";

        var result = (RedisValue[])(await db.ScriptEvaluateAsync(
            luaScript,
            new RedisKey[] { redisKey },
            new RedisValue[] { windowSeconds }))!;

        var currentCount = (int)result[0];
        var ttl = (int)result[1];

        var allowed = currentCount <= limit;
        var remaining = Math.Max(0, limit - currentCount);

        if (!allowed)
        {
            _logger.LogWarning(
                "⛔ RATE LIMIT EXCEEDED: Client {Client} — {Count}/{Limit} requests. Retry in {Ttl}s.",
                clientKey, currentCount, limit, ttl);
        }

        return new RateLimitResult(
            IsAllowed: allowed,
            CurrentCount: currentCount,
            Limit: limit,
            Remaining: remaining,
            RetryAfterSeconds: ttl);
    }
}

public record RateLimitResult(
    bool IsAllowed,
    int CurrentCount,
    int Limit,
    int Remaining,
    int RetryAfterSeconds);