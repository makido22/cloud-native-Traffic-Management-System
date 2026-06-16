using SmartCity.ServiceDefaults;

namespace SmartCity.Gateway.RateLimiting;

/// <summary>
/// Middleware that applies Redis-based rate limiting to emergency endpoints.
/// Runs BEFORE YARP forwards the request to the backend.
/// </summary>
public class RateLimitingMiddleware
{
    private readonly RequestDelegate _next;
    private readonly RedisRateLimiter _rateLimiter;
    private readonly ILogger<RateLimitingMiddleware> _logger;

    private const int EmergencyLimit = 10;
    private const int WindowSeconds = 60;

    public RateLimitingMiddleware(
        RequestDelegate next,
        RedisRateLimiter rateLimiter,
        ILogger<RateLimitingMiddleware> logger)
    {
        _next = next;
        _rateLimiter = rateLimiter;
        _logger = logger;
    }

    public async Task InvokeAsync(HttpContext context)
    {
        if (!context.Request.Path.StartsWithSegments("/emergency"))
        {
            await _next(context);
            return;
        }

        var instanceId = Environment.GetEnvironmentVariable("INSTANCE_ID")
            ?? Environment.MachineName;
        context.Response.Headers["X-Gateway-Instance"] = instanceId;

        // Identify the client by IP address
        var clientIp = GetClientIp(context);

        // Check the distributed rate limit
        var result = await _rateLimiter.CheckAsync(
            clientKey: clientIp,
            limit: EmergencyLimit,
            windowSeconds: WindowSeconds);

        // Add informative rate limit headers (industry standard)
        context.Response.Headers["X-RateLimit-Limit"] = result.Limit.ToString();
        context.Response.Headers["X-RateLimit-Remaining"] = result.Remaining.ToString();

        if (!result.IsAllowed)
        {
            Telemetry.RateLimitRejections.Add(1,
                new KeyValuePair<string, object?>("client", clientIp));

            context.Response.StatusCode = StatusCodes.Status429TooManyRequests;
            context.Response.Headers["Retry-After"] = result.RetryAfterSeconds.ToString();

            await context.Response.WriteAsJsonAsync(new
            {
                error = "Rate limit exceeded",
                message = $"Maximum {result.Limit} emergency requests per {WindowSeconds} seconds. " +
                          $"Retry after {result.RetryAfterSeconds} seconds.",
                limit = result.Limit,
                retryAfterSeconds = result.RetryAfterSeconds
            });

            return; // STOP — do not forward to backend
        }

        _logger.LogInformation(
            "✅ Request allowed for {Client}: {Count}/{Limit}",
            clientIp, result.CurrentCount, result.Limit);

        // Allowed — continue to YARP forwarding
        await _next(context);
    }

    private static string GetClientIp(HttpContext context)
    {
        // Check X-Forwarded-For header first (for load balancers / proxies)
        var forwardedFor = context.Request.Headers["X-Forwarded-For"].FirstOrDefault();
        if (!string.IsNullOrEmpty(forwardedFor))
        {
            // Take the first IP in the chain (the original client)
            return forwardedFor.Split(',')[0].Trim();
        }

        return context.Connection.RemoteIpAddress?.ToString() ?? "unknown";
    }
}