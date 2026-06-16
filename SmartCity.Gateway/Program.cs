using SmartCity.Gateway.RateLimiting;

var builder = WebApplication.CreateBuilder(args);

builder.AddServiceDefaults();

builder.AddRedisClient("cache");
builder.Services.AddSingleton<RedisRateLimiter>();

builder.Services.AddReverseProxy()
    .LoadFromConfig(builder.Configuration.GetSection("ReverseProxy"))
    .AddServiceDiscoveryDestinationResolver();

var app = builder.Build();

app.MapDefaultEndpoints();

app.UseMiddleware<RateLimitingMiddleware>();

app.MapReverseProxy();

app.Run();
