using MassTransit;
using SmartCity.EmergencyAPI.Models;

var builder = WebApplication.CreateBuilder(args);

builder.AddServiceDefaults();

builder.Services.AddMassTransit(x =>
{
    x.UsingRabbitMq((context, cfg) =>
    {
        var connectionString = builder.Configuration.GetConnectionString("messaging")
            ?? throw new InvalidOperationException("RabbitMQ connection string not found.");

        cfg.Host(new Uri(connectionString));

        cfg.Message<SmartCity.Contracts.Commands.EmergencyRouteCommand>(m =>
        {
            m.SetEntityName("emergency-route-commands");
        });
    });
});

builder.Services.AddEndpointsApiExplorer();
builder.Services.AddSwaggerGen(options =>
{
    options.SwaggerDoc("v1", new()
    {
        Title = "Smart City Emergency API",
        Version = "v1",
        Description = "Priority command path for emergency vehicle routing"
    });
});

var app = builder.Build();

// Configure the HTTP request pipeline.
if (app.Environment.IsDevelopment())
{
    app.MapDefaultEndpoints();
}

app.UseSwagger();
app.UseSwaggerUI(options =>
{
    options.SwaggerEndpoint("/swagger/v1/swagger.json", "Emergency API v1");
    options.RoutePrefix = string.Empty; // Swagger at root URL
});

app.MapPost("/emergency/route", async (
    EmergencyRouteRequest request,
    IBus bus,
    ILogger<Program> logger) =>
{
    if (request.IntersectionIds is null || request.IntersectionIds.Count == 0)
    {
        return Results.BadRequest("Route must contain at least one intersection.");
    }

    var issuedAt = DateTime.UtcNow;

    logger.LogWarning(
        "🚨 EMERGENCY ROUTE REQUEST: {Vehicle} requesting {Count} intersections: [{Ids}]",
        request.VehicleId,
        request.IntersectionIds.Count,
        string.Join(", ", request.IntersectionIds));

    // Publish one priority command per intersection in the route
    foreach (var intersectionId in request.IntersectionIds)
    {
        var command = new SmartCity.Contracts.Commands.EmergencyRouteCommand(
            IntersectionId: intersectionId,
            Direction: request.Direction,
            VehicleId: request.VehicleId,
            RoutePriority: request.Priority,
            IssuedAt: issuedAt
        );

        // Publish with AMQP priority header set to RoutePriority
        await bus.Publish(command, ctx =>
        {
            ctx.SetPriority((byte)request.Priority);
        });
    }

    return Results.Ok(new EmergencyRouteResponse
    {
        VehicleId = request.VehicleId,
        IntersectionsCleared = request.IntersectionIds.Count,
        IssuedAt = issuedAt,
        Message = $"Emergency route dispatched for {request.VehicleId}. " +
                  $"{request.IntersectionIds.Count} intersections set to priority GREEN."
    });
})
.WithName("DispatchEmergencyRoute")
.Produces<EmergencyRouteResponse>(200)
.Produces(400);

app.MapGet("/emergency/health", () => Results.Ok(new
{
    Status = "Operational",
    Service = "EmergencyAPI",
    Timestamp = DateTime.UtcNow
}))
.WithName("EmergencyHealth");

app.Run();