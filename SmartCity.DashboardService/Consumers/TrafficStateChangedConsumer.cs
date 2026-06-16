using MassTransit;
using Microsoft.AspNetCore.SignalR;
using SmartCity.Contracts.Events;
using SmartCity.DashboardService.Hubs;

namespace SmartCity.DashboardService.Consumers;

/// <summary>
/// Consumes TrafficStateChangedEvent from RabbitMQ and pushes it to all
/// connected browsers via the SignalR hub.
/// </summary>
public class TrafficStateChangedConsumer(
    IHubContext<TrafficHub> hubContext,
    ILogger<TrafficStateChangedConsumer> logger) : IConsumer<TrafficStateChangedEvent>
{
    private readonly IHubContext<TrafficHub> _hubContext = hubContext;
    private readonly ILogger<TrafficStateChangedConsumer> _logger = logger;

    public async Task Consume(ConsumeContext<TrafficStateChangedEvent> context)
    {
        var evt = context.Message;

        _logger.LogInformation(
            "📡 Pushing state change: Intersection {Id} → {Color} ({Mode})",
            evt.IntersectionId, evt.Color, evt.Mode);

        // Push to ALL connected clients. The browser JS listens for "StateChanged".
        await _hubContext.Clients.All.SendAsync("StateChanged", new
        {
            intersectionId = evt.IntersectionId,
            direction = evt.Direction,
            color = evt.Color,
            mode = evt.Mode,
            isEmergency = evt.IsEmergency,
            reason = evt.Reason,
            changedAt = evt.ChangedAt
        });
    }
}