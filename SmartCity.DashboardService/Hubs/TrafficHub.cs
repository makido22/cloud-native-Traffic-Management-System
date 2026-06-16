using Microsoft.AspNetCore.SignalR;

namespace SmartCity.DashboardService.Hubs;

/// <summary>
/// SignalR hub that browsers connect to via WebSocket.
/// The server pushes traffic state changes to all connected clients.
/// </summary>
public class TrafficHub : Hub
{
    private readonly ILogger<TrafficHub> _logger;

    public TrafficHub(ILogger<TrafficHub> logger)
    {
        _logger = logger;
    }

    public override async Task OnConnectedAsync()
    {
        _logger.LogInformation("🔌 Client connected: {ConnectionId}", Context.ConnectionId);
        await base.OnConnectedAsync();
    }

    public override async Task OnDisconnectedAsync(Exception? exception)
    {
        _logger.LogInformation("🔌 Client disconnected: {ConnectionId}", Context.ConnectionId);
        await base.OnDisconnectedAsync(exception);
    }
}