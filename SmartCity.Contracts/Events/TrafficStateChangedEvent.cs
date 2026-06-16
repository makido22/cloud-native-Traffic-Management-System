namespace SmartCity.Contracts.Events;

/// <summary>
/// Published by TrafficLightController AFTER a state change is committed to the DB.
/// Consumed by DashboardService to push live updates to browsers.
/// </summary>
public record TrafficStateChangedEvent(
    int IntersectionId,
    string Direction,
    string Color,         // RED, YELLOW, GREEN
    string Mode,          // Normal, Emergency
    bool IsEmergency,
    string Reason,
    DateTime ChangedAt
);