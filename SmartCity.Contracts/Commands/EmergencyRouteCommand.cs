namespace SmartCity.Contracts.Commands;

/// <summary>
/// Published by EmergencyAPI → Consumed by TrafficLightController via PRIORITY queue.
/// Represents a single intersection in an ambulance's route that must turn GREEN immediately.
/// </summary>
public record EmergencyRouteCommand(
    int IntersectionId,
    string Direction,
    string VehicleId,        // e.g., "AMBULANCE-42"
    int RoutePriority,       // 1-10, where 10 is highest urgency
    DateTime IssuedAt
);