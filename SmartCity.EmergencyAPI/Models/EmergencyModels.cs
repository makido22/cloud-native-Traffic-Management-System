namespace SmartCity.EmergencyAPI.Models;

/// <summary>
/// Request to clear a route for an emergency vehicle.
/// </summary>
public class EmergencyRouteRequest
{
    /// <summary>Identifier of the emergency vehicle (e.g., "AMBULANCE-42").</summary>
    public string VehicleId { get; set; } = string.Empty;

    /// <summary>Ordered list of intersection IDs along the route.</summary>
    public List<int> IntersectionIds { get; set; } = new();

    /// <summary>Direction of travel through the intersections.</summary>
    public string Direction { get; set; } = "Northbound";

    /// <summary>Priority level 1-10 (10 = highest urgency).</summary>
    public int Priority { get; set; } = 10;
}

public class EmergencyRouteResponse
{
    public string VehicleId { get; set; } = string.Empty;
    public int IntersectionsCleared { get; set; }
    public DateTime IssuedAt { get; set; }
    public string Message { get; set; } = string.Empty;
}