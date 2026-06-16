using System.ComponentModel.DataAnnotations;

namespace SmartCity.TrafficLightController.Data.Entities;

/// <summary>
/// Immutable audit log of every traffic light state change.
/// Used for thesis analytics and dashboard history.
/// </summary>
public class StateChangeLog
{
    [Key]
    public long Id { get; set; }

    public int IntersectionId { get; set; }

    [MaxLength(20)]
    public string PreviousColor { get; set; } = string.Empty;

    [MaxLength(20)]
    public string NewColor { get; set; } = string.Empty;

    [MaxLength(20)]
    public string Direction { get; set; } = string.Empty;

    [MaxLength(200)]
    public string Reason { get; set; } = string.Empty;

    public bool WasEmergency { get; set; }

    public double LatencyMs { get; set; }

    public DateTime ChangedAt { get; set; } = DateTime.UtcNow;

    public Intersection Intersection { get; set; } = null!;
}