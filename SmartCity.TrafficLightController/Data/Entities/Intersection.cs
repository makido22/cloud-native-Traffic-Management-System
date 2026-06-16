using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;

namespace SmartCity.TrafficLightController.Data.Entities;

/// <summary>
/// Represents a physical traffic intersection in the city.
/// This is the single source of truth for the current state of each traffic light.
/// </summary>
public class Intersection
{
    [Key]
    public int Id { get; set; }

    public string Name { get; set; } = string.Empty;

    public string CurrentColor { get; set; } = "RED";

    public string Direction { get; set; } = "Northbound";

    public string Mode { get; set; } = "Normal"; // "Normal" or "Emergency"

    public DateTime? EmergencyLockUntil { get; set; }

    [MaxLength(200)]
    public string? LastChangeReason { get; set; }

    public DateTime LastUpdatedAt { get; set; } = DateTime.UtcNow;

    public DateTime CreatedAt { get; set; } = DateTime.UtcNow;

    public uint RowVersion { get; set; }
}