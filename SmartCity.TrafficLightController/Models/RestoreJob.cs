namespace SmartCity.TrafficLightController.Models;

/// <summary>
/// A claimed restoration job ready to be executed.
/// </summary>
public record RestoreJob(int IntersectionId, string VehicleId);