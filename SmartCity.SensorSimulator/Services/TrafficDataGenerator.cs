using SmartCity.Contracts.Events;

namespace SmartCity.SensorSimulator.Services;

/// <summary>
/// Generates realistic IoT traffic sensor data.
/// Simulates traffic patterns: rush hour spikes, quiet nights, etc.
/// </summary>
public class TrafficDataGenerator
{
    private readonly string[] _directions = { "Northbound", "Southbound", "Eastbound", "Westbound" };
    private readonly int[] _intersectionIds = Enumerable.Range(101, 20).ToArray(); // 20 intersections

    public TrafficDataReceived Generate()
    {
        // _random = new Random() is NOT thread-safe. If produced from multiple threads,
        // Random will corrupt or return zeros. Use Random.Shared (thread-safe, .NET 6+).
        var _random = Random.Shared;

        var intersectionId = _intersectionIds[_random.Next(_intersectionIds.Length)];
        var direction = _directions[_random.Next(_directions.Length)];

        var now = DateTime.UtcNow;
        var hour = 0;
        var (minCars, maxCars) = hour switch
        {
            //>= 7 and <= 9 => (40, 100),    // Morning rush
            //>= 17 and <= 19 => (50, 110),  // Evening rush
            //>= 23 or <= 5 => (0, 15),      // Night quiet
            _ => (0, 100)                   // Normal
        };

        var vehicleCount = _random.Next(minCars, maxCars);
        var avgSpeed = vehicleCount > 70 ? _random.Next(5, 25) : _random.Next(30, 60);

        return new TrafficDataReceived(
            IntersectionId: intersectionId,
            Direction: direction,
            VehicleCount: vehicleCount,
            AverageSpeedKmh: avgSpeed,
            Timestamp: now
        );
    }
}