using MassTransit;
using MassTransit.Transports;
using Microsoft.EntityFrameworkCore;
using Microsoft.VisualBasic;
using SmartCity.Contracts.Events;
using SmartCity.TrafficLightController.Data;
using SmartCity.TrafficLightController.Data.Entities;
using SmartCity.TrafficLightController.Models;
using System.Collections.Concurrent;
using System.Threading;

namespace SmartCity.TrafficLightController;

/// <summary>
/// In-memory state store for intersection light states.
/// </summary>
public class IntersectionStateStore(ILogger<IntersectionStateStore> logger, IServiceScopeFactory scopeFactory, IBus bus)
{
    private readonly ILogger<IntersectionStateStore> _logger = logger;
    private readonly IServiceScopeFactory _scopeFactory = scopeFactory;
    private readonly IBus _bus = bus;

    /// <summary>
    /// Updates the state of a traffic light in the database.
    /// Uses optimistic concurrency to prevent race conditions between
    /// normal traffic commands and emergency overrides.
    /// </summary>
    public async Task<bool> UpdateStateAsync(
        int intersectionId,
        string direction,
        string targetColor,
        string reason,
        bool isEmergency,
        CancellationToken cancellationToken = default)
    {
        using var scope = _scopeFactory.CreateScope();
        var db = scope.ServiceProvider.GetRequiredService<TrafficDbContext>();

        // Retry loop for optimistic concurrency conflicts
        const int maxRetries = 3;
        for (int attempt = 1; attempt <= maxRetries; attempt++)
        {
            try
            {
                var intersection = await db.Intersections
                    .FindAsync(new object[] { intersectionId }, cancellationToken);

                if (intersection is null)
                {
                    _logger.LogError("Intersection {Id} not found in database!", intersectionId);
                    return false;
                }

                var previousColor = intersection.CurrentColor;

                intersection.CurrentColor = targetColor;
                intersection.Direction = direction;
                intersection.LastChangeReason = reason;
                intersection.LastUpdatedAt = DateTime.UtcNow;

                if (isEmergency)
                {
                    intersection.Mode = "Emergency";
                    intersection.EmergencyLockUntil = DateTime.UtcNow.AddSeconds(60);
                }
                else if (intersection.Mode == "Emergency" &&
                         intersection.EmergencyLockUntil.HasValue &&
                         intersection.EmergencyLockUntil.Value < DateTime.UtcNow)
                {
                    // Lock expired, restore normal mode
                    intersection.Mode = "Normal";
                    intersection.EmergencyLockUntil = null;
                }

                var logEntry = new StateChangeLog
                {
                    IntersectionId = intersectionId,
                    PreviousColor = previousColor,
                    NewColor = targetColor,
                    Direction = direction,
                    Reason = reason,
                    WasEmergency = isEmergency,
                    LatencyMs = (DateTime.UtcNow - intersection.LastUpdatedAt).TotalMilliseconds,
                    ChangedAt = DateTime.UtcNow
                };

                db.StateChangeLogs.Add(logEntry);

                // Save — this is where concurrency check happens
                await db.SaveChangesAsync(cancellationToken);


                _logger.LogInformation(
                    "DB Updated: Intersection {Id} → {Color} (was {Previous})",
                    intersectionId, targetColor, previousColor);

                await _bus.Publish(new TrafficStateChangedEvent(
                    IntersectionId: intersectionId,
                    Direction: direction,
                    Color: targetColor,
                    Mode: intersection.Mode,
                    IsEmergency: isEmergency,
                    Reason: reason,
                    ChangedAt: intersection.LastUpdatedAt), cancellationToken);

                _logger.LogInformation(
                    "TrafficStateChangedEvent Fired: Intersection {Id}",
                    intersectionId);

                return true;
            }
            catch (DbUpdateConcurrencyException ex)
            {
                _logger.LogWarning(
                    "⚠️ Concurrency conflict on Intersection {Id} (attempt {Attempt}/{Max}). Another thread updated it first.",
                    intersectionId, attempt, maxRetries);

                if (attempt == maxRetries)
                {
                    _logger.LogError(ex,
                        "❌ Concurrency conflict unresolved after {Max} attempts for Intersection {Id}.",
                        maxRetries, intersectionId);
                    return false;
                }

                var entry = db.ChangeTracker.Entries()
                    .FirstOrDefault(e => e.Entity is Intersection i && i.Id == intersectionId);
                entry?.State = EntityState.Detached;

                await Task.Delay(50 * attempt, cancellationToken); // Brief backoff
            }
        }

        return false;
    }

    /// <summary>
    /// Checks if an intersection is currently locked by an emergency override.
    /// </summary>
    public async Task<bool> IsLockedAsync(int intersectionId, CancellationToken cancellationToken = default)
    {
        using var scope = _scopeFactory.CreateScope();
        var db = scope.ServiceProvider.GetRequiredService<TrafficDbContext>();

        var intersection = await db.Intersections
            .AsNoTracking()
            .FirstOrDefaultAsync(i => i.Id == intersectionId, cancellationToken);

        if (intersection is null) return false;

        if (intersection.Mode != "Emergency") return false;

        // if lock expired
        if (intersection.EmergencyLockUntil.HasValue &&
            intersection.EmergencyLockUntil.Value < DateTime.UtcNow)
        {
            await RestoreNormalModeAsync(intersectionId, cancellationToken);
            return false;
        }

        return true;
    }

    /// <summary>
    /// Restores an intersection to normal mode after emergency lock expires.
    /// </summary>
    public async Task RestoreNormalModeAsync(int intersectionId, CancellationToken cancellationToken = default)
    {
        using var scope = _scopeFactory.CreateScope();
        var db = scope.ServiceProvider.GetRequiredService<TrafficDbContext>();

        var intersection = await db.Intersections
            .FindAsync(new object[] { intersectionId }, cancellationToken);

        if (intersection is null) return;

        intersection.Mode = "Normal";
        intersection.EmergencyLockUntil = null;
        intersection.LastUpdatedAt = DateTime.UtcNow;

        await db.SaveChangesAsync(cancellationToken);

        _logger.LogInformation("Intersection {Id} restored to Normal mode.", intersectionId);

        await _bus.Publish(new TrafficStateChangedEvent(
            IntersectionId: intersectionId,
            Direction: intersection.Direction,
            Color: intersection.CurrentColor,
            Mode: intersection.Mode,
            IsEmergency: intersection.EmergencyLockUntil.HasValue,
            Reason: intersection.LastChangeReason,
            ChangedAt: intersection.LastUpdatedAt), cancellationToken);

        _logger.LogInformation(
            "TrafficStateChangedEvent Fired: Intersection {Id}",
            intersectionId);
    }

    /// <summary>
    /// Gets the current state of a specific intersection.
    /// </summary>
    public async Task<Intersection?> GetStateAsync(int intersectionId, CancellationToken cancellationToken = default)
    {
        using var scope = _scopeFactory.CreateScope();
        var db = scope.ServiceProvider.GetRequiredService<TrafficDbContext>();

        return await db.Intersections
            .AsNoTracking()
            .FirstOrDefaultAsync(i => i.Id == intersectionId, cancellationToken);
    }

    /// <summary>
    /// Gets all intersection states (for dashboard).
    /// </summary>
    public async Task<List<Intersection>> GetAllStatesAsync(CancellationToken cancellationToken = default)
    {
        using var scope = _scopeFactory.CreateScope();
        var db = scope.ServiceProvider.GetRequiredService<TrafficDbContext>();

        return await db.Intersections
            .AsNoTracking()
            .OrderBy(i => i.Id)
            .ToListAsync(cancellationToken);
    }
}