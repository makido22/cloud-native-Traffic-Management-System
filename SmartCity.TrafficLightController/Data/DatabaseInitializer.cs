using Microsoft.EntityFrameworkCore;

namespace SmartCity.TrafficLightController.Data;

/// <summary>
/// Automatically applies pending EF Core migrations on application startup.
/// Also verifies seed data exists.
/// This ensures the database is always in the correct state without manual intervention.
/// </summary>
public class DatabaseInitializer : IHostedService
{
    private readonly IServiceProvider _serviceProvider;
    private readonly ILogger<DatabaseInitializer> _logger;

    public DatabaseInitializer(
        IServiceProvider serviceProvider,
        ILogger<DatabaseInitializer> logger)
    {
        _serviceProvider = serviceProvider;
        _logger = logger;
    }

    public async Task StartAsync(CancellationToken cancellationToken)
    {
        _logger.LogInformation("🗄️ Initializing database...");

        using var scope = _serviceProvider.CreateScope();
        var dbContext = scope.ServiceProvider.GetRequiredService<TrafficDbContext>();

        int maxRetries = 5;
        for (int attempt = 1; attempt <= maxRetries; attempt++)
        {
            try
            {
                var pendingMigrations = await dbContext.Database.GetPendingMigrationsAsync(cancellationToken);
                var migrations = pendingMigrations.ToList();

                if (migrations.Count > 0)
                {
                    _logger.LogInformation("Applying {Count} pending migration(s)...", migrations.Count);
                    await dbContext.Database.MigrateAsync(cancellationToken);
                    _logger.LogInformation("✅ Migrations applied successfully.");
                }
                else
                {
                    _logger.LogInformation("✅ Database is up to date.");
                }

                var intersectionCount = await dbContext.Intersections.CountAsync(cancellationToken);
                _logger.LogInformation("📊 Database contains {Count} intersections.", intersectionCount);

                break;
            }
            catch (Exception ex)
            {
                if (attempt == maxRetries)
                {
                    _logger.LogCritical(ex, "❌ Database initialization failed after {Max} attempts!", maxRetries);
                    throw;
                }

                _logger.LogWarning("Database not ready yet (Attempt {Attempt}/{Max}). Waiting 2 seconds...", attempt, maxRetries);
                await Task.Delay(2000, cancellationToken);
            }
        }
    }

    public Task StopAsync(CancellationToken cancellationToken) => Task.CompletedTask;
}