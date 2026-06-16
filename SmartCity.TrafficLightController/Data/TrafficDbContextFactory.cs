using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Design;

namespace SmartCity.TrafficLightController.Data;

/// <summary>
/// This class is ONLY used by the EF Core CLI tools (dotnet ef migrations add).
/// It bypasses Program.cs and provides a dummy connection string so migrations
/// can be generated without Aspire or Docker running.
/// </summary>
public class TrafficDbContextFactory : IDesignTimeDbContextFactory<TrafficDbContext>
{
    public TrafficDbContext CreateDbContext(string[] args)
    {
        var optionsBuilder = new DbContextOptionsBuilder<TrafficDbContext>();

        // This is a DUMMY connection string. 
        // EF Core just needs the format to know it's PostgreSQL.
        // It will not actually try to connect to this during 'migrations add'.
        optionsBuilder.UseNpgsql("Host=localhost;Database=dummy;Username=postgres;Password=postgres");

        return new TrafficDbContext(optionsBuilder.Options);
    }
}