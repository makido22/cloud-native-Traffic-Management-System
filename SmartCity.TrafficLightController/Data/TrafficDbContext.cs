using Microsoft.EntityFrameworkCore;
using SmartCity.TrafficLightController.Data.Entities;

namespace SmartCity.TrafficLightController.Data;

public class TrafficDbContext : DbContext
{
    public TrafficDbContext(DbContextOptions<TrafficDbContext> options) : base(options)
    {
    }

    public DbSet<Intersection> Intersections => Set<Intersection>();
    public DbSet<StateChangeLog> StateChangeLogs => Set<StateChangeLog>();

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        base.OnModelCreating(modelBuilder);

        modelBuilder.Entity<Intersection>(entity =>
        {
            entity.ToTable("intersections");

            entity.HasKey(e => e.Id);

            // Don't auto-generate IDs — use specific intersection numbers (101-120)
            entity.Property(e => e.Id)
                  .ValueGeneratedNever();

            entity.Property(e => e.Name)
                  .IsRequired();

            entity.Property(e => e.CurrentColor)
                  .HasDefaultValue("RED")
                  .IsRequired();

            entity.Property(e => e.Mode)
                  .HasDefaultValue("Normal")
                  .IsRequired();

            // Optimistic concurrency: Use PostgreSQL xmin system column
            entity.Property(e => e.RowVersion)
                   .IsRowVersion();

            // Index for fast lookups by mode (find all emergency-locked intersections)
            entity.HasIndex(e => e.Mode);

            entity.HasIndex(e => e.LastUpdatedAt);
        });

        modelBuilder.Entity<StateChangeLog>(entity =>
        {
            entity.ToTable("state_change_logs");

            entity.HasKey(e => e.Id);

            entity.Property(e => e.Id)
                  .UseIdentityAlwaysColumn(); // Auto-increment

            entity.HasOne(e => e.Intersection)
                  .WithMany()
                  .HasForeignKey(e => e.IntersectionId)
                  .OnDelete(DeleteBehavior.Cascade);

            // Index for querying history of a specific intersection
            entity.HasIndex(e => new { e.IntersectionId, e.ChangedAt });

            // Index for time-range queries (thesis analytics)
            entity.HasIndex(e => e.ChangedAt);
        });

        var seedDate = new DateTime(2024, 1, 1, 0, 0, 0, DateTimeKind.Utc);

        var seedData = Enumerable.Range(101, 20).Select(id => new Intersection
        {
            Id = id,
            Name = $"Intersection-{id}",
            CurrentColor = "RED",
            Direction = "Northbound",
            Mode = "Normal",
            LastUpdatedAt = seedDate,
            CreatedAt = seedDate
        }).ToArray();

        modelBuilder.Entity<Intersection>().HasData(seedData);
    }
}