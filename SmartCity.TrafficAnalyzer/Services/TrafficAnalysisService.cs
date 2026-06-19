using MassTransit;
using MassTransit.SqlTransport.Topology;
using Microsoft.AspNetCore.DataProtection.KeyManagement;
using Microsoft.AspNetCore.Http.HttpResults;
using RabbitMQ.Client;
using SmartCity.Contracts.Commands;
using SmartCity.Contracts.Events;
using SmartCity.TrafficAnalyzer.Models;
using System.Collections.Concurrent;
using System.Diagnostics.Metrics;
using System.Threading.Channels;
using static MassTransit.Logging.DiagnosticHeaders;
using static MassTransit.Transports.ReceiveEndpoint;

namespace SmartCity.TrafficAnalyzer.Services;

/// <summary>
/// Because Kafka guarantees all messages for intersection 101 go to the same partition, and that partition is owned by one replica,
/// the state.CurrentColor and LastCommandSentAt for intersection 101 are always consistent within that single replica.
/// Intersection 101 is ALWAYS handled by Replica 0.
/// → Replica 0's _stats[101] is the single source of truth for 101.
/// → No other replica ever touches 101.
/// → The cooldown and color logic remain correct.
/// The partitioning key inadvertently shards your state correctly. This is a beautiful, accidental consequence of good Kafka design.
/// </summary>
public class TrafficAnalysisService : IDisposable
{
    private readonly ILogger<TrafficAnalysisService> _logger;
    private readonly IBus _bus;

    private const string GREEN = "GREEN";
    private const string RED = "RED";   

    private readonly ConcurrentDictionary<int, IntersectionStats> _stats = new();

    // Buffers commands in memory so the Kafka consumer never waits for RabbitMQ
    private readonly Channel<ChangeTrafficLightCommand> _commandChannel;

    // vehicles
    private const int CongestionThreshold = 70; 
    private const int ClearThreshold = 30;
    private static readonly TimeSpan CommandCooldown = TimeSpan.FromSeconds(5);

    private readonly CancellationTokenSource _cts = new();
    private readonly Task _analyzerTask;
    private readonly Task _publisherTask;

    public TrafficAnalysisService(ILogger<TrafficAnalysisService> logger, IBus bus)
    {
        _logger = logger;
        _bus = bus;

        // Create a bounded channel to prevent OutOfMemory exceptions if RabbitMQ dies
        _commandChannel = Channel.CreateBounded<ChangeTrafficLightCommand>(new BoundedChannelOptions(10_000)
        {
            FullMode = BoundedChannelFullMode.Wait
        });

        _analyzerTask = RunAnalyzerLoop(_cts.Token);
        _publisherTask = RunPublisherLoop(_cts.Token);
    }

    public Task AnalyzeAsync(TrafficDataReceived data, CancellationToken cancellationToken)
    {
        var now = DateTime.UtcNow;

        //When Kafka rebalances(e.g., a replica crashes, or you scale up / down), partition ownership shifts:
        //Replica 0 crashes →
        //Kafka reassigns its partitions(0 - 3) to the surviving replicas.
        //→ Now Replica 1 suddenly owns intersection 101.
        //→ But Replica 1's _stats has NO history for 101!
        //→ state.CurrentColor defaults to null / empty, LastCommandSentAt = MinValue
        //→ It might send a redundant command or reset the cooldown.
        //The consequence: A brief moment of "amnesia" after rebalancing. For traffic lights,
        //this is usually harmless — the next sensor reading re - establishes the state within milliseconds.
        //"production hardening" approach -> use Redis.
        var state = _stats.AddOrUpdate(
            data.IntersectionId,
            _ => new IntersectionStats { TotalMessages = 1, LastVehicleCount = data.VehicleCount, LastSpeed = data.AverageSpeedKmh },
            (_, existing) =>
            {
                existing.TotalMessages++;
                existing.LastVehicleCount = data.VehicleCount;
                existing.LastSpeed = data.AverageSpeedKmh;
                return existing;
            });

        var timeInCurrentState = now - state.LastCommandSentAt;
        var isCooldownActive = timeInCurrentState < GetDynamicCooldown(data.VehicleCount);

        // Detect congestion
        if (data.VehicleCount > CongestionThreshold && state.CurrentColor == RED && !isCooldownActive)
        {
            _logger.LogWarning(
                "🚦 CONGESTION at Intersection {Id} ({Direction}): {Cars} vehicles, {Speed} km/h",
                data.IntersectionId, data.Direction, data.VehicleCount, data.AverageSpeedKmh);

            QueueLightChange(data.IntersectionId, data.Direction, GREEN,
                $"Congestion detected: {data.VehicleCount} vehicles", now);

            state.LastCommandSentAt = now;
            state.CurrentColor = GREEN;
        }
        else if (data.VehicleCount <= ClearThreshold && state.CurrentColor == GREEN && !isCooldownActive)
        {
            _logger.LogWarning(
                "Traffic cleared at Intersection {Id} ({Direction}): {Cars} vehicles, {Speed} km/h",
                data.IntersectionId, data.Direction, data.VehicleCount, data.AverageSpeedKmh);

            QueueLightChange(data.IntersectionId, data.Direction, RED,
                $"Traffic cleared: {data.VehicleCount} vehicles", now);

            state.CurrentColor = RED;
            state.LastCommandSentAt = now;
        }
        // 3. ANTI-STARVATION: Force RED if GREEN for too long during rush hour
        else if (state.CurrentColor == GREEN && timeInCurrentState.TotalSeconds > 60)
        {
            _logger.LogWarning(
                "FORCED RED at Intersection {Id} ({Direction}): {Cars} vehicles, {Speed} km/h",
                data.IntersectionId, data.Direction, data.VehicleCount, data.AverageSpeedKmh);

            QueueLightChange(data.IntersectionId, data.Direction, RED,
                "Anti-starvation: Max green time (120s) reached", now);

            state.LastCommandSentAt = now;
            state.CurrentColor = RED;
        }

        return Task.CompletedTask;
    }

    private void QueueLightChange(int intersectionId, string direction, string targetColor, string reason, DateTime now)
    {
        var command = new ChangeTrafficLightCommand(
            IntersectionId: intersectionId,
            Direction: direction,
            TargetColor: targetColor,
            Reason: reason,
            IssuedAt: now
        );

        // Drops it in the channel instantly. Non-blocking.
        _commandChannel.Writer.TryWrite(command);
    }

    private TimeSpan GetDynamicCooldown(int vehicleCount)
    {
        var jitter = TimeSpan.FromSeconds(Random.Shared.Next(0, 10));
        return CommandCooldown + jitter;
    }

    /// <summary>
    /// Background thread dedicated entirely to publishing to RabbitMQ.
    /// </summary>
    private async Task RunPublisherLoop(CancellationToken ct)
    {
        try
        {
            await foreach (var command in _commandChannel.Reader.ReadAllAsync(ct))
            {
                await _bus.Publish(command, ct);

                _logger.LogInformation(
                    "COMMAND PUBLISHED: Intersection {Id} ({Direction}) → {Color} | Reason: {Reason}",
                    command.IntersectionId, command.Direction, command.TargetColor, command.Reason);
            }
        }
        catch (OperationCanceledException) { /* clean exit */ }
        catch (Exception ex)
        {
            _logger.LogCritical(ex, "RabbitMQ Publisher loop crashed!");
        }
    }

    private async Task RunAnalyzerLoop(CancellationToken ct)
    {
        try
        {
            var total = 0L;
            while (!ct.IsCancellationRequested)
            {
                await Task.Delay(1000, ct);
                foreach (var intersection in _stats)
                {
                    total += intersection.Value.TotalMessages;
                }
                _logger.LogInformation("TrafficAnalysis. {Recieving {total} Msgs/sec}", total);
                total = 0;
            }
        }
        catch (OperationCanceledException) { /* clean exit */ }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Analyzer loop crashed");
        }
    }

    public void Dispose()
    {
        _commandChannel.Writer.Complete();
        _cts.Cancel();
        _cts.Dispose();
    }
}