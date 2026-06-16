using System.Diagnostics;
using System.Diagnostics.Metrics;

namespace SmartCity.ServiceDefaults;

/// <summary>
/// Centralized definitions for custom traces and metrics.
/// Shared across all microservices.
/// </summary>
public static class Telemetry
{
    // TRACING — for creating custom spans
    public const string ActivitySourceName = "SmartCity.Tracing";
    public static readonly ActivitySource ActivitySource = new(ActivitySourceName);

    // METRICS — for counting business events
    public const string MeterName = "SmartCity.Metrics";
    private static readonly Meter Meter = new(MeterName);

    public static readonly Counter<long> EmergencyRequests =
        Meter.CreateCounter<long>(
            "smartcity.emergency.requests",
            description: "Total number of emergency route requests");

    public static readonly Counter<long> DiscardedCommands =
        Meter.CreateCounter<long>(
            "smartcity.commands.discarded",
            description: "Commands discarded due to emergency lock");

    public static readonly Counter<long> RateLimitRejections =
        Meter.CreateCounter<long>(
            "smartcity.ratelimit.rejections",
            description: "Requests rejected by rate limiter");

    public static readonly Histogram<double> EmergencyLatency =
        Meter.CreateHistogram<double>(
            "smartcity.emergency.latency",
            unit: "ms",
            description: "End-to-end emergency command latency");

    public static readonly Counter<long> StateChanges =
        Meter.CreateCounter<long>(
            "smartcity.statechanges",
            description: "Total traffic light state changes");
}