namespace SmartCity.TrafficAnalyzer.Models
{
    public class IntersectionStats
    {
        public int LastVehicleCount { get; set; }
        public double LastSpeed { get; set; }
        public long TotalMessages { get; set; }
        public string CurrentColor { get; set; } = "RED";
        public DateTime LastCommandSentAt { get; set; } = DateTime.MinValue;
    }
}
