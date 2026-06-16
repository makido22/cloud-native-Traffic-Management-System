using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SmartCity.TrafficLightController.Models
{
    public class IntersectionRecord
    {
        public int IntersectionId { get; set; }
        public string Direction { get; set; } = string.Empty;
        public string CurrentColor { get; set; } = "RED";
        public bool IsEmergencyLocked { get; set; }
        public DateTime? LockExpiresAt { get; set; }
        public DateTime LastUpdatedAt { get; set; }
    }
}
