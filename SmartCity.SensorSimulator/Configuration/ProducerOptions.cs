using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SmartCity.SensorSimulator.Configuration
{
    public class ProducerOptions
    {
        public int ProducerThreads { get; set; }
        public int TargetMessagesPerSecond { get; set; }
    }
}
