namespace EasyCaching.Bus.RabbitMQ
{
    using EasyCaching.Core.Configurations;
    using System.Collections.Generic;

    /// <summary>
    /// RabbitMQ Bus options.
    /// </summary>
    public class RabbitMQBusOptions : BaseRabbitMQOptions
    {
        /// <summary>
        /// Gets or sets the name of the queue.
        /// </summary>
        /// <value>The name of the queue.</value>
        public string QueueName { get; set; } = "";

        public Dictionary<string, object> Arguments { get; set; } = new Dictionary<string, object>();

        public RabbitMQBusOptions WithMaxAge(int maxAgeMinutes)
        {
            Arguments["x-max-age"] = $"{maxAgeMinutes}m";
            return this;
        }

        public RabbitMQBusOptions WithMaxSegmentSize(int maxSegmentSizeBytes)
        {
            Arguments["x-stream-max-segment-size-bytes"] = maxSegmentSizeBytes;            
            return this;
        }
    }
}
