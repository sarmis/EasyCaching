namespace EasyCaching.Bus.RabbitMQ
{
    using System;
    using System.Collections.Generic;
    using System.Threading;
    using System.Threading.Tasks;
    using EasyCaching.Core;
    using EasyCaching.Core.Bus;
    using EasyCaching.Core.Serialization;
    using global::RabbitMQ.Client;
    using global::RabbitMQ.Client.Events;
    using Microsoft.Extensions.ObjectPool;
    using Microsoft.Extensions.Options;

    /// <summary>
    /// Default RabbitMQ Bus.
    /// </summary>
    public class DefaultRabbitMQStreamBus : EasyCachingAbstractBus
    {
        /// <summary>
        /// The subscriber connection.
        /// </summary>
        private readonly IConnection _subConnection;

        /// <summary>
        /// The publish channel pool.
        /// </summary>
        private readonly ObjectPool<IModel> _pubChannelPool;

        /// <summary>
        /// The rabbitMQ Bus options.
        /// </summary>
        private readonly RabbitMQBusOptions _options;

        /// <summary>
        /// The serializer.
        /// </summary>
        private readonly IEasyCachingSerializer _serializer;

        /// <summary>
        /// The identifier.
        /// </summary>
        private readonly string _busId;

        private static readonly Dictionary<string, object> _streamArgs = new Dictionary<string, object>
        {
            { "x-queue-type", "stream" },
            { "x-max-age", "5m" },
            { "x-stream-max-segment-size-bytes", 4_000_000 }
        };

        /// <summary>
        /// Initializes a new instance of the <see cref="T:EasyCaching.Bus.RabbitMQ.DefaultRabbitMQBus"/> class.
        /// </summary>
        /// <param name="_objectPolicy">Object policy.</param>
        /// <param name="rabbitMQOptions">RabbitMQ Options.</param>
        /// <param name="serializer">Serializer.</param>
        public DefaultRabbitMQStreamBus(
            IPooledObjectPolicy<IModel> _objectPolicy
            , IOptions<RabbitMQBusOptions> rabbitMQOptions
            , IEasyCachingSerializer serializer)
        {
            this._options = rabbitMQOptions.Value;
            this._serializer = serializer;

            var factory = new ConnectionFactory
            {
                HostName = _options.HostName,
                UserName = _options.UserName,
                Port = _options.Port,
                Password = _options.Password,
                VirtualHost = _options.VirtualHost,
                RequestedConnectionTimeout = System.TimeSpan.FromMilliseconds(_options.RequestedConnectionTimeout),
                SocketReadTimeout = System.TimeSpan.FromMilliseconds(_options.SocketReadTimeout),
                SocketWriteTimeout = System.TimeSpan.FromMilliseconds(_options.SocketWriteTimeout),
                ClientProvidedName = _options.ClientProvidedName
            };

            _subConnection = factory.CreateConnection();

            _subConnection.ConnectionShutdown += (_, e) =>
            {
                _options._logger($"ERROR: ConnectionShutdown: {e.ReplyText}");
            };

            var provider = new DefaultObjectPoolProvider();

            _pubChannelPool = provider.Create(_objectPolicy);

            _busId = Guid.NewGuid().ToString("N");

            BusName = "easycachingbus";
        }

        /// <summary>
        /// Publish the specified topic and message.
        /// </summary>
        /// <param name="topic">Topic.</param>
        /// <param name="message">Message.</param>
        public override void BasePublish(string topic, EasyCachingMessage message)
        {
            var channel = _pubChannelPool.Get();

            try
            {
                var body = _serializer.Serialize(message);

                channel.ExchangeDeclare(_options.TopicExchangeName, ExchangeType.Topic, true, false, null);
                channel.BasicPublish(_options.TopicExchangeName, topic, false, null, body);
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.Message);
            }
            finally
            {
                _pubChannelPool.Return(channel);
            }
        }

        /// <summary>
        /// Publish the specified topic and message async.
        /// </summary>
        /// <returns>The async.</returns>
        /// <param name="topic">Topic.</param>
        /// <param name="message">Message.</param>
        /// <param name="cancellationToken">Cancellation token.</param>
        public override Task BasePublishAsync(string topic, EasyCachingMessage message, CancellationToken cancellationToken = default(CancellationToken))
        {
            var channel = _pubChannelPool.Get();
            try
            {
                var body = _serializer.Serialize(message);

                channel.ExchangeDeclare(_options.TopicExchangeName, ExchangeType.Topic, true, false, null);
                channel.BasicPublish(_options.TopicExchangeName, topic, false, null, body);
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.Message);
            }
            finally
            {
                _pubChannelPool.Return(channel);
            }
            return Task.CompletedTask;
        }

        /// <summary>
        /// Subscribe the specified topic and action.
        /// </summary>
        /// <param name="topic">Topic.</param>
        /// <param name="action">Action.</param>
        public override void BaseSubscribe(string topic, Action<EasyCachingMessage> action)
        {
            var queueName = $"rmq.stream.easycaching.{topic}";

            Task.Factory.StartNew(
                () => StartConsumer(queueName, topic),
                TaskCreationOptions.LongRunning);
        }

        /// <summary>
        /// Subscribe the specified topic and action async.
        /// </summary>
        /// <param name="topic">Topic.</param>
        /// <param name="action">Action.</param>
        /// <param name="cancellationToken">Cancellation token.</param>
        public override Task BaseSubscribeAsync(string topic, Action<EasyCachingMessage> action, CancellationToken cancellationToken = default(CancellationToken))
        {
            _options._logger($"WARNING: BaseSubscribeAsync for {topic}");

            var queueName = $"rmq.stream.easycaching.{topic}";

            StartConsumer(queueName, topic);
            return Task.CompletedTask;
        }

        private void StartConsumer(string queueName, string topic)
        {
            _options._logger($"WARNING: Starting Consumer for {topic} ({queueName})");
            var channel = _subConnection.CreateModel();

            channel.ModelShutdown += (_, e) =>
            {
                _options._logger($"ERROR: ModelShutdown: {e.ReplyText}");
            };

            // Streams require Qos setup
            channel.BasicQos(0, 100, false);
            channel.ExchangeDeclare(_options.TopicExchangeName, ExchangeType.Topic, true, false, null);
            channel.QueueDeclare(queueName, true, false, false, _streamArgs);
            // bind the queue with the exchange.
            channel.QueueBind(queueName, _options.TopicExchangeName, topic);
            var consumer = new EventingBasicConsumer(channel);
            consumer.Received += OnMessage;

            consumer.ConsumerCancelled += (sender, e) =>
            {
                _options._logger($"ERROR: EasyCaching.DefaultRabbitMQStreamBus.OnConsumerCancelled: (Q: {queueName})");
            };

            consumer.Shutdown += (sender, e) =>
            {
                _options._logger($"ERROR: EasyCaching.DefaultRabbitMQStreamBus.OnConsumerShutdown: (Q: {queueName}) {e.ReplyText}");
                StartConsumer(queueName, topic);
                BaseOnReconnect();
            };

            channel.BasicConsume(queueName, false, consumer);
        }

        /// <summary>
        /// Ons the message.
        /// </summary>
        /// <param name="sender">Sender.</param>
        /// <param name="e">E.</param>
        private void OnMessage(object sender, BasicDeliverEventArgs e)
        {
            try
            {
                var message = _serializer.Deserialize<EasyCachingMessage>(e.Body.ToArray());
                _options._logger?.Invoke(string.Join(",", message.CacheKeys));
                BaseOnMessage(message);
            }
            finally
            {
                (sender as EventingBasicConsumer)?.Model.BasicAck(e.DeliveryTag, false);
            }
        }
    }
}
