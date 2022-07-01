using EasyCaching.Redis;

namespace EasyCaching.UnitTests
{
    using System;
    using System.Threading.Tasks;
    using EasyCaching.Core;
    using Microsoft.Extensions.DependencyInjection;
    using Xunit;

    public class HybridCachingWithRabbitBusTest //: BaseCachingProviderTest
    {
        private string _namespace;

        private readonly Node _node1;
        private readonly Node _node2;

        public static readonly TimeSpan _ttl = TimeSpan.FromMinutes(5);

        // These tests expect to find redis and rabbit in the following ports.
        const int RedisExposedPort = 6379;
        const int RabbitExposedPort = 5672;
        const int RabbitHttpExposedPort = 15672;

        private Node Setup(string id)
        {
            IServiceCollection services = new ServiceCollection();

            services.AddEasyCaching(options =>
            {
                options.UseInMemory($"mem-cache-{id}");

                options.UseRedis(config =>
                {
                    config.DBConfig = new RedisDBOptions { AllowAdmin = true };
                    config.DBConfig.Endpoints.Add(new Core.Configurations.ServerEndPoint("127.0.0.1", RedisExposedPort));
                    config.DBConfig.Database = 5;
                }, $"redis-cache-{id}");

                options.UseHybrid(config =>
                {
                    config.EnableLogging = false;
                    config.TopicName = "hybrid";
                    config.LocalCacheProviderName = $"mem-cache-{id}";
                    config.DistributedCacheProviderName = $"redis-cache-{id}";
                }, $"hybrid-cache-{id}");

                options.WithRabbitMQBus(config =>
                {
                    config.HostName = "127.0.0.1";
                    config.Port = RabbitExposedPort;
                    config.QueueName = $"easy-caching-hybrid-bus-{id}";
                });
            });

            IServiceProvider serviceProvider = services.BuildServiceProvider();

            var cacheFactory = serviceProvider.GetService<IEasyCachingProviderFactory>();

            var hybridCacheFactory = serviceProvider.GetService<IHybridProviderFactory>();

            return new Node
            {
                Hybrid = hybridCacheFactory.GetHybridCachingProvider($"hybrid-cache-{id}"),
                Memory = cacheFactory.GetCachingProvider($"mem-cache-{id}"),
                Redis = cacheFactory.GetCachingProvider($"redis-cache-{id}")
            };
        }

        public class Node
        {
            public IHybridCachingProvider Hybrid;
            public IEasyCachingProvider Memory;
            public IEasyCachingProvider Redis;
        }

        public HybridCachingWithRabbitBusTest()
        {
            _node1 = Setup("01");
            _node2 = Setup("02");
        }

        [Fact]
        public async Task Set_And_Get_Should_Succeed()
        {
            var cacheKey = $"{_namespace}_{Guid.NewGuid()}";

            _node1.Hybrid.Set(cacheKey, "val", TimeSpan.FromSeconds(30));

            var res = _node1.Hybrid.Get<string>(cacheKey);

            Assert.Equal("val", res.Value);
        }

        [Fact]
        public async Task SetAsync_And_GetAsync_FromOtherHybrid_Should_Succeed()
        {
            var cacheKey = $"{_namespace}_{Guid.NewGuid()}";

            await _node1.Hybrid.SetAsync(cacheKey, "test-001", _ttl);

            // Value does not exist on mem-cache2 until request for the first time.
            Assert.False(await _node2.Memory.ExistsAsync(cacheKey));

            var res = _node2.Hybrid.Get<string>(cacheKey);

            Assert.Equal("test-001", res.Value);
            Assert.True(await _node2.Memory.ExistsAsync(cacheKey));
        }

        [Fact]
        public async Task Rabbit_OnDataUpdate_FromOtherHybrid_Should_Succeed()
        {
            var cacheKey = $"{_namespace}_{Guid.NewGuid()}";

            await _node1.Hybrid.SetAsync(cacheKey, "test-001", _ttl);

            Assert.False(await _node2.Memory.ExistsAsync(cacheKey), "Value should not exit until Get(...)");

            Assert.Equal("test-001", (await _node2.Hybrid.GetAsync<string>(cacheKey)).Value);

            await _node1.Hybrid.SetAsync(cacheKey, "test-002", _ttl);

            await Task.Delay(100); // invalidation propagation time..

            Assert.False(await _node2.Memory.ExistsAsync(cacheKey), "Value should not exit until Get(...)");

            var res = _node2.Hybrid.Get<string>(cacheKey);

            Assert.Equal("test-002", res.Value);
            Assert.True(await _node2.Memory.ExistsAsync(cacheKey));
        }

        [Fact]
        public async Task Rabbit_OnDataUpdate_FromOtherHybrid_WithoutBus_Should_ReadOldValue()
        {
            var cacheKey = $"{_namespace}_{Guid.NewGuid()}";

            await _node1.Hybrid.SetAsync(cacheKey, "test-001", _ttl);

            Assert.False(await _node2.Memory.ExistsAsync(cacheKey), "Value should not exit until Get(...)");

            Assert.Equal("test-001", (await _node2.Hybrid.GetAsync<string>(cacheKey)).Value);

            await _node1.Hybrid.SetAsync(cacheKey, "test-002", _ttl);

            await Task.Delay(100); // invalidation propagation time..

            Assert.True(await _node2.Memory.ExistsAsync(cacheKey), "Value should be still valid since bus is down.");

            var res = _node2.Hybrid.Get<string>(cacheKey);

            Assert.Equal("test-001", res.Value);
            Assert.True(await _node2.Memory.ExistsAsync(cacheKey));
        }

    }
}