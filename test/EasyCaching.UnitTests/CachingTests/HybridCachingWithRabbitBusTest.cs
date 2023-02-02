using EasyCaching.Redis;
using Xunit.Abstractions;


namespace EasyCaching.UnitTests
{
    using System;
    using System.Threading.Tasks;
    using EasyCaching.Core;
    using Microsoft.Extensions.DependencyInjection;
    using Xunit;

    public class HybridCachingWithRabbitBusTest //: BaseCachingProviderTest
    {
        private readonly ITestOutputHelper _testOutputHelper;

        private static Node _node1;
        private static Node _node2;
        private static Node _node3;
        private static Node _node4;
        private static Node _node5;
        private static Node _node6;


        public static readonly TimeSpan _ttl = TimeSpan.FromMinutes(5);

        // These tests expect to find redis and rabbit in the following ports.
        const int RedisExposedPort = 6379;
        const int RabbitExposedPort = 5672;

        private Node Setup(string id, string topic, bool streams)
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
                }, $"redis-cache-{id}").WithJson($"redis-cache-{id}");

                options.UseHybrid(config =>
                {
                    config.EnableLogging = false;
                    config.TopicName = topic;
                    config.LocalCacheProviderName = $"mem-cache-{id}";
                    config.DistributedCacheProviderName = $"redis-cache-{id}";
                }, $"hybrid-cache-{id}");

                if (streams)
                {
                    options.WithRabbitMQStreamBus(config =>
                    {
                        config.HostName = "127.0.0.1";
                        config.Port = RabbitExposedPort;
                        config.QueueName = $"easy-caching-hybrid-bus-{id}";
                        config._logger = (s) => _testOutputHelper.WriteLine($"{id}: {s}");
                    });
                }
                else
                {
                    options.WithRabbitMQBus(config =>
                    {
                        config.HostName = "127.0.0.1";
                        config.Port = RabbitExposedPort;
                        config.QueueName = $"easy-caching-hybrid-bus-{id}";
                        config._logger = (s) => _testOutputHelper.WriteLine($"{id}: {s}");
                    });
                }
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

        public HybridCachingWithRabbitBusTest(ITestOutputHelper testOutputHelper)
        {
            _testOutputHelper = testOutputHelper;

            if (_node1 == null)
            {
                _node1 = Setup("01", "test-default", false);
                _node2 = Setup("02", "test-default", false);

                _node3 = Setup("03", "test-default", true);
                _node4 = Setup("04", "test-default", true);

                _node5 = Setup("05", "test-custom-topic", true);
                _node6 = Setup("06", "test-custom-topic", true);
            }
        }

        [Fact]
        public void Set_And_Get_Should_Succeed()
        {
            var cacheKey = GenerateCacheKey();

            _node1.Hybrid.Set(cacheKey, "val", TimeSpan.FromSeconds(30));

            var res = _node1.Hybrid.Get<string>(cacheKey);

            Assert.Equal("val", res.Value);
        }

        [Fact]
        public async Task SetAndGet_FromOtherHybrid_Should_Succeed()
        {
            var cacheKey = GenerateCacheKey();

            var cacheValue = "#1";

            _node1.Hybrid.Set(cacheKey, cacheValue, _ttl);

            await AssertKeyDoesNotExistsInMemoryAsync(cacheKey, _node2, _node3, _node4, _node5, _node6);

            await AssertKeyValueAsync(cacheKey, cacheValue, _node1, _node2, _node3, _node4);

            AssertKeyExistsInMemory(cacheKey, _node1, _node2, _node3, _node4);

            await AssertKeyDoesNotExistsInMemoryAsync(cacheKey, _node5, _node6);
        }

        [Fact]
        public async Task UpdateAsync_And_GetAsync_FromOtherHybrid_Should_Succeed()
        {
            var cacheKey = GenerateCacheKey();

            var cacheValue = "#1";
            _node1.Hybrid.Set(cacheKey, cacheValue, _ttl);

            // Queue-based nodes
            await AssertKeyValueAsync(cacheKey, cacheValue, _node1, _node2);

            // Stream-based nodes
            await AssertKeyValueAsync(cacheKey, cacheValue, _node3, _node4);

            cacheValue = "#2";

            _node1.Hybrid.Set(cacheKey, cacheValue, _ttl);

            await Task.Delay(100); // invalidation propagation

            await AssertKeyDoesNotExistsInMemoryAsync(cacheKey, _node2, _node3, _node4, _node5, _node6);

            // Queue-based nodes
            await AssertKeyValueAsync(cacheKey, cacheValue, _node1, _node2);

            // Stream-based nodes
            await AssertKeyValueAsync(cacheKey, cacheValue, _node3, _node4);

            await AssertKeyDoesNotExistsInMemoryAsync(cacheKey, _node5, _node6);
        }

        [Fact]
        public async Task Rabbit_OnDataUpdate_FromOtherHybrid_Should_Succeed()
        {
            var cacheKey = GenerateCacheKey();

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

        /*
        [Fact]
        public async Task Rabbit_OnDataUpdate_FromOtherHybrid_WithoutBus_Should_ReadOldValue()
        {
            var cacheKey = GenerateCacheKey();

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
        */

        [Fact]
        public async Task MultipleInvalidations_Should_Work()
        {
            for (var j = 0; j < 20; j++)
            {
                var cacheKey = GenerateCacheKey();

                for (var i = 0; i < 10; i++)
                {
                    var v = $"{i}";
                    await _node1.Hybrid.SetAsync(cacheKey, v, _ttl);
                    await Task.Delay(10);
                    await AssertKeyDoesNotExistsInMemoryAsync(cacheKey, _node2, _node3, _node4, _node5, _node6);
                    await AssertKeyValueAsync(cacheKey, v, _node1, _node2, _node3, _node4);
                }
            }
        }

        private static string GenerateCacheKey() => $"cache-key-{Guid.NewGuid()}";

        private static void AssertKeyExistsInMemory(string cacheKey, params Node[] nodes)
        {
            foreach (var node in nodes)
            {
                Assert.True(node.Memory.Exists(cacheKey));
            }
        }

        private static async Task AssertKeyDoesNotExistsInMemoryAsync(string cacheKey, params Node[] nodes)
        {
            foreach (var node in nodes)
            {
                Assert.False(await node.Memory.ExistsAsync(cacheKey));
            }
        }

        private static async Task AssertKeyValueAsync(string cacheKey, string value, params Node[] nodes)
        {
            foreach (var node in nodes)
            {
                Assert.Equal(value, (await node.Hybrid.GetAsync<string>(cacheKey))?.Value);
            }
        }
    }
}