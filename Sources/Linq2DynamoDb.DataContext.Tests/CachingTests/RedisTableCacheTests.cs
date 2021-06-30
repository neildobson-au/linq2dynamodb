﻿using System;
using Linq2DynamoDb.DataContext.Caching.Redis;
using NUnit.Framework;
using StackExchange.Redis;

namespace Linq2DynamoDb.DataContext.Tests.CachingTests
{
    public class RedisTableCacheTests : TableCacheTestsBase
    {
        private ConnectionMultiplexer _redisConn;

        public override void SetUp()
        {
            var config = new ConfigurationOptions();
            config.EndPoints.Add(TestConfiguration.RedisLocalAddress);
            config.AllowAdmin = true;
            this._redisConn = ConnectionMultiplexer.Connect(config);
            this._redisConn.GetServer(TestConfiguration.RedisLocalAddress).FlushAllDatabases();

            this.TableCache = new RedisTableCache(this._redisConn, TimeSpan.FromSeconds(10));
            base.SetUp();
        }
        
        protected override void DropIndexEntityFromCache(string indexKey)
        {
            indexKey = "{Books}:" + indexKey;
            bool success = this._redisConn.GetDatabase().KeyDelete(indexKey);
            Assert.IsTrue(success, "The index wasn't dropped from cache. Check the key format.");
        }

    }
}
