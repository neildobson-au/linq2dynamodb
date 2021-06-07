﻿using System.Threading.Tasks;
using Linq2DynamoDb.DataContext.Tests.QueryTests;
using NUnit.Framework;

namespace Linq2DynamoDb.DataContext.Tests.NonCachingTests
{
    [TestFixture]
    public class PositioningTests : PositioningTestsCommon
    {
        public override void SetUp()
        {
            Context = TestConfiguration.GetDataContext();
        }

        public override Task TearDown()
        {
            return Task.CompletedTask;
        }
    }
}
