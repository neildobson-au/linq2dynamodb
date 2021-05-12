using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Linq2DynamoDb.DataContext.Tests.Helpers;
using log4net;
using NUnit.Framework;

namespace Linq2DynamoDb.DataContext.Tests
{
    public abstract class DataContextTestBase
    {
        protected static readonly ILog Logger = LogManager.GetLogger(typeof(DataContextTestBase));

        protected DataContext Context { get; set; }

        [OneTimeSetUp]
        public static async Task ClassInit()
        {
            await BooksHelper.StartSessionAsync();
            await BookPocosHelper.StartSessionAsync();
        }

        [OneTimeTearDown]
        public static async Task ClassClean()
        {
            await BooksHelper.CleanSessionAsync();
            await BookPocosHelper.CleanSessionAsync();
        }

        [SetUp]
        public abstract void SetUp();

        [TearDown]
        public abstract Task TearDown();
        
        public static async Task ParallelForAsync(int fromInclusive, int toInclusive, Func<int, Task> body)
        {
            var tasks = new List<Task>();
            for (var i = fromInclusive; i < toInclusive; i++)
            {
                tasks.Add(body(i));
            }

            await Task.WhenAll(tasks);
        }
    }
}
