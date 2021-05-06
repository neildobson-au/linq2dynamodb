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

        [TestFixtureSetUp]
        public static async Task ClassInit()
        {
            BooksHelper.StartSession();
            await BookPocosHelper.StartSession();
        }

        [TestFixtureTearDown]
        public static async Task ClassClean()
        {
            BooksHelper.CleanSession();
            await BookPocosHelper.CleanSession();
        }

        [SetUp]
        public abstract void SetUp();

        [TearDown]
        public abstract void TearDown();
    }
}
