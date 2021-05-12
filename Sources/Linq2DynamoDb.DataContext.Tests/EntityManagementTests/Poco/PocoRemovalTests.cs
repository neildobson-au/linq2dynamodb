using System.Linq;
using System.Threading.Tasks;
using Linq2DynamoDb.DataContext.Tests.Entities;
using Linq2DynamoDb.DataContext.Tests.Helpers;
using NUnit.Framework;

namespace Linq2DynamoDb.DataContext.Tests.EntityManagementTests.Poco
{
    [TestFixture]
    public class PocoRemovalTests : DataContextTestBase
    {
        public override void SetUp()
        {
            this.Context = TestConfiguration.GetDataContext();
        }

        public override Task TearDown()
        {
            return Task.CompletedTask;
        }

        [Test]
        public async Task DataContext_EntityRemoval_RemovesExistingRecordFromDynamoDb()
        {
            var book = await BookPocosHelper.CreateBookPocoAsync();

            var booksTable = this.Context.GetTable<BookPoco>();
            booksTable.RemoveOnSubmit(book);
            await this.Context.SubmitChangesAsync();

            var storedBookPocosCount = booksTable.Count(storedBookPoco => storedBookPoco.Name == book.Name);
            Assert.AreEqual(0, storedBookPocosCount, "Record was not deleted");
        }

        [Test]
        public async Task DataContext_EntityRemoval_DoesNotThrowAnyExceptionsIfRecordToRemoveDoesNotExist()
        {
            var book = await BookPocosHelper.CreateBookPocoAsync(persistToDynamoDb: false);

            var booksTable = this.Context.GetTable<BookPoco>();
            booksTable.RemoveOnSubmit(book);
            await this.Context.SubmitChangesAsync();
        }
    }
}
