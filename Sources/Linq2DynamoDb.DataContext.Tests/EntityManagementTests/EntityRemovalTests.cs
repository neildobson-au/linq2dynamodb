using System.Linq;
using System.Threading.Tasks;
using Linq2DynamoDb.DataContext.Tests.Entities;
using Linq2DynamoDb.DataContext.Tests.Helpers;
using NUnit.Framework;

namespace Linq2DynamoDb.DataContext.Tests.EntityManagementTests
{
    [TestFixture]
    public class EntityRemovalTests : DataContextTestBase
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
            var book = await BooksHelper.CreateBookAsync();

            var booksTable = this.Context.GetTable<Book>();
            booksTable.RemoveOnSubmit(book);
            await this.Context.SubmitChangesAsync();

            var storedBooksCount = booksTable.Count(storedBook => storedBook.Name == book.Name);
            Assert.AreEqual(0, storedBooksCount, "Record was not deleted");
        }

        [Test]
        public async Task DataContext_EntityRemoval_DoesNotThrowAnyExceptionsIfRecordToRemoveDoesNotExist()
        {
            var book = await BooksHelper.CreateBookAsync(persistToDynamoDb: false);

            var booksTable = this.Context.GetTable<Book>();
            booksTable.RemoveOnSubmit(book);
            await this.Context.SubmitChangesAsync();
        }
    }
}
