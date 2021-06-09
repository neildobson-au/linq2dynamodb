using Linq2DynamoDb.DataContext.Tests.Entities;
using Linq2DynamoDb.DataContext.Tests.Helpers;
using NUnit.Framework;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace Linq2DynamoDb.DataContext.Tests.EntityManagementTests.Poco
{
    [TestFixture]
    public class PocoModificationTests : DataContextTestBase
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
        public async Task DataContext_EntityModification_UpdatesRecordWithNewValues()
        {
            var book = await BookPocosHelper.CreateBookPocoAsync(popularityRating: BookPoco.Popularity.Average, persistToDynamoDb: false);

            var booksTable = this.Context.GetTable<BookPoco>();
            booksTable.InsertOnSubmit(book);
            await this.Context.SubmitChangesAsync();

            book.PopularityRating = BookPoco.Popularity.High;
            await this.Context.SubmitChangesAsync();

            var storedBookPoco = await booksTable.FindAsync(book.Name, book.PublishYear);
            Assert.AreEqual(book.PopularityRating, storedBookPoco.PopularityRating, "Record was not updated");
        }

        [Test]
        public async Task DataContext_EntityModification_UpdateRecordWithNewArray()
        {
            var book = await BookPocosHelper.CreateBookPocoAsync(rentingHistory: null, persistToDynamoDb: false);
            var booksTable = this.Context.GetTable<BookPoco>();
            booksTable.InsertOnSubmit(book);
            await this.Context.SubmitChangesAsync();

            var storedBookPoco = await booksTable.FindAsync(book.Name, book.PublishYear);

            storedBookPoco.RentingHistory = new List<string>() { "non-empty array" };
            await this.Context.SubmitChangesAsync();

            var storedBookPocoAfterModification = await booksTable.FindAsync(book.Name, book.PublishYear);

            CollectionAssert.AreEquivalent(storedBookPoco.RentingHistory, storedBookPocoAfterModification.RentingHistory);
        }

        [Ignore("This behavior is currently expected. SubmitChanges() uses DocumentBatchWrite, which only supports PUT operations with default 'replace' behavior")]
        [Test]
        public async Task DataContext_EntityModification_UpdateShouldNotAffectFieldsModifiedFromOutside()
        {
            var book = await BookPocosHelper.CreateBookPocoAsync(popularityRating: BookPoco.Popularity.Average, persistToDynamoDb: false);

            var booksTable = this.Context.GetTable<BookPoco>();
            booksTable.InsertOnSubmit(book);
            await this.Context.SubmitChangesAsync();

            // Update record from outside of DataTable
            await BookPocosHelper.CreateBookPocoAsync(book.Name, book.PublishYear, numPages: 15);

            book.PopularityRating = BookPoco.Popularity.High;
            await this.Context.SubmitChangesAsync();

            var storedBookPoco = await booksTable.FindAsync(book.Name, book.PublishYear);
            Assert.AreEqual(book.PopularityRating, storedBookPoco.PopularityRating, "Record was not updated");
            Assert.AreEqual(book.NumPages, 15, "Update has erased changes from outside");
        }

        [Test]
        public async Task DataContext_UpdateEntity_UpdatesRecordWhenOldRecordIsNull()
        {
            var book = await BookPocosHelper.CreateBookPocoAsync(popularityRating: BookPoco.Popularity.Average);

            var booksTable = this.Context.GetTable<BookPoco>();

            book.PopularityRating = BookPoco.Popularity.High;
            ((ITableCudOperations)booksTable).UpdateEntity(book, null);

            var storedBookPoco = await booksTable.FindAsync(book.Name, book.PublishYear);
            Assert.AreEqual(book.PopularityRating, storedBookPoco.PopularityRating, "Record was not updated");
        }

        [Test]
        public async Task DataContext_UpdateEntity_UpdatesRecordWhenOldRecordDoesNotMatchNewRecord()
        {
            var book = await BookPocosHelper.CreateBookPocoAsync(popularityRating: BookPoco.Popularity.Average);

            var booksTable = this.Context.GetTable<BookPoco>();
            var storedBookPoco = await booksTable.FindAsync(book.Name, book.PublishYear);

            storedBookPoco.PopularityRating = BookPoco.Popularity.High;
            ((ITableCudOperations)booksTable).UpdateEntity(storedBookPoco, book);

            var updatedBookPoco = await booksTable.FindAsync(book.Name, book.PublishYear);
            Assert.AreEqual(storedBookPoco.PopularityRating, updatedBookPoco.PopularityRating, "Record was not updated");
        }
    }
}
