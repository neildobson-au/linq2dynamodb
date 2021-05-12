using System.Linq;
using Linq2DynamoDb.DataContext.Tests.Entities;
using Linq2DynamoDb.DataContext.Tests.Helpers;
using NUnit.Framework;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace Linq2DynamoDb.DataContext.Tests.EntityManagementTests
{
    [TestFixture]
    public class EntityModificationTests : DataContextTestBase
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
        public async Task DataContext_EntityModification_UpdatesManyEntities()
        {
            var bookRev1 = await BooksHelper.CreateBookAsync(publishYear: 0);

            for(int i = 0; i < 200; i++)
            {
                await BooksHelper.CreateBookAsync(bookRev1.Name, i);
            }

            var query = Context.GetTable<Book>().Where(b => b.Name == bookRev1.Name);

            foreach(var b in query)
            {
                b.Author = "scale-tone";
            }

            await Context.SubmitChangesAsync();
        }



        [Test]
        public async Task DataContext_EntityModification_UpdatesRecordWithNewValues()
        {
            var book = await BooksHelper.CreateBookAsync(popularityRating: Book.Popularity.Average, persistToDynamoDb: false);

            var booksTable = this.Context.GetTable<Book>();
            booksTable.InsertOnSubmit(book);
            await this.Context.SubmitChangesAsync();

            book.PopularityRating = Book.Popularity.High;
            await this.Context.SubmitChangesAsync();

            var storedBook = await booksTable.FindAsync(book.Name, book.PublishYear);
            Assert.AreEqual(book.PopularityRating, storedBook.PopularityRating, "Record was not updated");
        }

        [Test]
        public async Task DataContext_EntityModification_UpdateRecordWithNewArray() 
        {
            var book = await BooksHelper.CreateBookAsync(rentingHistory: null, persistToDynamoDb: false);
            var booksTable = this.Context.GetTable<Book>();
            booksTable.InsertOnSubmit(book);
            await this.Context.SubmitChangesAsync();

            var storedBook = await booksTable.FindAsync(book.Name, book.PublishYear);

            storedBook.RentingHistory = new List<string>() { "non-empty array" };
            await this.Context.SubmitChangesAsync();

            var storedBookAfterModification = await booksTable.FindAsync(book.Name, book.PublishYear);
            
            CollectionAssert.AreEquivalent(storedBook.RentingHistory, storedBookAfterModification.RentingHistory);
        }

        [Ignore("This behavior is currently expected. SubmitChanges() uses DocumentBatchWrite, which only supports PUT operations with default 'replace' behavior")]
        [Test]
        public async Task DataContext_EntityModification_UpdateShouldNotAffectFieldsModifiedFromOutside()
        {
            var book = await BooksHelper.CreateBookAsync(popularityRating: Book.Popularity.Average, persistToDynamoDb: false);

            var booksTable = this.Context.GetTable<Book>();
            booksTable.InsertOnSubmit(book);
            await this.Context.SubmitChangesAsync();

            // Update record from outside of DataTable
            await BooksHelper.CreateBookAsync(book.Name, book.PublishYear, numPages: 15);

            book.PopularityRating = Book.Popularity.High;
            await this.Context.SubmitChangesAsync();

            var storedBook = await booksTable.FindAsync(book.Name, book.PublishYear);
            Assert.AreEqual(book.PopularityRating, storedBook.PopularityRating, "Record was not updated");
            Assert.AreEqual(book.NumPages, 15, "Update has erased changes from outside");
        }

        [Test]
        public async Task DataContext_UpdateEntity_UpdatesRecordWhenOldRecordIsNull()
        {
            var book = await BooksHelper.CreateBookAsync(popularityRating: Book.Popularity.Average);

            var booksTable = this.Context.GetTable<Book>();

            book.PopularityRating = Book.Popularity.High;
            ((ITableCudOperations)booksTable).UpdateEntity(book, null);

            var storedBook = await booksTable.FindAsync(book.Name, book.PublishYear);
            Assert.AreEqual(book.PopularityRating, storedBook.PopularityRating, "Record was not updated");
        }

        [Test]
        public async Task DataContext_UpdateEntity_UpdatesRecordWhenOldRecordDoesNotMatchNewRecord()
        {
            var book = await BooksHelper.CreateBookAsync(popularityRating: Book.Popularity.Average);

            var booksTable = this.Context.GetTable<Book>();
            var storedBook = await booksTable.FindAsync(book.Name, book.PublishYear);

            storedBook.PopularityRating = Book.Popularity.High;
            ((ITableCudOperations)booksTable).UpdateEntity(storedBook, book);

            var updatedBook = await booksTable.FindAsync(book.Name, book.PublishYear);
            Assert.AreEqual(storedBook.PopularityRating, updatedBook.PopularityRating, "Record was not updated");
        }
    }
}
