using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Linq2DynamoDb.DataContext.Tests.Entities;
using Linq2DynamoDb.DataContext.Tests.Helpers;
using NUnit.Framework;
using Shouldly;

namespace Linq2DynamoDb.DataContext.Tests.EntityManagementTests
{
    [TestFixture]
    public class EntityCreationTests : DataContextTestBase
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
        public async Task DataContext_EntityCreation_PersistsRecordToDynamoDb()
        {
            var book = await BooksHelper.CreateBookAsync(persistToDynamoDb: false);

            var booksTable = this.Context.GetTable<Book>();
            booksTable.InsertOnSubmit(book);
            await this.Context.SubmitChangesAsync();

            var storedBook = await booksTable.FindAsync(book.Name, book.PublishYear);
            Assert.IsNotNull(storedBook);
        }

        [Ignore("This behavior is currently expected. SubmitChanges() uses DocumentBatchWrite, which only supports PUT operations, which by default replaces existing entities")]
        [Test]
        // [ExpectedException(typeof(InvalidOperationException), ExpectedMessage = "cannot be added, because entity with that key already exists", MatchType = MessageMatch.Contains)]
        public async Task DataContext_EntityCreation_ThrowsExceptionWhenEntityAlreadyExistsInDynamoDbButWasNeverQueriedInCurrentContext()
        {
            var book = await BooksHelper.CreateBookAsync(popularityRating: Book.Popularity.Average);

            book.PopularityRating = Book.Popularity.High;

            var booksTable = this.Context.GetTable<Book>();
            booksTable.InsertOnSubmit(book);
            await this.Context.SubmitChangesAsync();
        }

        [Test]
        public async Task DataContext_EntityCreation_ThrowsExceptionWhenTryingToAddSameEntityTwice()
        {
            var book = await BooksHelper.CreateBookAsync(popularityRating: Book.Popularity.Average, persistToDynamoDb: false);

            var booksTable = this.Context.GetTable<Book>();
            booksTable.InsertOnSubmit(book);
            await this.Context.SubmitChangesAsync();

            book.PopularityRating = Book.Popularity.High;

            booksTable.InsertOnSubmit(book);

            (await Should.ThrowAsync<InvalidOperationException>(() => this.Context.SubmitChangesAsync())).Message.ShouldContain(
                "cannot be added, because entity with that key already exists"
            );
        }

        [Test]
        public async Task DataContext_EntityCreation_ThrowsExceptionWhenEntityPreviouslyStoredInDynamoDbWasQueriedInCurrentContext()
        {
            var book = await BooksHelper.CreateBookAsync(popularityRating: Book.Popularity.Average);

            var booksTable = this.Context.GetTable<Book>();
            await booksTable.FindAsync(book.Name, book.PublishYear);

            book.PopularityRating = Book.Popularity.High;

            booksTable.InsertOnSubmit(book);

            (await Should.ThrowAsync<InvalidOperationException>(() => this.Context.SubmitChangesAsync())).Message.ShouldContain(
                "cannot be added, because entity with that key already exists"
            );
        }

        [Test]
        public async Task DataContext_EntityCreation_StoresComplexObjectProperties()
        {
            var book = await BooksHelper.CreateBookAsync(persistToDynamoDb: false, publisher: new Book.PublisherDto { Title = "O’Reilly Media", Address = "Sebastopol, CA" });

            var booksTable = this.Context.GetTable<Book>();
            booksTable.InsertOnSubmit(book);
            await this.Context.SubmitChangesAsync();

            var storedBook = await booksTable.FindAsync(book.Name, book.PublishYear);
            Assert.AreEqual(book.Publisher.ToString(), storedBook.Publisher.ToString(), "Complex object properties are not equal");

            storedBook.Publisher = new Book.PublisherDto { Title = "O’Reilly Media", Address = "Illoqortormiut, Greenland" };

            await this.Context.SubmitChangesAsync();

            var storedBook2 = await booksTable.FindAsync(book.Name, book.PublishYear);

            Assert.AreEqual(storedBook2.Publisher.ToString(), storedBook.Publisher.ToString(), "Complex object properties are not equal after updating");
        }


        [Test]
        public async Task DataContext_EntityCreation_StoresComplexObjectListProperties()
        {
            var book = await BooksHelper.CreateBookAsync(persistToDynamoDb: false, reviews: new List<Book.ReviewDto> { new Book.ReviewDto { Author = "Beavis", Text = "Cool" }, new Book.ReviewDto { Author = "Butt-head", Text = "This sucks!" } });

            var booksTable = this.Context.GetTable<Book>();
            booksTable.InsertOnSubmit(book);
            await this.Context.SubmitChangesAsync();

            var storedBook = await booksTable.FindAsync(book.Name, book.PublishYear);

            var expectedSequence1 = string.Join(", ", book.ReviewsList.Select(r => r.ToString()).OrderBy(s => s));
            var actualSequence1 = string.Join(", ", storedBook.ReviewsList.Select(r => r.ToString()).OrderBy(s => s));
            Assert.AreEqual(expectedSequence1, actualSequence1, "Complex object list properties are not equal");
        }

    }
}
