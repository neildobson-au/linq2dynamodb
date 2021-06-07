using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Linq2DynamoDb.DataContext.Tests.Entities;
using Linq2DynamoDb.DataContext.Tests.Helpers;
using NUnit.Framework;
using Shouldly;

namespace Linq2DynamoDb.DataContext.Tests.EntityManagementTests.Poco
{
    [TestFixture]
    public class PocoCreationTests : DataContextTestBase
    {
        public override void SetUp()
        {
            Context = TestConfiguration.GetDataContext();
        }

        public override Task TearDown()
        {
            return Task.CompletedTask;
        }

        [Test]
        public async Task DataContext_EntityCreation_PersistsRecordToDynamoDb()
        {
            var book = await BookPocosHelper.CreateBookPocoAsync(persistToDynamoDb: false);

            var booksTable = Context.GetTable<BookPoco>();
            booksTable.InsertOnSubmit(book);
            await Context.SubmitChangesAsync();

            var storedBookPoco = await booksTable.FindAsync(book.Name, book.PublishYear);
            Assert.IsNotNull(storedBookPoco);
        }

        [Ignore("This behavior is currently expected. SubmitChanges() uses DocumentBatchWrite, which only supports PUT operations, which by default replaces existing entities")]
        [Test]
//        [ExpectedException(typeof(InvalidOperationException), ExpectedMessage = "cannot be added, because entity with that key already exists", MatchType = MessageMatch.Contains)]
        public async Task DataContext_EntityCreation_ThrowsExceptionWhenEntityAlreadyExistsInDynamoDbButWasNeverQueriedInCurrentContext()
        {
            var book = await BookPocosHelper.CreateBookPocoAsync(popularityRating: BookPoco.Popularity.Average);

            book.PopularityRating = BookPoco.Popularity.High;

            var booksTable = Context.GetTable<BookPoco>();
            booksTable.InsertOnSubmit(book);
            await Context.SubmitChangesAsync();
        }

        [Test]
        public async Task DataContext_EntityCreation_ThrowsExceptionWhenTryingToAddSameEntityTwice()
        {
            var book = await BookPocosHelper.CreateBookPocoAsync(popularityRating: BookPoco.Popularity.Average, persistToDynamoDb: false);

            var booksTable = Context.GetTable<BookPoco>();
            booksTable.InsertOnSubmit(book);
            await Context.SubmitChangesAsync();

            book.PopularityRating = BookPoco.Popularity.High;

            booksTable.InsertOnSubmit(book);

            (await Should.ThrowAsync<InvalidOperationException>(() => Context.SubmitChangesAsync())).Message.ShouldContain(
                "cannot be added, because entity with that key already exists"
            );
        }

        [Test]
        public async Task DataContext_EntityCreation_ThrowsExceptionWhenEntityPreviouslyStoredInDynamoDbWasQueriedInCurrentContext()
        {
            var book = await BookPocosHelper.CreateBookPocoAsync(popularityRating: BookPoco.Popularity.Average);

            var booksTable = Context.GetTable<BookPoco>();
            await booksTable.FindAsync(book.Name, book.PublishYear);

            book.PopularityRating = BookPoco.Popularity.High;

            booksTable.InsertOnSubmit(book);

            (await Should.ThrowAsync<InvalidOperationException>(() => Context.SubmitChangesAsync())).Message.ShouldContain(
                "cannot be added, because entity with that key already exists"
            );
        }

        [Test]
        public async Task DataContext_EntityCreation_StoresComplexObjectProperties()
        {
            var book = await BookPocosHelper.CreateBookPocoAsync(persistToDynamoDb: false, publisher: new BookPoco.PublisherDto { Title = "O’Reilly Media", Address = "Sebastopol, CA" });

            var booksTable = Context.GetTable<BookPoco>();
            booksTable.InsertOnSubmit(book);
            await Context.SubmitChangesAsync();

            var storedBookPoco = await booksTable.FindAsync(book.Name, book.PublishYear);
            Assert.AreEqual(book.Publisher.ToString(), storedBookPoco.Publisher.ToString(), "Complex object properties are not equal");

            storedBookPoco.Publisher = new BookPoco.PublisherDto { Title = "O’Reilly Media", Address = "Illoqortormiut, Greenland" };

            await Context.SubmitChangesAsync();

            var storedBookPoco2 = await booksTable.FindAsync(book.Name, book.PublishYear);

            Assert.AreEqual(storedBookPoco2.Publisher.ToString(), storedBookPoco.Publisher.ToString(), "Complex object properties are not equal after updating");
        }


        [Test]
        public async Task DataContext_EntityCreation_StoresComplexObjectListProperties()
        {
            var book = await BookPocosHelper.CreateBookPocoAsync(persistToDynamoDb: false, reviews: new List<BookPoco.ReviewDto> { new BookPoco.ReviewDto { Author = "Beavis", Text = "Cool" }, new BookPoco.ReviewDto { Author = "Butt-head", Text = "This sucks!" } });

            var booksTable = Context.GetTable<BookPoco>();
            booksTable.InsertOnSubmit(book);
            await Context.SubmitChangesAsync();

            var storedBookPoco = await booksTable.FindAsync(book.Name, book.PublishYear);

            var expectedSequence1 = string.Join(", ", book.ReviewsList.Select(r => r.ToString()).OrderBy(s => s));
            var actualSequence1 = string.Join(", ", storedBookPoco.ReviewsList.Select(r => r.ToString()).OrderBy(s => s));
            Assert.AreEqual(expectedSequence1, actualSequence1, "Complex object list properties are not equal");
        }

    }
}
