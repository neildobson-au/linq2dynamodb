using Amazon.DynamoDBv2.Model;
using Amazon.DynamoDBv2.DocumentModel;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Linq2DynamoDb.DataContext.Tests.Entities;
using Linq2DynamoDb.DataContext.Tests.Helpers;
using NUnit.Framework;

namespace Linq2DynamoDb.DataContext.Tests.QueryTests
{
    public abstract class BasicQueryTestsCommon : DataContextTestBase
	{
		// ReSharper disable InconsistentNaming
        [Test]
        public async Task DataContext_Find_ReturnsExistingRecordWhenUsedWithHashAndRangeKeys()
        {
            var book = await BooksHelper.CreateBookAsync(publishYear: 1);
            await BooksHelper.CreateBookAsync(book.Name, book.PublishYear + 1);

            var bookTable = Context.GetTable<Book>();
            var storedBook = await bookTable.FindAsync(book.Name, book.PublishYear);

            Assert.AreEqual(book.Name, storedBook.Name, "Book with wrong name was returned");
            Assert.AreEqual(book.PublishYear, storedBook.PublishYear, "Book with wrong publishYear was returned");

            var storedBook2 = bookTable.FindAsync(book.Name, book.PublishYear).Result;

            Assert.AreEqual(book.Name, storedBook2.Name, "Book with wrong name was returned");
            Assert.AreEqual(book.PublishYear, storedBook2.PublishYear, "Book with wrong publishYear was returned");
        }

        [Test]
        // [ExpectedException(typeof(InvalidOperationException), ExpectedMessage = "Sequence contains no elements")]
        public async Task DataContext_Find_ThrowsExceptionWhenRecordDoesNotExist()
        {
            var bookTable = Context.GetTable<Book>();
            await bookTable.FindAsync(Guid.NewGuid().ToString(), 0);
        }


        [Test]
        // [ExpectedException(typeof(AggregateException))]
        public void DataContext_FindAsync_ThrowsExceptionWhenRecordDoesNotExist()
        {
            var bookTable = Context.GetTable<Book>();
            bookTable.FindAsync(Guid.NewGuid().ToString(), 0).Wait();
        }

        [Test]
        // [ExpectedException(typeof(InvalidOperationException), ExpectedMessage = "has 2 key fields, but 1 key values was provided", MatchType = MessageMatch.Contains)]
        public async Task DataContext_Find_ThrowsExceptionIfUserDoesNotProvideRangeKeyInHashRangeTables()
        {
            var book = await BooksHelper.CreateBookAsync();

            var bookTable = Context.GetTable<Book>();
            await bookTable.FindAsync(book.Name);
        }

        [Test]
        public async Task DataContext_Find_RespectsPredefinedHashKey()
        {
            var book = await BooksHelper.CreateBookAsync(publishYear: 1234);

            var bookTable = Context.GetTable<Book>(book.Name);
            await bookTable.FindAsync(book.PublishYear);
        }

        [Test]
		public async Task DataContext_Query_ReturnsEnumFieldsStoredAsInt()
		{
			var book = await BooksHelper.CreateBookAsync(userFeedbackRating: Book.Stars.Platinum);

			var bookTable = Context.GetTable<Book>();
			var booksQuery = from record in bookTable where record.Name == book.Name select record;
			var storedBook = booksQuery.First();

			Assert.AreEqual(book.UserFeedbackRating, storedBook.UserFeedbackRating);
		}

		[Test]
		public async Task DataContext_Query_ReturnsEnumFieldsStoredAsString()
		{
			var book = await BooksHelper.CreateBookAsync(popularityRating: Book.Popularity.Average);

			var bookTable = Context.GetTable<Book>();
			var booksQuery = from record in bookTable where record.Name == book.Name select record;
			var storedBook = booksQuery.First();

			Assert.AreEqual(book.PopularityRating, storedBook.PopularityRating);
		}

		[Test]
		public async Task DataContext_Query_ReturnsStringFields()
		{
			var book = await BooksHelper.CreateBookAsync(author: "TestAuthor");

			var bookTable = Context.GetTable<Book>();
			var booksQuery = from record in bookTable where record.Name == book.Name select record;
			var storedBook = booksQuery.First();

			Assert.AreEqual(book.Author, storedBook.Author);
		}

		[Test]
		public async Task DataContext_Query_ReturnsIntFields()
		{
			var book = await BooksHelper.CreateBookAsync(numPages: 555);

			var bookTable = Context.GetTable<Book>();
			var booksQuery = from record in bookTable where record.Name == book.Name select record;
			var storedBook = booksQuery.First();

			Assert.AreEqual(book.NumPages, storedBook.NumPages);
		}

		[Test]
		public async Task DataContext_Query_ReturnsDateTimeFields()
		{
			var testTime = DateTime.Today.Add(new TimeSpan(0, 8, 45, 30, 25));

			var book = await BooksHelper.CreateBookAsync(lastRentTime: testTime);

			var bookTable = Context.GetTable<Book>();
			var booksQuery = from record in bookTable where record.Name == book.Name select record;
			var storedBook = booksQuery.First();

			Assert.AreEqual(book.LastRentTime.ToUniversalTime(), storedBook.LastRentTime.ToUniversalTime());
		}

		[Test]
		public async Task DataContext_Query_ReturnsListArrays()
		{
			var book = await BooksHelper.CreateBookAsync(rentingHistory: new List<string> { "Marie", "Anna", "Alex" });

			var bookTable = Context.GetTable<Book>();
			var booksQuery = from record in bookTable where record.Name == book.Name select record;
			var storedBook = booksQuery.First();

			Assert.IsNotNull(storedBook.RentingHistory, "Expected non-null string array");
			var expectedSequence = string.Join(", ", book.RentingHistory.OrderBy(s => s));
            var actualSequence = string.Join(", ", storedBook.RentingHistory.OrderBy(s => s));
			Assert.AreEqual(expectedSequence, actualSequence, "String array elements sequence incorrect");
		}

        [Test]
        public async Task DataContext_Query_ReturnsComplexObjectProperties()
        {
            var book = await BooksHelper.CreateBookAsync(publisher: new Book.PublisherDto { Title = "O’Reilly Media", Address = "Sebastopol, CA"});

            var bookTable = Context.GetTable<Book>();
            var booksQuery = from record in bookTable where record.Name == book.Name select record;
            var storedBook = booksQuery.First();

            Assert.AreEqual(book.Publisher.ToString(), storedBook.Publisher.ToString(), "Complex object properties are not equal");
        }

        [Test]
        public async Task DataContext_Query_ReturnsComplexObjectListProperties()
        {
            var book = await BooksHelper.CreateBookAsync(reviews: new List<Book.ReviewDto> { new Book.ReviewDto { Author = "Beavis", Text = "Cool" }, new Book.ReviewDto { Author = "Butt-head", Text = "This sucks!" } });

            var bookTable = Context.GetTable<Book>();
            var booksQuery = from record in bookTable where record.Name == book.Name select record;
            var storedBook = booksQuery.First();

            var expectedSequence1 = string.Join(", ", book.ReviewsList.Select(r => r.ToString()).OrderBy(s => s));
            var actualSequence1 = string.Join(", ", storedBook.ReviewsList.Select(r => r.ToString()).OrderBy(s => s));
            Assert.AreEqual(expectedSequence1, actualSequence1, "Complex object list properties are not equal");
        }

		[Test]
		public async Task DataContext_Query_ReturnsDictionaryStringTimeSpan()
		{
			var book = await 
				BooksHelper.CreateBookAsync(
					filmsBasedOnBook:
						new Dictionary<string, TimeSpan>
						{
							{ "A Man and a Test", TimeSpan.FromMinutes(90) },
							{ "Avatar 12", TimeSpan.FromMinutes(9000) },
							{ "Aliens 8", TimeSpan.FromMinutes(80) }
						});

			var bookTable = Context.GetTable<Book>();
			var booksQuery = from record in bookTable where record.Name == book.Name select record;
			var storedBook = booksQuery.First();

			Assert.IsNotNull(storedBook.FilmsBasedOnBook, "Expected non-null dictionary");
			Assert.AreEqual(book.FilmsBasedOnBook.Count, storedBook.FilmsBasedOnBook.Count, "Dictionary size mismatch");
			foreach (var key in book.FilmsBasedOnBook.Keys)
			{
				Assert.IsTrue(
					storedBook.FilmsBasedOnBook.ContainsKey(key),
					"Stored dictionary does not have required key (" + key + ")");
				Assert.AreEqual(book.FilmsBasedOnBook[key], storedBook.FilmsBasedOnBook[key], "Values mismatch");
			}
		}

		[Test]
		public async Task DataContext_Query_ReturnsUninitializedFields()
		{
			var book = await BooksHelper.CreateBookAsync(publishYear: 2013);

			var bookTable = Context.GetTable<Book>();
			var booksQuery = from record in bookTable where record.Name == book.Name select record;
			var storedBook = booksQuery.First();

			Assert.AreEqual(book.NumPages, storedBook.NumPages, "Incorrect default value for int field");
			Assert.AreEqual(book.PopularityRating, storedBook.PopularityRating, "Incorrect default value for enum (string) field");
			Assert.AreEqual(book.UserFeedbackRating, storedBook.UserFeedbackRating, "Incorrect default value for enum (int) field");
			Assert.AreEqual(book.LastRentTime.ToUniversalTime(), storedBook.LastRentTime.ToUniversalTime(), "Incorrect default value for DateTime field");
            Assert.IsNull(storedBook.RentingHistory, "Incorrect default value for IList<string> field");
			Assert.IsNull(storedBook.FilmsBasedOnBook, "Incorrect default value for IDictionary<string, TimeSpan> field");
		}

		[Test]
		public async Task DataContext_Query_ReturnsCorrectRecordIfPositionedByRangeKey()
		{
			var bookRev1 = await BooksHelper.CreateBookAsync(publishYear: 2012);
			var bookRev2 = await BooksHelper.CreateBookAsync(bookRev1.Name, 2013);

			var bookTable = Context.GetTable<Book>();
			var booksQuery = from record in bookTable
				where record.Name == bookRev1.Name && record.PublishYear == bookRev2.PublishYear
				select record;

			Assert.AreEqual(1, booksQuery.Count());

			var storedBook = booksQuery.First();

			Assert.AreEqual(bookRev2.Name, storedBook.Name, "Returned record does not contain the required HashKey");
			Assert.AreEqual(bookRev2.PublishYear, storedBook.PublishYear, "Returned record does not contain the required RangeKey");
		}

		// GroupBy operation is currently not supported
		[Test]
		[Ignore("it")]
		public async Task DateContext_Query_GroupByReturnsGroupedRecords()
		{
			var bookRev1 = await BooksHelper.CreateBookAsync(publishYear: 2012);
			var bookRev2 = await BooksHelper.CreateBookAsync(bookRev1.Name, 2013);

			var bookTable = Context.GetTable<Book>();
			var booksGroupsByName = from record in bookTable
									where record.Name == bookRev1.Name
									group record by record.Name
										into recordGroup
										select recordGroup;

			Assert.AreEqual(1, booksGroupsByName.Count());

			var firstGroup = booksGroupsByName.First();

			Assert.IsTrue(firstGroup.Contains(bookRev1, new BooksComparer()));
			Assert.IsTrue(firstGroup.Contains(bookRev2, new BooksComparer()));
		}

		[Test]
		public void DateContext_Query_DefaultIfEmptyReturnsCollectionWithOneNullRecordIfNoDefaultValueSpecified()
		{
			var bookTable = Context.GetTable<Book>();
			var booksQuery = from record in bookTable where record.Name == Guid.NewGuid().ToString() select record;

			Assert.AreEqual(0, booksQuery.Count());

			var defaultIfEmptyResult = booksQuery.DefaultIfEmpty();
			Assert.AreEqual(1, defaultIfEmptyResult.Count());

			Assert.IsNull(defaultIfEmptyResult.First());
		}

		[Test]
		public void DateContext_Query_DefaultIfEmptyReturnsCollectionWithOneDefaultRecordIfDefaultValueSpecified()
		{
			var bookTable = Context.GetTable<Book>();
			var booksQuery = from record in bookTable where record.Name == Guid.NewGuid().ToString() select record;

			Assert.AreEqual(0, booksQuery.Count());

			var defaultBook = new Book { Name = Guid.NewGuid().ToString(), PublishYear = 1999 };
			var defaultIfEmptyResult = booksQuery.DefaultIfEmpty(defaultBook);
			Assert.AreEqual(1, defaultIfEmptyResult.Count());

			var actualResult = defaultIfEmptyResult.First();

			Assert.IsNotNull(actualResult);
			Assert.AreEqual(defaultBook.Name, actualResult.Name);
			Assert.AreEqual(defaultBook.PublishYear, actualResult.PublishYear);
		}

		[Test]
		public async Task DateContext_Query_AnyFunctionReturnsTrueIfOneElementMatchesPredicate()
		{
			var bookRev1 = await BooksHelper.CreateBookAsync(publishYear: 2012);
			var bookRev2 = await BooksHelper.CreateBookAsync(bookRev1.Name, 2013);

			var anyResult = Context.GetTable<Book>().Any(book => book.Name == bookRev1.Name && book.PublishYear == bookRev2.PublishYear);

			Assert.IsTrue(anyResult);
		}

		[Test]
		public async Task DateContext_Query_AnyFunctionReturnsTrueIfAllElementsMatchPredicate()
		{
			var bookRev1 = await BooksHelper.CreateBookAsync(publishYear: 2012);
			await BooksHelper.CreateBookAsync(bookRev1.Name, 2013);

			var anyResult = Context.GetTable<Book>().Any(book => book.Name == bookRev1.Name);

			Assert.IsTrue(anyResult);
		}

		[Test]
		public async Task DateContext_Query_AnyFunctionReturnsFalseIfNoneElementsMatchPredicate()
		{
			var bookRev1 = await BooksHelper.CreateBookAsync(publishYear: 2012);
			await BooksHelper.CreateBookAsync(bookRev1.Name, 2013);

			var anyResult = Context.GetTable<Book>().Any(book => book.Name == Guid.NewGuid().ToString());

			Assert.IsFalse(anyResult);
		}

		[Test]
		public async Task DateContext_Query_AllFunctionReturnsTrueIfAllElementsMatchPredicate()
		{
			var bookRev1 = await BooksHelper.CreateBookAsync(publishYear: 2012);
			await BooksHelper.CreateBookAsync(bookRev1.Name, 2013);

			var bookTable = Context.GetTable<Book>();
			var booksQuery = from record in bookTable where record.Name == bookRev1.Name select record;

			var allResult = booksQuery.All(book => book.Name == bookRev1.Name);
			Assert.IsTrue(allResult);
		}

		[Test]
		public async Task DateContext_Query_AllFunctionReturnsFalseIfAtLeastOneDoesNotMatchPredicate()
		{
			var bookRev1 = await BooksHelper.CreateBookAsync(publishYear: 2012, numPages: 123);
			await BooksHelper.CreateBookAsync(bookRev1.Name, 2013, numPages: bookRev1.NumPages);
			await BooksHelper.CreateBookAsync(bookRev1.Name, 2014, numPages: 124);

			var allResult = Context.GetTable<Book>().All(book => book.Name == bookRev1.Name && book.NumPages == bookRev1.NumPages);

			Assert.IsFalse(allResult);
		}

		[Test]
		public async Task DateContext_Query_AllFunctionReturnsFalseIfNoneElementsMatchPredicate()
		{
			var bookRev1 = await BooksHelper.CreateBookAsync(publishYear: 2012);
			await BooksHelper.CreateBookAsync(bookRev1.Name, 2013);

			var allResult = Context.GetTable<Book>().All(book => book.Name == Guid.NewGuid().ToString());

			Assert.IsFalse(allResult);
		}

		[Test]
		public async Task DateContext_Query_CountFunctionReturnsCorrectNumberOfRecordsOnSmallDataSets()
		{
			const int DataSetLength = 20;
			var bookRev1 = await BooksHelper.CreateBookAsync();
			
			await ParallelForAsync(1, DataSetLength, i => BooksHelper.CreateBookAsync(bookRev1.Name, bookRev1.PublishYear + i));

			var numberOfRecordsInDb = Context.GetTable<Book>().Count(book => book.Name == bookRev1.Name);

			Assert.AreEqual(DataSetLength, numberOfRecordsInDb);

            numberOfRecordsInDb = Context.GetTable<Book>().ToListAsync().Result.Count(book => book.Name == bookRev1.Name);

            Assert.AreEqual(DataSetLength, numberOfRecordsInDb);
        }
		
        [Test]
		public async Task DateContext_Query_LongCountFunctionReturnsCorrectNumberOfRecordsOnSmallDataSets()
		{
			const int DataSetLength = 20;
			var bookRev1 = await BooksHelper.CreateBookAsync();
			await ParallelForAsync(1, DataSetLength, i => BooksHelper.CreateBookAsync(bookRev1.Name, bookRev1.PublishYear + i));

			var numberOfRecordsInDb = Context.GetTable<Book>().LongCount(book => book.Name == bookRev1.Name);

			Assert.AreEqual(DataSetLength, numberOfRecordsInDb);
		}

        [Test]
        public async Task DateContext_RetrievesManyRecords()
        {
            const int DataSetLength = 500;
            var bookRev1 = await BooksHelper.CreateBookAsync();
            await ParallelForAsync(1, DataSetLength, async i => 
                {
                    try
                    {
                         await BooksHelper.CreateBookAsync(bookRev1.Name, bookRev1.PublishYear + i);
                    }
                    catch (ProvisionedThroughputExceededException)
                    {
                        Thread.Sleep(10000);
                        // trying one more time
                        await BooksHelper.CreateBookAsync(bookRev1.Name, bookRev1.PublishYear + i);
                    }
                });

            var allBooks = Context.GetTable<Book>().Where(book => book.Name == bookRev1.Name).ToList();
            Assert.AreEqual(DataSetLength, allBooks.Count);

            var query = from b in Context.GetTable<Book>() where b.Name == bookRev1.Name select b;
            allBooks = query.ToListAsync().Result;
            Assert.AreEqual(DataSetLength, allBooks.Count);
        }

	    [Test]
	    public async Task DateContext_Query_ReturnsProjection()
	    {
	        var bookRev1 = await BooksHelper.CreateBookAsync(
                numPages: 123,
                lastRentTime: DateTime.Today,
                popularityRating: Book.Popularity.AboveAverage,
                filmsBasedOnBook: new Dictionary<string, TimeSpan> {{ "Avatar 12", TimeSpan.FromMinutes(9000) }},
                publisher: new Book.PublisherDto { Title = "Avatar" }
            );

            var query = from b in Context.GetTable<Book>()
	            where b.Name == bookRev1.Name && b.PublishYear == bookRev1.PublishYear
	            select new {b.Author, b.NumPages, b.LastRentTime, b.PopularityRating, b.FilmsBasedOnBook, b.Publisher };

	        var bookProjection = query.Single();

	        Assert.AreEqual(bookRev1.Author, bookProjection.Author);
	        Assert.AreEqual(bookRev1.NumPages, bookProjection.NumPages);
	        Assert.AreEqual(bookRev1.LastRentTime, bookProjection.LastRentTime);
	        Assert.AreEqual(bookRev1.PopularityRating, bookProjection.PopularityRating);
	        Assert.AreEqual(bookRev1.FilmsBasedOnBook.Count, bookProjection.FilmsBasedOnBook.Count);
	        Assert.AreEqual(bookRev1.Publisher.Title, bookProjection.Publisher.Title);
	    }

	    [Test]
        // [ExpectedException(typeof(InvalidOperationException))]
	    public void DateContext_Query_ThrowsOnMultipleConditionsForTheSameField()
	    {
	        var query = from b in Context.GetTable<Book>()
	            where b.PublishYear > 2000 && b.PublishYear < 3000
                select b;

	        var array = query.ToArray();
	    }

        [Test]
        public async Task DateContext_QueryWithFilterExpressionReturnsExpectedResults()
        {
            var bookRev1 = await BooksHelper.CreateBookAsync(publishYear: 2012, numPages: 30);
            await BooksHelper.CreateBookAsync(bookRev1.Name, 2013, numPages: 20);
            await BooksHelper.CreateBookAsync(bookRev1.Name, 2014, numPages: 10);

            var filterExp1 = new Expression()
            {
                ExpressionStatement = "#N1 < :V1 AND #N1 > :V2",
                ExpressionAttributeNames = { { "#N1", "NumPages" } },
                ExpressionAttributeValues = { { ":V1", 1000 }, { ":V2", 1 } }
            };

            var filterExp2 = new Expression()
            {
                ExpressionStatement = "#N1 >= :V1",
                ExpressionAttributeNames = { { "#N1", "NumPages" } },
                ExpressionAttributeValues = { { ":V1", 20 } }
            };

            var filterExp3 = new Expression()
            {
                ExpressionStatement = "#N1 > :V1",
                ExpressionAttributeNames = { { "#N1", "NumPages" } },
                ExpressionAttributeValues = { { ":V1", 100 } }
            };

            var query1 = from b in Context.GetTable<Book>().WithFilterExpression(filterExp1)
                         where b.Name == bookRev1.Name
                         select b;

            var query2 = 
                (
                    from b in Context.GetTable<Book>()
                    where b.Name == bookRev1.Name
                    select b
                )
                .WithFilterExpression(filterExp2);

            var query3 = Context.GetTable<Book>().Where(b => b.Name == bookRev1.Name);
            query3.WithFilterExpression(filterExp3);

            Assert.AreEqual(3, query1.Count());
            Assert.AreEqual(3, query1.Count());

            Assert.AreEqual(2, query2.Count());
            Assert.AreEqual(2, query2.Count());

            Assert.AreEqual(0, query3.Count());
            Assert.AreEqual(0, query3.Count());

            Assert.IsTrue(Context.GetTable<Book>().Count() > 2);
        }

        [Test]
        public async Task DateContext_QueryOperationConfigCanBeCustomized()
        {
            var bookRev1 = await BooksHelper.CreateBookAsync(publishYear: 2012, author: "Gogol");
            await BooksHelper.CreateBookAsync(bookRev1.Name, 2013);

            var query = Context.GetTable<Book>().Where(b => b.Name == bookRev1.Name)
            .ConfigureQueryOperation(config => { config.FilterExpression = new Expression() { ExpressionStatement = "attribute_exists (Author)" }; });

            Assert.AreEqual(1, query.Count());
        }

        [Test]
        public async Task DateContext_ScanOperationConfigCanBeCustomized()
        {
            var bookRev1 = await BooksHelper.CreateBookAsync(publishYear: 2012, author: "Mogol");
            await BooksHelper.CreateBookAsync(bookRev1.Name, 2013);

            var query = Context.GetTable<Book>();

            var filterExp = new Expression()
            {
                ExpressionStatement = "#N1 = :V1 AND attribute_exists(#N2)",
                ExpressionAttributeNames = { { "#N1", "Name" }, { "#N2", "Author" } },
                ExpressionAttributeValues = { { ":V1", bookRev1.Name } }
            };
            query.ConfigureScanOperation(config => { config.FilterExpression = filterExp; });

            Assert.AreEqual(1, query.Count());
        }

        [Test]
        public async Task DateContext_GetOperationConfigCanBeCustomized()
        {
            var bookRev1 = await BooksHelper.CreateBookAsync(publishYear: 2012, numPages: 30);
            await BooksHelper.CreateBookAsync(bookRev1.Name, 2013, numPages: 20);

            var customQuery = Context.GetTable<Book>()
                .Where(b => b.Name == bookRev1.Name && b.PublishYear == bookRev1.PublishYear);

            customQuery = customQuery.ConfigureGetOperation(
                config =>
                    {
                        config.AttributesToGet = new List<string>{"Name", "PublishYear"};
                    });

            Assert.AreEqual(0, customQuery.Single().NumPages, "The AttributesToGet parameter wasn't applied and therefore all the item fields were returned");

            var normalQuery = Context.GetTable<Book>()
                .Where(b => b.Name == bookRev1.Name && b.PublishYear == bookRev1.PublishYear);

            Assert.AreEqual(30, normalQuery.Single().NumPages, "The customization callback wasn't cleaned out");
        }

        // ReSharper restore InconsistentNaming
    }
}
