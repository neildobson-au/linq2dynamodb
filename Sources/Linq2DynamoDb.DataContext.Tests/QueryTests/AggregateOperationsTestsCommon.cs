using System;
using System.Linq;
using System.Threading.Tasks;
using Linq2DynamoDb.DataContext.Tests.Entities;
using Linq2DynamoDb.DataContext.Tests.Helpers;
using NUnit.Framework;

namespace Linq2DynamoDb.DataContext.Tests.QueryTests
{
    public abstract class AggregateOperationsTestsCommon : DataContextTestBase
	{
		// ReSharper disable InconsistentNaming
		[Test]
		public async Task DateContext_Query_MaxFunctionReturnsCorrectValue()
		{
			var bookRev1 = await BooksHelper.CreateBookAsync(publishYear: 2012);
			await BooksHelper.CreateBookAsync(bookRev1.Name, 2013);
			await BooksHelper.CreateBookAsync(bookRev1.Name, 2014);
			await BooksHelper.CreateBookAsync(bookRev1.Name, 2015);
			var bookRev5 = await BooksHelper.CreateBookAsync(bookRev1.Name, 2016);

			var bookTable = Context.GetTable<Book>();
			var booksQuery = from record in bookTable where record.Name == bookRev1.Name select record;

			var maxPublishYear = booksQuery.Max(book => book.PublishYear);

			Assert.AreEqual(bookRev5.PublishYear, maxPublishYear);
		}

		[Test]
		// [ExpectedException(typeof(InvalidOperationException), ExpectedMessage = "Sequence contains no elements")]
		public void DateContext_Query_MaxFunctionReturnsExceptionIfNoElementsPresent()
		{
			var bookTable = Context.GetTable<Book>();
			var booksQuery = from record in bookTable where record.Name == Guid.NewGuid().ToString() select record;

			// ReSharper disable once ReturnValueOfPureMethodIsNotUsed
			booksQuery.Max(book => book.PublishYear);
		}

		[Test]
		public async Task DateContext_Query_MinFunctionReturnsCorrectValue()
		{
			var bookRev1 = await BooksHelper.CreateBookAsync(publishYear: 2012);
			await BooksHelper.CreateBookAsync(bookRev1.Name, 2013);
			await BooksHelper.CreateBookAsync(bookRev1.Name, 2014);
			await BooksHelper.CreateBookAsync(bookRev1.Name, 2015);
			await BooksHelper.CreateBookAsync(bookRev1.Name, 2016);

			var bookTable = Context.GetTable<Book>();
			var booksQuery = from record in bookTable where record.Name == bookRev1.Name select record;

			var maxPublishYear = booksQuery.Min(book => book.PublishYear);

			Assert.AreEqual(bookRev1.PublishYear, maxPublishYear);
		}

		[Test]
		// [ExpectedException(typeof(InvalidOperationException), ExpectedMessage = "Sequence contains no elements")]
		public void DateContext_Query_MinFunctionReturnsExceptionIfNoElementsPresent()
		{
			var bookTable = Context.GetTable<Book>();
			var booksQuery = from record in bookTable where record.Name == Guid.NewGuid().ToString() select record;

			// ReSharper disable once ReturnValueOfPureMethodIsNotUsed
			booksQuery.Min(book => book.PublishYear);
		}

		[Test]
		public async Task DateContext_Query_AverageFunctionReturnsCorrectValue()
		{
			var bookRev1 = await BooksHelper.CreateBookAsync(publishYear: 2012);
			await BooksHelper.CreateBookAsync(bookRev1.Name, 2013);
			var bookRev3 = await BooksHelper.CreateBookAsync(bookRev1.Name, 2014);
			await BooksHelper.CreateBookAsync(bookRev1.Name, 2015);
			await BooksHelper.CreateBookAsync(bookRev1.Name, 2016);

			var bookTable = Context.GetTable<Book>();
			var booksQuery = from record in bookTable where record.Name == bookRev1.Name select record;

			var maxPublishYear = booksQuery.Average(book => book.PublishYear);

			Assert.AreEqual(bookRev3.PublishYear, maxPublishYear);
		}

		[Test]
		// [ExpectedException(typeof(InvalidOperationException), ExpectedMessage = "Sequence contains no elements")]
		public void DateContext_Query_AverageFunctionReturnsExceptionIfNoElementsPresent()
		{
			var bookTable = Context.GetTable<Book>();
			var booksQuery = from record in bookTable where record.Name == Guid.NewGuid().ToString() select record;

			// ReSharper disable once ReturnValueOfPureMethodIsNotUsed
			booksQuery.Average(book => book.PublishYear);
		}

		// ReSharper restore InconsistentNaming
	}
}
