using System.Linq;
using System.Threading.Tasks;
using Linq2DynamoDb.DataContext.Tests.Entities;
using Linq2DynamoDb.DataContext.Tests.Helpers;
using NUnit.Framework;

namespace Linq2DynamoDb.DataContext.Tests.QueryTests
{
    public abstract class ConversionTestsCommon : DataContextTestBase
	{
        // ReSharper disable InconsistentNaming
		[Test]
		public async Task DateContext_Query_SupportsToList()
		{
			var book = await BooksHelper.CreateBookAsync();

			var bookTable = Context.GetTable<Book>();
			var booksQuery = from record in bookTable where record.Name == book.Name select record;

			var queryList = booksQuery.ToList();

			Assert.AreEqual(1, queryList.Count);

			var storedBook = queryList.First();

			Assert.AreEqual(book.Name, storedBook.Name);
		}

		[Test]
		public async Task DateContext_Query_SupportsToArray()
		{
			var book = await BooksHelper.CreateBookAsync();

			var bookTable = Context.GetTable<Book>();
			var booksQuery = from record in bookTable where record.Name == book.Name select record;

			var queryList = booksQuery.ToArray();

			Assert.AreEqual(1, queryList.Length);

			var storedBook = queryList.First();

			Assert.AreEqual(book.Name, storedBook.Name);
		}

		[Test]
		public async Task DateContext_Query_SupportsToDictionary()
		{
			var book = await BooksHelper.CreateBookAsync();

			var bookTable = Context.GetTable<Book>();
			var booksQuery = from record in bookTable where record.Name == book.Name select record;

			var i = 0;
			var queryList = booksQuery.ToDictionary(book1 => book1, book1 => i++);

			Assert.AreEqual(1, queryList.Count);

			var (key, value) = queryList.First();

			Assert.AreEqual(book.Name, key.Name);
			Assert.AreEqual(0, value);
		}

		// ReSharper restore InconsistentNaming
	}
}
