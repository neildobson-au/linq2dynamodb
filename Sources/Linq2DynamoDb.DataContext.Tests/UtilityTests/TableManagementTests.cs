using System;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Threading.Tasks;
using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.Model;
using Linq2DynamoDb.DataContext.Tests.Entities;
using NUnit.Framework;

namespace Linq2DynamoDb.DataContext.Tests.UtilityTests
{
    [TestFixture]
    [Category(TestCategories.Slow)]
    public class TableManagementTests : DataContextTestBase
    {
        private string TablePrefix { get; set; }

        private string BooksTableName
        {
            get { return TablePrefix + typeof(Book).Name; }
        }

        private IAmazonDynamoDB DynamoDbClient { get; set; }

        public override void SetUp()
        {
            TablePrefix = typeof(TableManagementTests).Name + Guid.NewGuid();

            DynamoDbClient = TestConfiguration.GetDynamoDbClient();
            Context = TestConfiguration.GetDataContext(DynamoDbClient, TablePrefix);
        }

        public override async Task TearDown()
        {
            try
            {
                await DynamoDbClient.DeleteTableAsync(new DeleteTableRequest { TableName = BooksTableName });
                Logger.DebugFormat("Table {0} delete initiated", BooksTableName);
            }
            catch (ResourceNotFoundException)
            {
                Logger.DebugFormat("Table {0} does not exist", BooksTableName);
            }
        }

        [Test]
        public async Task CreatesTableWithHashKey()
        {
            // arrange
            var args = new CreateTableArgs<Book>(book => book.Name);

            // act
            await Context.CreateTableIfNotExistsAsync(args);
            var tableData = await DynamoDbClient.DescribeTableAsync(new DescribeTableRequest { TableName = BooksTableName });

            // assert
            Assert.IsNotNull(tableData, "Table was not created");
        }

        [Test]
        public async Task CreatesTableWithHashAndRangeKeys()
        {
            // arrange
            var args = new CreateTableArgs<Book>(book => book.Name, book => book.PublishYear);

            // act
            await Context.CreateTableIfNotExistsAsync(args);
            var tableData = await DynamoDbClient.DescribeTableAsync(new DescribeTableRequest { TableName = BooksTableName });

            // assert
            Assert.IsNotNull(tableData, "Table was not created");
        }

        [Test]
        public async Task CreatesTableWithLocalSecondaryIndexes()
        {
            // arrange
            var args = new CreateTableArgs<Book>(book => book.Name, book => book.PublishYear, book => book.NumPages, book => book.PopularityRating);

            // act
            await Context.CreateTableIfNotExistsAsync(args);
            var tableData = await DynamoDbClient.DescribeTableAsync(new DescribeTableRequest { TableName = BooksTableName });

            // assert
            Assert.IsNotNull(tableData, "Table was not created");
            var secondaryIndexes = tableData.Table.LocalSecondaryIndexes;
            Assert.AreEqual(2, secondaryIndexes.Count, "Expected 2 local secondary indexes to be created");
            Assert.IsTrue(secondaryIndexes.Any(description => description.IndexName.Contains("NumPages")));
            Assert.IsTrue(secondaryIndexes.Any(description => description.IndexName.Contains("PopularityRating")));
        }

        [Test]
        public async Task CreatesTableWithGlobalSecondaryIndexes()
        {
            // arrange
            var args = new CreateTableArgs<Book>
            (
                book => book.Name,
                book => book.PublishYear,
                localSecondaryIndexFieldExps: null,
                globalSecondaryIndexFieldExps: new GlobalSecondaryIndexDefinitions<Book>
                {
                    book => new GlobalSecondaryIndexDefinition
                    {
                        HashKeyField = book.PublishYear 
                    },
                    book => new GlobalSecondaryIndexDefinition
                    {
                        HashKeyField = book.Author, 
                        RangeKeyField = book.NumPages
                    }
                }
            );

            // act
            await Context.CreateTableIfNotExistsAsync(args);
            var tableData = await DynamoDbClient.DescribeTableAsync(new DescribeTableRequest { TableName = BooksTableName });

            // assert
            Assert.IsNotNull(tableData, "Table was not created");
            var secondaryIndexes = tableData.Table.GlobalSecondaryIndexes;
            Assert.AreEqual(2, secondaryIndexes.Count, "Expected 2 global secondary indexes to be created");
            Assert.IsTrue(secondaryIndexes.Any(description => description.IndexName.Contains("PublishYear")));
            Assert.IsTrue
            (
                secondaryIndexes.Any
                (
                    description =>
                        description.IndexName.Contains("Author")
                        &&
                        description.IndexName.Contains("NumPages")
                )
            );
        }

        [Test]
        public async Task CreatesTableWithConstantCapacity()
        {
            // arrange
            const long readCapacity = 2;
            const long writeCapacity = 1;
            var args = new CreateTableArgs<Book>(readCapacity, writeCapacity, book => book.Name, book => book.PublishYear);

            // act
            await Context.CreateTableIfNotExistsAsync(args);
            var tableData = await DynamoDbClient.DescribeTableAsync(new DescribeTableRequest { TableName = BooksTableName });

            // assert
            Assert.IsNotNull(tableData, "Table was not created");
            var tableCapacity = tableData.Table.ProvisionedThroughput;
            Assert.AreEqual(readCapacity, tableCapacity.ReadCapacityUnits);
            Assert.AreEqual(writeCapacity, tableCapacity.WriteCapacityUnits);
        }

        [Test]
        public void DoesNotThrowIfTableAlreadyExists()
        {
            // arrange
            var args = new CreateTableArgs<Book>(book => book.Name, book => book.PublishYear);
            Context.CreateTableIfNotExists(args);

            // act
            Context.CreateTableIfNotExists(args);
        }

        [Test]
        public async Task CreatesTableWithGlobalSecondaryIndexUsingConstantCapacity()
        {
            // arrange
            const long readCapacity = 2;
            const long writeCapacity = 1;
            var args = new CreateTableArgs<Book>
            (
                book => book.Name,
                book => book.PublishYear,
                localSecondaryIndexFieldExps: null,
                globalSecondaryIndexFieldExps: new GlobalSecondaryIndexDefinitions<Book>
                {
                    book => new GlobalSecondaryIndexDefinition
                    {
                        HashKeyField = book.Author, 
                        RangeKeyField = book.NumPages,
                        ReadCapacityUnits = readCapacity,
                        WriteCapacityUnits = writeCapacity
                    }
                }
            );

            // act
            await Context.CreateTableIfNotExistsAsync(args);
            var tableData = await DynamoDbClient.DescribeTableAsync(new DescribeTableRequest { TableName = BooksTableName });

            // assert
            Assert.IsNotNull(tableData, "Table was not created");
            var secondaryIndexes = tableData.Table.GlobalSecondaryIndexes;
            Assert.AreEqual(1, secondaryIndexes.Count, "Expected 1 global secondary index to be created");
            var indexThroughput = secondaryIndexes[0].ProvisionedThroughput;
            Assert.AreEqual(readCapacity, indexThroughput.ReadCapacityUnits);
            Assert.AreEqual(writeCapacity, indexThroughput.WriteCapacityUnits);
        }

        [Test]
        [MethodImpl(MethodImplOptions.NoOptimization | MethodImplOptions.NoInlining)]
        public async Task CreatesTableWithRuntimeVariableCapacity()
        {
            // arrange
            Func<long> getReadCapacityFunc = () => 2;
            Func<long> getWriteCapacityFunc = () => 1;
            var args = new CreateTableArgs<Book>(getReadCapacityFunc(), getWriteCapacityFunc(), book => book.Name, book => book.PublishYear);

            // act
            await Context.CreateTableIfNotExistsAsync(args);
            var tableData = await DynamoDbClient.DescribeTableAsync(new DescribeTableRequest { TableName = BooksTableName });

            // assert
            Assert.IsNotNull(tableData, "Table was not created");
            var tableCapacity = tableData.Table.ProvisionedThroughput;
            Assert.AreEqual(getReadCapacityFunc(), tableCapacity.ReadCapacityUnits);
            Assert.AreEqual(getWriteCapacityFunc(), tableCapacity.WriteCapacityUnits);
        }

        [Test]
        [MethodImpl(MethodImplOptions.NoOptimization | MethodImplOptions.NoInlining)]
        public async Task CreatesTableWithGlobalSecondaryIndexUsingRuntimeVariableCapacity()
        {
            // arrange
            Func<long> getReadCapacityFunc = () => 2;
            Func<long> getWriteCapacityFunc = () => 1;
            var args = new CreateTableArgs<Book>
            (
                book => book.Name,
                book => book.PublishYear,
                localSecondaryIndexFieldExps: null,
                globalSecondaryIndexFieldExps: new GlobalSecondaryIndexDefinitions<Book>
                {
                    book => new GlobalSecondaryIndexDefinition
                    {
                        HashKeyField = book.Author, 
                        RangeKeyField = book.NumPages,
                        ReadCapacityUnits = getReadCapacityFunc(),
                        WriteCapacityUnits = getWriteCapacityFunc()
                    }
                }
            );

            // act
            await Context.CreateTableIfNotExistsAsync(args);
            var tableData = await DynamoDbClient.DescribeTableAsync(new DescribeTableRequest { TableName = BooksTableName });

            // assert
            Assert.IsNotNull(tableData, "Table was not created");
            var secondaryIndexes = tableData.Table.GlobalSecondaryIndexes;
            Assert.AreEqual(1, secondaryIndexes.Count, "Expected 1 global secondary index to be created");
            var indexThroughput = secondaryIndexes[0].ProvisionedThroughput;
            Assert.AreEqual(getReadCapacityFunc(), indexThroughput.ReadCapacityUnits);
            Assert.AreEqual(getWriteCapacityFunc(), indexThroughput.WriteCapacityUnits);
        }

        [Test]
        public async Task DeletesExistingTable()
        {
            // arrange
            var args = new CreateTableArgs<Book>(book => book.Name, book => book.PublishYear);
            await Context.CreateTableIfNotExistsAsync(args);

            // act
            await Context.DeleteTableAsync<Book>();

            try
            {
                await DynamoDbClient.DescribeTableAsync(new DescribeTableRequest { TableName = BooksTableName });

                Assert.Fail();
            }
            catch (ResourceNotFoundException)
            {
            }
        }
    }
}