using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading.Tasks;
using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.DataModel;
using Amazon.DynamoDBv2.Model;
using Linq2DynamoDb.DataContext.Tests.Entities;
using log4net;

namespace Linq2DynamoDb.DataContext.Tests.Helpers {
    public static class BookPocosHelper {
        private static readonly ILog Logger = LogManager.GetLogger(typeof(BooksHelper));

        private static readonly IAmazonDynamoDB DynamoDbClient = TestConfiguration.GetDynamoDbClient();
        private static readonly DynamoDBContext PersistenceContext = TestConfiguration.GetDynamoDbContext();
        private static ConcurrentQueue<BookPoco> _recordsForCleanup;

        public static async Task StartSessionAsync() {
            await CreateBooksTableAsync(TestConfiguration.TablePrefix + "BookPoco");
            _recordsForCleanup = new ConcurrentQueue<BookPoco>();
        }

        public static async Task CleanSessionAsync() {
            Logger.DebugFormat("Removing {0} records from DynamoDb", _recordsForCleanup.Count);

            foreach (var book in _recordsForCleanup)
            {
                await PersistenceContext.DeleteAsync(book);
            }

            _recordsForCleanup = new ConcurrentQueue<BookPoco>();
        }

        public static async Task<BookPoco> CreateBookPocoAsync(
            string name = null,
            int publishYear = default(int),
            string author = default(string),
            int numPages = default(int),
            BookPoco.Popularity popularityRating = default(BookPoco.Popularity),
            BookPoco.Stars userFeedbackRating = default(BookPoco.Stars),
            List<string> rentingHistory = default(List<string>),
            IDictionary<string, TimeSpan> filmsBasedOnBook = default(IDictionary<string, TimeSpan>),
            DateTime lastRentTime = default(DateTime),
            bool persistToDynamoDb = true,
            BookPoco.PublisherDto publisher = default(BookPoco.PublisherDto),
            List<BookPoco.ReviewDto> reviews = default(List<BookPoco.ReviewDto>)) {
            name ??= "TestBook" + Guid.NewGuid();

            var book = new BookPoco {
                Name = name,
                PublishYear = publishYear,
                Author = author,
                NumPages = numPages,
                PopularityRating = popularityRating,
                UserFeedbackRating = userFeedbackRating,
                RentingHistory = rentingHistory,
                FilmsBasedOnBook = filmsBasedOnBook,
                LastRentTime = lastRentTime,
                Publisher = publisher,
                ReviewsList = reviews,
            };

            if (persistToDynamoDb) {
                Logger.DebugFormat("Persisting book: {0}", book.Name);
                await PersistenceContext.SaveAsync(book);
            } else {
                Logger.DebugFormat("Created in-memory book: {0}", book.Name);
            }

            if (_recordsForCleanup != null) {
                _recordsForCleanup.Enqueue(book);
            }

            return book;
        }

        public static async Task CreateBooksTableAsync(string tableName) {
            try {
                await DynamoDbClient.CreateTableAsync(
                    new CreateTableRequest {
                        TableName = tableName,
                        AttributeDefinitions =
                            new List<AttributeDefinition>
                            {
                                new AttributeDefinition { AttributeName = "Name", AttributeType = "S" },
                                new AttributeDefinition { AttributeName = "PublishYear", AttributeType = "N" }
                            },
                        ProvisionedThroughput = new ProvisionedThroughput { ReadCapacityUnits = 5, WriteCapacityUnits = 5 },
                        KeySchema =
                            new List<KeySchemaElement>
                            {
                                new KeySchemaElement { AttributeName = "Name", KeyType = "HASH" },
                                new KeySchemaElement { AttributeName = "PublishYear", KeyType = "RANGE" }
                            }
                    });

                Logger.DebugFormat("Created table {0}", tableName);
            } catch {
                Logger.DebugFormat("Table already existed {0}", tableName);
            }
        }
    }
}
