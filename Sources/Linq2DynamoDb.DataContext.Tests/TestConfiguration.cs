using Amazon;
using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.DataModel;
using Amazon.Runtime;
using log4net;

namespace Linq2DynamoDb.DataContext.Tests
{
	public static class TestConfiguration
	{
		public static readonly string TablePrefix = "";

        private static readonly ILog DataContextLogger = LogManager.GetLogger(typeof(DataContext));

        public static IAmazonDynamoDB GetDynamoDbClient() => new AmazonDynamoDBClient(
            new BasicAWSCredentials("ex", "ex"),
            new AmazonDynamoDBConfig
            {
                RegionEndpoint = RegionEndpoint.APSoutheast2,
                ServiceURL = "http://localhost:8000",
            }
        );

        public static DynamoDBContext GetDynamoDbContext()
        {
            return GetDynamoDbContext(GetDynamoDbClient());
        }

	    public static DynamoDBContext GetDynamoDbContext(IAmazonDynamoDB dynamoDbClient)
	    {
	        return new(dynamoDbClient, new DynamoDBContextConfig { TableNamePrefix = TablePrefix });
	    }

	    public static DataContext GetDataContext()
	    {
            return GetDataContext(GetDynamoDbClient());
	    }

        public static DataContext GetDataContext(IAmazonDynamoDB dynamoDbClient)
        {
            return GetDataContext(dynamoDbClient, TablePrefix);
        }

        public static DataContext GetDataContext(string tablePrefix)
        {
            return GetDataContext(GetDynamoDbClient(), tablePrefix);
        }

        public static DataContext GetDataContext(IAmazonDynamoDB dynamoDbClient, string tablePrefix)
        {
            var dataContext = new DataContext(dynamoDbClient, tablePrefix);
            dataContext.OnLog += DataContextLogger.Debug;

            return dataContext;
        }
	}
}
