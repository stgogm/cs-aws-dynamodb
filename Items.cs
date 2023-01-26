using Amazon.DynamoDBv2.DocumentModel;
using Amazon.DynamoDBv2.DataModel;
using Amazon.DynamoDBv2.Model;
using Amazon.DynamoDBv2;
using Xaevik.Cuid;

namespace CsAwsDynamoDB;

internal class Items : Logger
{
  private readonly AmazonDynamoDBClient Client = new AmazonDynamoDBClient();

  private readonly DynamoDBContext Context;

  private readonly string TableName;

  private readonly Table NotesTable;

  public Items(string tableName)
  {
    NotesTable = Table.LoadTable(Client, tableName);
    Context = new DynamoDBContext(Client);

    TableName = tableName;
  }

  public async Task Run()
  {
    // await Put();
    // await Update();
    // await Delete();
    // await ConditionalPut();
    // await IncrementViews();
    await Query();
  }

  async Task Put()
  {
    Log.Debug("Inserting item using OPM...");

    await Context.SaveAsync(new Note
    {
      content = "This is a new content created with Object Persistence Model",
      timestamp = DateTimeOffset.UtcNow.ToUnixTimeSeconds(),
      user_id = new Cuid2(8).ToString(),
      title = "Title",
      views = 0,
    });
  }

  async Task ConditionalPut()
  {
    Log.Debug("Conditionally putting item...");

    var timestamp = DateTimeOffset.UtcNow.ToUnixTimeSeconds();

    await NotesTable.PutItemAsync(new Document {
      { "user_id", new Cuid2(8).ToString() },
      { "content", "This is a new content" },
      { "timestamp", timestamp },
      { "title", "Title" },
      { "views", 0 },
    },
    new PutItemOperationConfig
    {
      ConditionalExpression = new Expression
      {
        ExpressionStatement = "#t <> :t",
        ExpressionAttributeValues = {
          { ":t", timestamp },
        },
        ExpressionAttributeNames = {
          { "#t", "timestamp" }
        }
      }
    });
  }

  async Task Update()
  {
    Log.Debug("Updating item...");

    await Context.SaveAsync(new Note
    {
      // Key
      user_id = "bda72c62-2a3e-4725-83a1-04484e172833",
      timestamp = 1674584344,
      // Updates
      content = "Some useful and interesting content from OPM",
      title = "Some title",
    });
  }

  async Task IncrementViews()
  {
    // var Request = new UpdateItemRequest()
    // {
    //   UpdateExpression = "ADD #v :i",
    //   TableName = TableName,
    //   ExpressionAttributeValues = new Dictionary<string, AttributeValue> {
    //     { ":i", new AttributeValue { N = "1" } }
    //   },
    //   ExpressionAttributeNames = new Dictionary<string, string> {
    //     { "#v", "views" }
    //   },
    //   Key = new Dictionary<string, AttributeValue> {
    //     { "user_id", new AttributeValue { S = "kk3y6fqc" } },
    //     { "timestamp", new AttributeValue { N = "1674674868" } }
    //   },
    // };

    Log.Debug("Increment views...");

    // var res = await Client.UpdateItemAsync(Request);

    // TODO: Looks like there's a bug on the SDK here...
    // https://github.com/aws/aws-sdk-net/issues/2528
    var doc = new Document {
      { "user_id", "kk3y6fqc" },
      { "timestamp", 1674674868 },
    };

    var config = new UpdateItemOperationConfig
    {
      ReturnValues = ReturnValues.AllNewAttributes,
      ConditionalExpression = new Expression
      {
        ExpressionStatement = "ADD #v :i",
        ExpressionAttributeNames = {
          { "#v", "views" },
        },
        ExpressionAttributeValues = {
          { ":i", 1 },
        },
      },
    };

    await NotesTable.UpdateItemAsync(doc, config);
  }

  async Task Query()
  {
    var hashKey = new Primitive("kk3y6fqc");
    var filter = new QueryFilter("timestamp", QueryOperator.GreaterThanOrEqual, 1674674868);
    var search = NotesTable.Query(hashKey, filter);
    var res = search.GetNextSetAsync();

    System.Console.WriteLine(res.Result.ToJsonPretty());
  }

  async Task Delete()
  {
    Log.Debug("Deleting item...");

    await NotesTable.DeleteItemAsync(new Document {
      { "user_id", "bda72c62-2a3e-4725-83a1-04484e172833" },
      { "timestamp", "1674584344" },
    });
  }
}