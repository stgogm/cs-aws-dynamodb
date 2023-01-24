using Amazon.DynamoDBv2.DataModel;
using Amazon.DynamoDBv2.Model;
using Amazon.DynamoDBv2;

namespace CsAwsDynamoDB;

internal class Items : Logger
{
  private readonly AmazonDynamoDBClient Client = new AmazonDynamoDBClient();

  private readonly DynamoDBContext Context;

  private readonly string TableName;

  public Items(string tableName)
  {
    Context = new DynamoDBContext(Client);
    TableName = tableName;
  }

  public async Task Run()
  {
    await OpmPut();
    // await Put();
    // await Update();
    // await Delete();
    // await ConditionalPut();
    // await IncrementViews();
  }

  async Task Put()
  {
    var Item = new Dictionary<string, AttributeValue> {
      { "timestamp", new AttributeValue { N = DateTimeOffset.UtcNow.ToUnixTimeSeconds().ToString() } },
      { "user_id", new AttributeValue { S = Guid.NewGuid().ToString() } },
      { "content", new AttributeValue { S = "This is a new content" } },
      { "title", new AttributeValue { S = "Title" } },
      { "views", new AttributeValue { N = "0" } }
    };

    Log.Debug("Inserting item...");

    var res = await Client.PutItemAsync(TableName, Item);

    Log.Debug($"user_id: {Item["user_id"].S}");
    Log.Debug($"timestamp: {Item["timestamp"].N}");
    Log.Debug($"content: {Item["content"].S}");
    Log.Debug($"title: {Item["title"].S}");
    Log.Debug($"views: {Item["views"].N}");
  }

  async Task OpmPut()
  {
    Log.Debug("Inserting item using OPM...");

    await Context.SaveAsync(new Note
    {
      content = "This is a new content created with Object Persistence Model",
      timestamp = DateTimeOffset.UtcNow.ToUnixTimeSeconds(),
      user_id = Guid.NewGuid().ToString(),
      title = "Title",
      views = 0,
    });
  }

  async Task ConditionalPut()
  {
    var Timestamp = DateTimeOffset.UtcNow.ToUnixTimeSeconds().ToString();
    var Request = new PutItemRequest()
    {
      ConditionExpression = "#t <> :t",
      TableName = TableName,
      ExpressionAttributeValues = new Dictionary<string, AttributeValue> {
        { ":t", new AttributeValue { N = Timestamp } },
      },
      ExpressionAttributeNames = new Dictionary<string, string> {
        { "#t", "timestamp" }
      },
      Item = new Dictionary<string, AttributeValue> {
        { "user_id", new AttributeValue { S = Guid.NewGuid().ToString() } },
        { "content", new AttributeValue { S = "This is a new content" } },
        { "timestamp", new AttributeValue { N = Timestamp } },
        { "title", new AttributeValue { S = "Title" } }
      },
    };

    Log.Debug("Conditionally putting item...");

    var res = await Client.PutItemAsync(Request);
  }

  async Task Update()
  {
    var Updates = new Dictionary<string, AttributeValueUpdate> {
      { "content", new AttributeValueUpdate(new AttributeValue { S = "Some useful and interesting content" }, AttributeAction.PUT) },
      { "title", new AttributeValueUpdate(new AttributeValue { S = "Some title" }, AttributeAction.PUT) }
    };

    var Key = new Dictionary<string, AttributeValue> {
      { "user_id", new AttributeValue { S = "bda72c62-2a3e-4725-83a1-04484e172833" } },
      { "timestamp", new AttributeValue { N = "1674584344" } }
    };

    Log.Debug("Updating item...");

    var res = await Client.UpdateItemAsync(TableName, Key, Updates);

    Log.Debug($"user_id: {res.Attributes["user_id"].S}");
    Log.Debug($"timestamp: {res.Attributes["timestamp"].N}");
    Log.Debug($"content: {res.Attributes["content"].S}");
    Log.Debug($"title: {res.Attributes["title"].S}");
    Log.Debug($"views: {res.Attributes["views"].N}");
  }

  async Task IncrementViews()
  {
    var Request = new UpdateItemRequest()
    {
      UpdateExpression = "set #views = #views + :inc",
      TableName = TableName,
      ExpressionAttributeValues = new Dictionary<string, AttributeValue> {
        { ":inc", new AttributeValue { N = "1" } }
      },
      ExpressionAttributeNames = new Dictionary<string, string> {
        { "#views", "views" }
      },
      Key = new Dictionary<string, AttributeValue> {
        { "user_id", new AttributeValue { S = "e8e7f21e-4519-43b3-bea1-d4e966cb3544" } },
        { "timestamp", new AttributeValue { N = "1674586925" } }
      },
    };

    Log.Debug("Increment views...");

    var res = await Client.UpdateItemAsync(Request);
  }

  async Task Delete()
  {
    var Key = new Dictionary<string, AttributeValue> {
      { "user_id", new AttributeValue { S = "bda72c62-2a3e-4725-83a1-04484e172833" } },
      { "timestamp", new AttributeValue { N = "1674584344" } }
    };

    Log.Debug("Deleting item...");

    var res = await Client.DeleteItemAsync(TableName, Key);
  }
}