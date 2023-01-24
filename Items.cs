using Amazon.DynamoDBv2.Model;
using Amazon.DynamoDBv2;

namespace CsAwsDynamoDB;

internal class Items : Logger
{
  private readonly AmazonDynamoDBClient Client = new AmazonDynamoDBClient();

  private readonly string TableName;

  public Items(string tableName)
  {
    TableName = tableName;
  }

  public async Task Run()
  {
    // await Put();
    // await Update();
    await Delete();
  }

  async Task Put()
  {
    var Item = new Dictionary<string, AttributeValue>();

    Item.Add("timestamp", new AttributeValue() { N = DateTimeOffset.UtcNow.ToUnixTimeSeconds().ToString() });
    Item.Add("user_id", new AttributeValue(Guid.NewGuid().ToString()));

    Log.Debug("Inserting item...");

    var res = await Client.PutItemAsync(TableName, Item);
  }

  async Task Update()
  {
    var Updates = new Dictionary<string, AttributeValueUpdate>();
    var Key = new Dictionary<string, AttributeValue>();

    Key.Add("user_id", new AttributeValue("bda72c62-2a3e-4725-83a1-04484e172833"));
    Key.Add("timestamp", new AttributeValue() { N = "1674584344" });

    Updates.Add("content", new AttributeValueUpdate(new AttributeValue("Some useful and interesting content"), AttributeAction.PUT));
    Updates.Add("title", new AttributeValueUpdate(new AttributeValue("Some title"), AttributeAction.PUT));

    Log.Debug("Updating item...");

    var res = await Client.UpdateItemAsync(TableName, Key, Updates);
  }

  async Task Delete()
  {
    var Key = new Dictionary<string, AttributeValue>();

    Key.Add("user_id", new AttributeValue("bda72c62-2a3e-4725-83a1-04484e172833"));
    Key.Add("timestamp", new AttributeValue() { N = "1674584344" });

    Log.Debug("Deleting item...");

    var res = await Client.DeleteItemAsync(TableName, Key);
  }
}