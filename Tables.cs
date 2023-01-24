using Amazon.DynamoDBv2.Model;
using Amazon.DynamoDBv2;

namespace CsAwsDynamoDB;

internal class Tables : Logger
{

  private readonly AmazonDynamoDBClient Client = new AmazonDynamoDBClient();

  private readonly string TableName;

  public Tables(string tableName)
  {
    TableName = tableName;
  }

  public async Task Run()
  {
    await ShowTables();
    // await CreateTable();
    // await UpdateTable();
    // await DeleteTable();
  }

  private async Task ShowTables()
  {
    Log.Debug("Retrieving DynamoDB tables...");

    var tables = await Client.ListTablesAsync();

    foreach (var table in tables.TableNames)
    {
      var description = await Client.DescribeTableAsync(table);

      Log.Debug($"TableName: {description.Table.TableName}");
      Log.Debug($"  TableSizeBytes: {description.Table.TableSizeBytes}");
      Log.Debug($"  ItemCount: {description.Table.ItemCount}");
      Log.Debug($"  ProvisionedThroughput: {description.Table.ProvisionedThroughput.ReadCapacityUnits} RCUs / {description.Table.ProvisionedThroughput.WriteCapacityUnits} WCUs");

      foreach (var index in description.Table.LocalSecondaryIndexes)
      {
        Log.Debug($"  LocalIndexName: {index.IndexName} ({index.IndexSizeBytes} bytes)");
      }

      foreach (var index in description.Table.GlobalSecondaryIndexes)
      {
        Log.Debug($"  GlobalIndexName: {index.IndexName} ({index.IndexSizeBytes} bytes)");
      }
    }
  }

  private async Task CreateTable()
  {
    Log.Debug($"Creating table {TableName}...");

    var res = await Client.CreateTableAsync(
      TableName,
      new List<KeySchemaElement>() {
        new KeySchemaElement() {
          AttributeName= "user_id",
          KeyType="HASH"
        },
        new KeySchemaElement() {
          AttributeName= "timestamp",
          KeyType="RANGE"
        },
      },
      new List<AttributeDefinition>() {
        new AttributeDefinition() {
          AttributeName = "user_id",
          AttributeType = "S",
        },
        new AttributeDefinition() {
          AttributeName = "timestamp",
          AttributeType = "N",
        },
      },
      new ProvisionedThroughput()
      {
        WriteCapacityUnits = 1,
        ReadCapacityUnits = 1,
      }
    );

    Log.Debug(res.TableDescription.TableArn);
    Log.Debug(res.TableDescription.TableId);
    Log.Debug(res.TableDescription.TableName);
  }

  private async Task UpdateTable()
  {
    Log.Debug($"Updating table {TableName}...");

    var res = await Client.UpdateTableAsync(TableName, new ProvisionedThroughput()
    {
      WriteCapacityUnits = 1,
      ReadCapacityUnits = 1,
    });

    Log.Debug(res.TableDescription.TableStatus);
    Log.Debug(res.TableDescription.ProvisionedThroughput.ReadCapacityUnits);
    Log.Debug(res.TableDescription.ProvisionedThroughput.WriteCapacityUnits);
  }

  private async Task DeleteTable()
  {
    Log.Debug($"Deleting table {TableName}...");

    var res = await Client.DeleteTableAsync(TableName);

    Log.Debug(res.TableDescription.TableStatus);
  }
}