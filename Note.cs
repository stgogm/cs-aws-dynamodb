using Amazon.DynamoDBv2.DataModel;

namespace CsAwsDynamoDB;

[DynamoDBTable("td_notes_sdk")]
class Note
{
  [DynamoDBHashKey]
  public string user_id { get; set; }

  [DynamoDBRangeKey]
  public long timestamp { get; set; }

  public string content { get; set; }

  public string title { get; set; }

  public int views { get; set; }
}