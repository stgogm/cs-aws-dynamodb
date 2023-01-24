namespace CsAwsDynamoDB;

internal class Program
{
  const string TableName = "td_notes_sdk";

  private static async Task Main(string[] args)
  {
    // await new Tables(TableName).Run();
    await new Items(TableName).Run();
  }
}