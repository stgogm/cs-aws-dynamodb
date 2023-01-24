using log4net.Config;
using log4net;

class Logger
{
  public ILog Log;

  public Logger()
  {
    Log = LogManager.GetLogger(this.ToString());

    BasicConfigurator.Configure();
  }
}