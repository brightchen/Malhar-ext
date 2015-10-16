package cg.dt.malharlib.op;

import cg.dt.malharlib.conf.DefaultConf;

public class DumpConf
{
  public String host = DefaultConf.instance.getHost();
  public int port = DefaultConf.instance.getPort();
  public String userName = DefaultConf.instance.getUserName();
  
  private static DumpConf instance;
  public static DumpConf instance()
  {
    if(instance == null)
    {
      synchronized(DumpConf.class)
      {
        if(instance == null)
          instance = new DumpConf();
      }
    }
    return instance;
  }
  
  protected DumpConf()
  {
    host = DefaultConf.instance.getHost();
    port = DefaultConf.instance.getPort();
    userName = DefaultConf.instance.getUserName();
  }
  
}
