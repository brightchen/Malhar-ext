package cg.dt.malharlib.conf;

public class DefaultConf
{
  public static DefaultConf instance = new DefaultConf();
  
  private String host = "defaultHost";
  private int port = 0;
  private String userName = "defaultUser";
  
  private DefaultConf(){}
  
  public String getHost()
  {
    return host;
  }
  public void setHost(String host)
  {
    this.host = host;
  }
  public int getPort()
  {
    return port;
  }
  public void setPort(int port)
  {
    this.port = port;
  }
  public String getUserName()
  {
    return userName;
  }
  public void setUserName(String userName)
  {
    this.userName = userName;
  }
  
  
}
