package cg.dt.cassandra;

public class DataWarehouseConfig {
  protected String host = "localhost";
  protected int port = 9160;

  protected String userName = "bright";
  protected String password;

  protected String database = "telecomdemo";
  protected String tableName = "test";
  
  public String getHost() {
    return host;
  }
  public void setHost(String host) {
    this.host = host;
  }
  public int getPort() {
    return port;
  }
  public void setPort(int port) {
    this.port = port;
  }
  public String getUserName() {
    return userName;
  }
  public void setUserName(String userName) {
    this.userName = userName;
  }
  public String getPassword() {
    return password;
  }
  public void setPassword(String password) {
    this.password = password;
  }
  public String getDatabase() {
    return database;
  }
  public void setDatabase(String database) {
    this.database = database;
  }
  public String getTableName() {
    return tableName;
  }
  public void setTableName(String tableName) {
    this.tableName = tableName;
  }
  
  @Override
  public String toString()
  {
    return String.format("host=%s; port=%d; userName=%s; database=%s; tableName=%s ", host, port, userName, database, tableName );
  }
}
