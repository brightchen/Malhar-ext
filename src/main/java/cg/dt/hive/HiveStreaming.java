package cg.dt.hive;

import java.io.Serializable;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * https://cwiki.apache.org/confluence/display/Hive/Streaming+Data+Ingest
Streaming Requirements 
A few things are required to use streaming.
The following settings are required in hive-site.xml to enable ACID support for streaming:
hive.txn.manager = org.apache.hadoop.hive.ql.lockmgr.DbTxnManager
hive.compactor.initiator.on = true
hive.compactor.worker.threads > 0 
“stored as orc” must be specified during table creation. Only ORC storage format is supported currently.
tblproperties("transactional"="true") must be set on the table during creation.
The Hive table must be bucketed, but not sorted. So something like “clustered by (colName) into 10 buckets” must be specified during table creation. The number of buckets is ideally the same as the number of streaming writers.
User of the client streaming process must have the necessary permissions to write to the table or partition and create partitions in the table.
(Temporary requirements) When issuing queries on streaming tables, the client needs to set
hive.vectorized.execution.enabled  to  false     (for Hive version < 0.14.0)
hive.input.format  to  org.apache.hadoop.hive.ql.io.HiveInputFormat
 * @author bright
 *
 */
public class HiveStreaming
{
  public static class SingleRecord implements Serializable
  {
    private static final long serialVersionUID = -1426132792117389626L;

    public static final Collection<String> fields = Collections.unmodifiableCollection( Arrays.asList("OperatorCode", "OperatorName","Imsi", "isdn", "imei",  "deviceBrand", "deviceModel"));
    
    public final String id;
    public final String imsi;
    public final String isdn;
    public final String imei;
    public final String operatorName;
    public final String operatorCode; //carrier 
    public final String deviceBrand;   
    public final String deviceModel;
    
    protected SingleRecord()
    {
      this.id = "";
      this.imsi = "";
      this.isdn = "";
      this.imei = "";
      this.operatorName = "";
      this.operatorCode = "";
      this.deviceBrand = "";
      this.deviceModel = "";
    }
    
    public SingleRecord(int index)
    {
      this(""+index, "imsi"+index, "isdn"+index, "imei"+index, "operatorName"+index, "operatorCode"+index, "deviceBrand"+index, "deviceModel"+index);
    }
    
    public SingleRecord(Map<String,String> nameValueMap)
    {
      this.id = nameValueMap.get("id");
      this.imsi = nameValueMap.get("imsi");
      this.isdn = nameValueMap.get("isdn");
      this.imei = nameValueMap.get("imei");
      this.operatorName = nameValueMap.get("operatorName");
      this.operatorCode = nameValueMap.get("operatorCode");
      this.deviceBrand = nameValueMap.get("deviceBrand");
      this.deviceModel = nameValueMap.get("deviceModel");
    }
    
    public SingleRecord(String id, String imsi, String isdn, String imei, String operatorName, String operatorCode, String deviceBrand, String deviceModel)
    {
      this.id = id;
      this.imsi = imsi;
      this.isdn = isdn;
      this.imei = imei;
      this.operatorName = operatorName;
      this.operatorCode = operatorCode;
      this.deviceBrand = deviceBrand;
      this.deviceModel = deviceModel;
    }
    
    @Override
    public String toString()
    {
      return String.format("id:%s; imsi:%s; isdn:%s; imei:%s; operatorName:%s; operatorCode:%s; deviceBrand:%s; deviceModel:%s", 
          id, imsi, isdn, imei, operatorName, operatorCode, deviceBrand, deviceModel);
    }
    public String getImsi() {
      return imsi;
    }
    public String getIsdn() {
      return isdn;
    }
    public String getImei() {
      return imei;
    }
    public String getOperatorName() {
      return operatorName;
    }
    public String getOperatorCode() {
      return operatorCode;
    }
    public String getDeviceBrand() {
      return deviceBrand;
    }
    public String getDeviceModel() {
      return deviceModel;
    }
  }

  public static final int RECORDS = 10000;
  public static void main(String[] argv)
  {
    HiveStreaming hiveStreaming = new HiveStreaming();
    hiveStreaming.setup();
    
    long start = Calendar.getInstance().getTimeInMillis();
    
    for( int i=0; i<RECORDS; ++i)
    {
      SingleRecord record = new SingleRecord(i);
      hiveStreaming.processTuple(record);
    }
    
    long runTime = Calendar.getInstance().getTimeInMillis() - start;
    logger.info("{} records takes {} milli-seconds.", RECORDS, runTime);
  }
  
  
  private static final transient Logger logger = LoggerFactory.getLogger(HiveStreaming.class);

  private String host = "localhost";
  private int port = 10000;
  private String database = "streaming";
  private String tableName = "test";
  
  private boolean startOver = false;
  
  protected Connection connect;
  
  public void setup()
  {
    //create table;
    try
    {
      createTable();
    }
    catch(Exception e)
    {
      logger.error("create table '{}' failed.\n exception: {}", tableName, e.getMessage());
    }
  }
  
  protected Connection getConnect() throws SQLException, ClassNotFoundException
  {
    
    if(connect == null)
    {
      String url = HiveUtil.getUrl(host, port, database);
      connect = DriverManager.getConnection(url, "", "");    
    }
    return connect;
  }

  /**
   * 
     create table streaming.test 
       (id String, imsi string, isdn string, imei string, operatorCode string, operatorName string, deviceBrand string, deviceModel string) 
       clustered by (deviceBrand) into 10 buckets 
       STORED AS ORC 
       TBLPROPERTIES("transactional"="true")
       
   * @throws Exception
   */
  protected void createTable() throws Exception
  {
    Connection connect = getConnect();
    Statement stmt = connect.createStatement();
    
    ResultSet rs = stmt.executeQuery("SHOW TABLES LIKE '" + tableName + "'");
    //hive doesn't support first();
    //boolean hasTable = rs.first();
    boolean hasTable = false;
    try
    {
      hasTable = rs.next();
    }
    catch(SQLException e)
    {
      logger.warn(e.getMessage());
    }
    
    if(hasTable && startOver)
    {
      stmt.executeUpdate("drop table " + tableName);
      hasTable = false;
    }
    
    if(!hasTable)
    {
      //create table;
      //not support long?
      String tableSchema = " (id String, imsi string, isdn string, imei string, operatorCode string, operatorName string, deviceBrand string, deviceModel string) "
       + " clustered by (deviceBrand) into 10 buckets "
       + " STORED AS ORC "
       + " TBLPROPERTIES(\"transactional\"=\"true\") ";
      stmt.executeUpdate("create table " + tableName + tableSchema);
    }
    stmt.close();
  }
  
  private Statement insertStatement;
  protected Statement getInsertStatement() throws ClassNotFoundException, SQLException
  {
    if(insertStatement == null)
      insertStatement = getConnect().createStatement();
    return insertStatement;
  }

  private int batchCount = 1000;
  private int batchSize = 0;
  public void processTuple(SingleRecord tuple)
  {
    final String sqlValueFormat = "('%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s')";
    String sql = "insert into table " + tableName + " values " 
        + String.format( sqlValueFormat, tuple.id, tuple.imsi, tuple.isdn, tuple.imei, tuple.operatorCode, tuple.operatorName, tuple.deviceBrand, tuple.deviceModel );
    
    try {
      //hive doesn't support batch
      //getInsertStatement().addBatch(sql);
      getInsertStatement().executeUpdate(sql);
//      ++batchSize;
//      
//      if(batchSize >= batchCount)
//      {
//        insertStatement.executeBatch();
//        batchSize = 0;
//      }
      
    } catch (ClassNotFoundException | SQLException e) {
      logger.error(e.getMessage(), e);
    }
  }

}
