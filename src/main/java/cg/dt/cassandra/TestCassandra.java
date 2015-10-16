package cg.dt.cassandra;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.exceptions.DriverException;

public class TestCassandra
{
  public static final int TOTAL_SIZE = 100000;
  public static final int BATCH_SIZE = 100;
  
  public static void main(String[] argv)
  {
    TestCassandra test = new TestCassandra();
    test.cassandraConfig.host = "localhost";
    test.cassandraConfig.port = 9160;
    
    test.setup();
    
    test.testBatch();
    test.testOneByOne();
    test.testOneByOne();
    test.testBatch();
  }
  
  protected void testBatch()
  {
    Calendar start = Calendar.getInstance();
    
    List<TestData> datas = new ArrayList<TestData>(BATCH_SIZE);
    for(int index=0; index<TOTAL_SIZE; ++index)
    {
      datas.add(new TestData(index));
      
      if(datas.size() == BATCH_SIZE)
      {
        insertData(datas);
        datas.clear();
      }
    }
    
    Calendar end = Calendar.getInstance();
    logger.info("Batch started at {}, end at {} takes {} milli-seconds", start.getTimeInMillis(), end.getTimeInMillis(), end.getTimeInMillis()-start.getTimeInMillis() ); 
  }
  
  protected void testOneByOne()
  {
    Calendar start = Calendar.getInstance();
    
    for(int index=0; index<TOTAL_SIZE; ++index)
    {
      insertData(new TestData(index));
    }
    
    Calendar end = Calendar.getInstance();
    logger.info("OneByOne started at {}, end at {} takes {} milli-seconds", start.getTimeInMillis(), end.getTimeInMillis(), end.getTimeInMillis()-start.getTimeInMillis() ); 
  }
  
  
  private static final transient Logger logger = LoggerFactory.getLogger(TestCassandra.class);
  
  public static class TestData
  {
    public String name;
    private int age;
    public String address;
    public String desc;
    
    public TestData(int index)
    {
      name = "name" + index;
      age = index;
      address = "address" + index;
      desc = "desc" + index;
    }
  }
  
  protected DataWarehouseConfig cassandraConfig;
  protected String sqlCommand;
  
  protected int batchSize = 1000;
  protected transient Session session;
  protected PreparedStatement preparedStatement;
  protected long id = 0;
  
  public TestCassandra()
  {
    cassandraConfig = new DataWarehouseConfig();
  }
  
  public void setup()
  {
    logger.info("setup() starting"); 
    createSession();
    createTables();
    createSqlFormat();

    preparedStatement = prepareStatement();
    logger.info("setup() done."); 
  }
  
  public void insertData(List<TestData> datas)
  {
    BatchStatement batchStat = new BatchStatement();
    for(TestData data : datas)
    {
      batchStat.add(setStatementParameters(preparedStatement, data));
    }
    session.execute(batchStat);
  }
  
  public void insertData(TestData data)
  {
    session.execute(setStatementParameters(preparedStatement, data));
  }

  
  protected void createBusinessTables(Session session)
  {
    String createTable = "CREATE TABLE IF NOT EXISTS " + cassandraConfig.getDatabase() + "." + cassandraConfig.getTableName()
        + " ( id bigint primary key, name text, age int, address text, detail text);";
    session.execute(createTable);
    logger.info("created table: {}",cassandraConfig.getDatabase() + "." + cassandraConfig.getTableName());
  }
  
  protected String createSqlFormat()
  {
    sqlCommand = "INSERT INTO " + cassandraConfig.getDatabase() + "."
        + cassandraConfig.getTableName()
        + " (id, name, age, address, detail ) "
        + "VALUES ( ?, ?, ?, ?, ? );";
    return sqlCommand;
  }
  


  protected Statement setStatementParameters(PreparedStatement updateCommand, TestData data) throws DriverException
  {
    final BoundStatement boundStmnt = new BoundStatement(updateCommand);
    //or use bind
    //boundStmnt.bind(arg0)
    boundStmnt.setLong(0, ++id);
    boundStmnt.setString(1, data.name);
    boundStmnt.setInt(2, data.age);
    boundStmnt.setString(3, data.address);
    boundStmnt.setString(4, data.desc);
    
    //or boundStatement.bind();
    return boundStmnt;
  }
  
  protected void createSession()
  {
    Cluster cluster = Cluster.builder().addContactPoint(cassandraConfig.getHost()).build();
    session = cluster.connect(cassandraConfig.getDatabase());
  }
  
  protected void createTables()
  {
    createBusinessTables(session);
  }
  
  
  //@Override
  protected PreparedStatement prepareStatement()
  {
    return session.prepare(sqlCommand);
  }


  public DataWarehouseConfig getCassandraConfig()
  {
    return cassandraConfig;
  }

  public void setCassandraConfig(DataWarehouseConfig cassandraConfig)
  {
    this.cassandraConfig = cassandraConfig;
  }
}
