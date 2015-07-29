package cg.dt.malharlib.streamlog;

import org.apache.hadoop.conf.Configuration;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.api.DAG;
import com.datatorrent.api.LocalMode;
import com.datatorrent.api.StreamingApplication;


public class StreamLogPartitionTest extends StreamLogApp {
  
  private static final Logger logger = LoggerFactory.getLogger( StreamLogPartitionTest.class );
      
  @Test
  public void test() throws Exception
  {
    LocalMode lma = LocalMode.newInstance();
    DAG dag = lma.getDAG();
    Configuration conf = new Configuration(false);
    
    super.populateDAG(dag, conf);
    
    StreamingApplication app = new StreamingApplication() {
      @Override
      public void populateDAG(DAG dag, Configuration conf)
      {
      }
    };
    
    lma.prepareDAG(app, conf);
    
 // Create local cluster
    final LocalMode.Controller lc = lma.getController();
    lc.runAsync();
    
    
    if(outputOperator != null)
    {
      int lastReceivedSize = 0;
      int receivedSize = 0;
      while(true)
      {
        try
        {
          Thread.sleep(10000);
        }
        catch(Exception e)
        {
          e.printStackTrace();
        }
        receivedSize = outputOperator.getReceivedTuples() == null ? 0 : outputOperator.getReceivedTuples().size();
        
        logger.info( "total received size: {}", receivedSize );
        if(lastReceivedSize == receivedSize && receivedSize != 0 )
          break;
        lastReceivedSize = receivedSize;
      }
    }
    else
      Thread.sleep(60000);
    
    lc.shutdown();
  }
  
}
