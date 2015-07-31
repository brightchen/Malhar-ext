package cg.dt.malharlib.streamlog;

import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.api.DAG;
import com.datatorrent.api.LocalMode;
import com.datatorrent.api.StreamingApplication;
import com.google.common.collect.Maps;

import cg.dt.malharlib.TupleCacheOutputOperator;


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
      Map<Integer, Integer> lastReceivedSizes = Maps.newHashMap();
      Map<Integer, Integer> receivedSize = Maps.newHashMap();
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
        
        Map< Integer, List<?> > tuplesMap = TupleCacheOutputOperator.getReceivedTuplesMap();
        for( Map.Entry< Integer, List<?> > entry : tuplesMap.entrySet() )
        {
          receivedSize.put(entry.getKey(), entry.getValue().size());
          logger.info( "total received size: {}\t{}", entry.getKey(), entry.getValue().size() );
        }
        if(receivedSize.equals(lastReceivedSizes))
          break;
        else
        {
          lastReceivedSizes = receivedSize;
          receivedSize.clear();
        }
      }
    }
    else
      Thread.sleep(60000);
    
    lc.shutdown();
  }
  
}
