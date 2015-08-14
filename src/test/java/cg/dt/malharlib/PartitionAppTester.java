package cg.dt.malharlib;

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

import cg.dt.malharlib.streamlog.PartitionApp;
import cg.dt.malharlib.streamlog.StreamLogPartitionTest;

public class PartitionAppTester  extends PartitionApp {
  
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
          Thread.sleep(5000);
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
        {
          for( Map.Entry< Integer, List<?> > entry : tuplesMap.entrySet() )
          {
            logger.debug("{}:", entry.getKey());
            logger.debug(toString(entry.getValue(), 100));
              
          }
          break;
        }
        else
        {
          lastReceivedSizes = receivedSize;
          receivedSize.clear();
        }
      }
    }
    else
      Thread.sleep(30000);
    
    lc.shutdown();
  }
  
  public static String toString( List<?> list, int size )
  {
    StringBuilder sb = new StringBuilder();
    for( int i=0; i<Math.min(list.size(),size); ++i )
      sb.append( list.get(i).toString() ).append(", ");
    return sb.toString();
  }
}