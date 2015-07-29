package cg.dt.malharlib;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.annotation.InputPortFieldAnnotation;
import com.datatorrent.common.util.BaseOperator;

public class TupleCacheOutputOperator<T>  extends BaseOperator
{
  private static final long serialVersionUID = 3090932382383138500L;
  private static final Logger logger = LoggerFactory.getLogger( TupleCacheOutputOperator.class );
  
  //one instance of TupleCacheOutputOperator map to one 
  private static Map< String, List<?> > receivedTuplesMap = new ConcurrentHashMap< String, List<?>>();
  
  private transient List<T> receivedTuples = null;
  
  //the StreamMeta.persistent() will throw NullPointerException if don't put annotation
  @InputPortFieldAnnotation(optional = true)
  public final transient DefaultInputPort<T> inputPort = new DefaultInputPort<T>() {

    @Override
    public void process(T tuple)
    {
      processTuple( tuple );
    }
  };
  
  private String uuid;
  
  public TupleCacheOutputOperator()
  {
    uuid = java.util.UUID.randomUUID().toString();
  }
  
  
  public void setup(OperatorContext context)
  {
    prepareReceivedTupleList();
    logger.debug( "setup() done." );
  }
  
  public String getUuid()
  {
    return uuid;
  }

  public synchronized void processTuple( T tuple )
  {
    receivedTuples.add(tuple);
    
    if( receivedTuples.size()%1000 == 0 )
      logger.debug( "( {}, {} ): {}.", getName(), System.identityHashCode(this), receivedTuples.size() );
  }

  public List<T> prepareReceivedTupleList()
  {
    if( receivedTuples == null )
    {
      receivedTuples = (List<T>)receivedTuplesMap.get(uuid);
    }
    if( receivedTuples == null )
    {
      receivedTuples = new ArrayList<T>();
      receivedTuplesMap.put(uuid, receivedTuples);
    }
    return receivedTuples;
  }
  
  public List<T> getReceivedTuples()
  {
    if( receivedTuples == null )
    {
      receivedTuples = (List<T>)receivedTuplesMap.get(uuid);
    }
    return receivedTuples;
  }
  
  public static List<Object> getReceivedTuples( String uuid )
  {
    return (List<Object>)receivedTuplesMap.get(uuid);
  }
}