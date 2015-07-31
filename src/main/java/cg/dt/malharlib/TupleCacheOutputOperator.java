package cg.dt.malharlib;

import java.io.Serializable;
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


public class TupleCacheOutputOperator<T>  extends BaseOperator implements Serializable
{
  private static final long serialVersionUID = 3090932382383138500L;
  private static final Logger logger = LoggerFactory.getLogger( TupleCacheOutputOperator.class );
  
  //one instance of TupleCacheOutputOperator map to one 
  private static transient Map< Integer, List<?> > receivedTuplesMap = new ConcurrentHashMap< Integer, List<?>>();
  
  private transient List<T> receivedTuples = null;
  
  //the StreamMeta.persistent() will throw NullPointerException if don't put annotation
  @InputPortFieldAnnotation(optional = true)
  public final transient TupleCacheInputPort<T> inputPort = new TupleCacheInputPort<T>();
//  public final transient DefaultInputPort<T> inputPort = new DefaultInputPort<T>() {
//
//    @Override
//    public void process(T tuple)
//    {
//      processTuple( tuple );
//    }
//  };
  
  private transient int identity;
  
  public TupleCacheOutputOperator()
  {
    identity = System.identityHashCode(this);
  }
  
  
  public void setup(OperatorContext context)
  {
    inputPort.setOperator(this);
    prepareReceivedTupleList();
    logger.debug( "setup() done." );
  }
  

  public int getIdentity() {
    return identity;
  }


  public  void processTuple( T tuple )
  {
    synchronized(TupleCacheOutputOperator.class)
    {
      receivedTuples.add(tuple);
      
      if( receivedTuples.size()%1000 == 0 )
        logger.debug( "( {}, {}, {} ): {}.", getName(), identity, System.identityHashCode(this), receivedTuples.size() );
    }
  }

  public List<T> prepareReceivedTupleList()
  {
    if( receivedTuples == null )
    {
      receivedTuples = (List<T>)receivedTuplesMap.get(identity);
    }
    if( receivedTuples == null )
    {
      receivedTuples = new ArrayList<T>();
      receivedTuplesMap.put(identity, receivedTuples);
    }
    return receivedTuples;
  }
  
  public List<T> getReceivedTuples()
  {
    if( receivedTuples == null )
    {
      receivedTuples = (List<T>)receivedTuplesMap.get(identity);
    }
    return receivedTuples;
  }
  
  public static List<Object> getReceivedTuples( String uuid )
  {
    return (List<Object>)receivedTuplesMap.get(uuid);
  }
  public static Map< Integer, List<?> > getReceivedTuplesMap()
  {
    return receivedTuplesMap;
  }
}