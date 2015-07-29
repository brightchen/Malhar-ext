package cg.dt.malharlib.util;

import java.util.concurrent.atomic.AtomicInteger;

import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.InputOperator;
import com.datatorrent.api.Operator.ActivationListener;

/**
 * @param <T> the type of tuple
 */
public class POJOTupleGenerateOperator<T> implements InputOperator, ActivationListener<OperatorContext>
{
  protected final int DEFAULT_TUPLE_NUM = 10000;
  public final transient DefaultOutputPort<T> outputPort = new DefaultOutputPort<T>();
  
  private int blockTime = 100;
  private int tupleNum = DEFAULT_TUPLE_NUM;
  private int batchNum = 5;
  private TupleGenerator<T> tupleGenerator = null;
  private Class<T> tupleClass;
  private AtomicInteger emitedTuples = new AtomicInteger(0);

  public POJOTupleGenerateOperator()
  {
  }
  
  public POJOTupleGenerateOperator( Class<T> tupleClass )
  {
    this.tupleClass = tupleClass;
  }
  
  public void setTupleType( Class<T> tupleClass )
  {
    this.tupleClass = tupleClass;
  }
  
  @Override
  public void beginWindow(long windowId)
  {
  }

  @Override
  public void endWindow()
  {
  }

  @Override
  public void setup(OperatorContext context)
  {
  }

  @Override
  public void teardown()
  {
  }

  @Override
  public void activate(OperatorContext ctx)
  {
  }

  @Override
  public void deactivate()
  {
  }

  @Override
  public void emitTuples()
  {
    final int theTupleNum = getTupleNum();
    if( emitedTuples.get() >= theTupleNum )
    {
      try
      {
        Thread.sleep(10);
      }
      catch( Exception e ){}
      return;
    }
      
    
    for( int i=0; i<batchNum; ++i )
    {
      int count = emitedTuples.get();
      if( count >= theTupleNum )
        return;
      
      if( emitedTuples.compareAndSet(count, count+1) )
      {
        T tuple = getNextTuple();        
        outputPort.emit ( tuple );
        tupleEmitted( tuple );
        
        if( count+1 == theTupleNum )
        {
          tupleEmitDone();
          return;
        }
        if(blockTime >= 0)
        {
          try
          {
            Thread.sleep(blockTime);
          }
          catch(Exception e)
          {
            
          }
        }
      }
      
    }
  }
  
  
  protected void tupleEmitted( T tuple ){}
  protected void tupleEmitDone(){}
  
  public int getEmitedTupleCount()
  {
    return emitedTuples.get();
  }
  
  public int getTupleNum()
  {
    return tupleNum;
  }
  public void setTupleNum( int tupleNum )
  {
    this.tupleNum = tupleNum;
  }
  
  protected T getNextTuple()
  {
    if( tupleGenerator == null )
      tupleGenerator = createTupleGenerator();

    return tupleGenerator.getNextTuple();
  }

  protected TupleGenerator<T> createTupleGenerator()
  {
    return new TupleGenerator( tupleClass );
  }

  public int getBlockTime() {
    return blockTime;
  }

  public void setBlockTime(int blockTime) {
    this.blockTime = blockTime;
  }

  public int getBatchNum() {
    return batchNum;
  }

  public void setBatchNum(int batchNum) {
    this.batchNum = batchNum;
  }
  
}