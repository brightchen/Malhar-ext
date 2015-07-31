package cg.dt.malharlib;

import java.io.Serializable;
import java.util.UUID;

import com.datatorrent.api.DefaultInputPort;

@SuppressWarnings("rawtypes")
public class TupleCacheInputPort<T> extends DefaultInputPort<T> implements Serializable
{
  private static final long serialVersionUID = 1361047435502080638L;
  private TupleCacheOutputOperator<T> operator;
  
  private String id;
  
  public TupleCacheInputPort()
  {
    id = UUID.randomUUID().toString();
  }

  public TupleCacheInputPort( TupleCacheOutputOperator operator )
  {
    this();
    setOperator(operator);
  }
  
  public TupleCacheOutputOperator getOperator() {
    return operator;
  }

  public void setOperator(TupleCacheOutputOperator operator) {
    this.operator = operator;
  }

  @Override
  public void process(T tuple)
  {
    operator.processTuple( tuple );
  }
  
  @Override
  public int hashCode()
  {
    return id.hashCode();
  }
  
  @Override
  public boolean equals( Object obj )
  {
    if( obj == null || !( obj instanceof TupleCacheInputPort ) )
      return false;
    return id.equals( ((TupleCacheInputPort)obj).id );
  }
}
