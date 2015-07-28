package cg.dt.malharlib;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.annotation.InputPortFieldAnnotation;
import com.datatorrent.api.annotation.OutputPortFieldAnnotation;
import com.datatorrent.common.util.BaseOperator;

public class PassthroughOperator<T> extends BaseOperator {
  private static final Logger logger = LoggerFactory.getLogger(PassthroughOperator.class);
  
  @InputPortFieldAnnotation(optional = true)
  public final transient DefaultInputPort<T> input = new DefaultInputPort<T>()
  {
    @Override
    public void process(T t)
    {
      processTuple(t);
    }
  };
  
  @OutputPortFieldAnnotation(optional = true)
  public final transient DefaultOutputPort<Object> outport = new DefaultOutputPort<Object>();
  
  public void processTuple( T tuple )
  {
    outport.emit(tuple);
  }
}
