package cg.dt.malharlib;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.StreamCodec;
import com.datatorrent.api.annotation.InputPortFieldAnnotation;
import com.datatorrent.common.util.BaseOperator;
import com.datatorrent.lib.codec.KryoSerializableStreamCodec;
import com.datatorrent.netlet.util.Slice;

public class TupleWriteOperator<T> extends BaseOperator {
  private static final Logger logger = LoggerFactory.getLogger(TupleWriteOperator.class);
  
  public final int COUNT = 100000;
  public final int BLOCK_SIZE = 100;
  private StreamCodec<T> codec = new KryoSerializableStreamCodec<T>();
  
  private String filePath = "/tmp/TupleWriteOperator.out";

  private Writer writer;
  
  @InputPortFieldAnnotation(optional = true)
  public final transient DefaultInputPort<T> input = new DefaultInputPort<T>()
  {
    @Override
    public void process(T t)
    {
      processTuple(t);
    }
  };

  public void processTuple( T tuple )
  {
    Slice slice = codec.toByteArray(tuple);
    writer.write(slice.buffer, slice.offset, slice.length);
  }
  
  
  public String getFilePath() {
    return filePath;
  }


  public void setFilePath(String filePath) {
    this.filePath = filePath;
  }


  @Override
  public void setup(OperatorContext context)
  {
//    FsSaver fsSaver = new FsSaver(filePath);
//    writer = new BlockWriter(BLOCK_SIZE, fsSaver);
    writer = new SimpleFileWriter(filePath);
  }
  
  @Override
  public void teardown()
  {
  }
  
  @Override
  public void endWindow()
  {
    writer.flush();
  }
}
