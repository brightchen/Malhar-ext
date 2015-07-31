package cg.dt.malharlib.streamlog;

import org.apache.hadoop.conf.Configuration;

import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.Context.PortContext;
import com.datatorrent.api.DAG;
import com.datatorrent.api.DAG.Locality;
import com.datatorrent.api.DAG.StreamMeta;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.StreamCodec;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.api.annotation.ApplicationAnnotation;
import com.datatorrent.common.partitioner.StatelessPartitioner;
import com.datatorrent.common.util.BaseOperator;
import com.datatorrent.lib.codec.KryoSerializableStreamCodec;
import com.datatorrent.lib.math.Sum;

import cg.dt.malharlib.TupleCacheOutputOperator;
import cg.dt.malharlib.TupleWriteOperator;
import cg.dt.malharlib.util.POJOTupleGenerateOperator;
import cg.dt.malharlib.util.SimpleTuple;

@ApplicationAnnotation(name="StreamLogTestApp")
@SuppressWarnings({"rawtypes", "unchecked"})
public class StreamLogApp implements StreamingApplication {
  
  
  /********************************
  public static class LogTestCodec extends KryoSerializableStreamCodec<TestTuple>
  {
    private static final long serialVersionUID = -2061989344770955107L;

    @Override
    public int getPartition(TestTuple tuple)
    {
      return (int)(long)tuple.getRowId();
    }
  };
  
  
  public static class LogTestCodec2 implements StreamCodec<TestTuple>, Serializable
  {
    private static final long serialVersionUID = -7025318072432275282L;

    @Override
    public int getPartition(TestTuple tuple)
    {
      return (int)(long)tuple.getRowId();
    }

    @Override
    public Object fromByteArray(Slice fragment) {
      String row = new String( fragment.buffer, fragment.offset, fragment.length );
      return new TestTuple( row );
    }

    @Override
    public Slice toByteArray(TestTuple tuple) {
      byte[] bytes = tuple.getRow().getBytes();
      return new Slice(bytes, 0, bytes.length);
    }
  };
  /********************************/
  
  
  protected final StreamCodec codec = new KryoSerializableStreamCodec();
  protected final Class tupleClass = SimpleTuple.class; //TestTuple.class;
  protected final String logFilePath = "/tmp/sl/StreamLog.out";
  protected final String writeFilePath = "/tmp/sl/TupleWriter.out";
  
  protected boolean useWriteOperator = false;
  protected TupleWriteOperator writeOperator = null;
  protected TupleCacheOutputOperator outputOperator = null;
  
  @Override
  public void populateDAG(DAG dag, Configuration conf)
  {
    POJOTupleGenerateOperator generator = new POJOTupleGenerateOperator();
    generator.setBlockTime(1);
    generator.setTupleNum(10000);
    generator.setTupleType(tupleClass);
    dag.addOperator("generator", generator);
    
    StreamMeta sm = null;
    
    if(useWriteOperator)
    {
      writeOperator = new TupleWriteOperator();
      writeOperator.setFilePath(writeFilePath);
      writeOperator.setName("Writer");
      sm = addWorkOperator( dag, generator.outputPort, writeOperator, writeOperator.input );
    }
    else
    {
      outputOperator = new TupleCacheOutputOperator();
      outputOperator.setName("Output");
      sm = addWorkOperator( dag, generator.outputPort, outputOperator, outputOperator.inputPort );
    }
    
    //for log
    if(useWriteOperator)
    {
      TupleWriteOperator logOperator = new TupleWriteOperator();
      logOperator.setName("log");
      logOperator.setFilePath(logFilePath);
      sm.persist(logOperator);
    }
    else
    {
      TupleCacheOutputOperator logOperator = new TupleCacheOutputOperator();
      logOperator.setName("log");
      sm.persist(logOperator);
    }
  }
  
  protected StreamMeta addWorkOperator( DAG dag, DefaultOutputPort upstreamOutputPort, BaseOperator operator, DefaultInputPort operatorInputPort  )
  {
    dag.addOperator("writer", operator);
    
    //partition
    dag.getMeta(operator).getAttributes().put(OperatorContext.PARTITIONER, new StatelessPartitioner<Sum<Integer>>(2));
    
    dag.setInputPortAttribute(operatorInputPort, PortContext.STREAM_CODEC, codec);
    
    // Connect ports
    StreamMeta sm = dag.addStream("stream1", upstreamOutputPort, operatorInputPort).setLocality(Locality.CONTAINER_LOCAL);
    
    return sm;
  }
  
}
