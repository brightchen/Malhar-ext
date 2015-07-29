package cg.dt.malharlib.streamlog;

import java.io.Serializable;

import org.apache.hadoop.conf.Configuration;

import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.Context.PortContext;
import com.datatorrent.api.DAG;
import com.datatorrent.api.DAG.Locality;
import com.datatorrent.api.DAG.StreamMeta;
import com.datatorrent.api.StreamCodec;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.api.annotation.ApplicationAnnotation;
import com.datatorrent.common.partitioner.StatelessPartitioner;
import com.datatorrent.lib.codec.KryoSerializableStreamCodec;
import com.datatorrent.lib.math.Sum;
import com.datatorrent.netlet.util.Slice;

import cg.dt.malharlib.TupleWriteOperator;
import cg.dt.malharlib.util.POJOTupleGenerateOperator;
import cg.dt.malharlib.util.TestTuple;

@ApplicationAnnotation(name="StreamLogTestApp")
public class StreamLogApp implements StreamingApplication {
  public static class LogTestCodec1 extends KryoSerializableStreamCodec<TestTuple>
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
  
  
  private final StreamCodec<TestTuple> codec = new LogTestCodec1();
  private final String logFilePath = "/tmp/sl/StreamLog.out";
  private final String writeFilePath = "/tmp/sl/TupleWriter.out";
  
  @Override
  public void populateDAG(DAG dag, Configuration conf)
  {

    POJOTupleGenerateOperator<TestTuple> generator = new POJOTupleGenerateOperator<TestTuple>();
    generator.setBlockTime(10);
    generator.setTupleType(TestTuple.class);
    dag.addOperator("generator", generator);
    
    TupleWriteOperator<TestTuple> writeOperator = new TupleWriteOperator<TestTuple>();
    writeOperator.setFilePath(writeFilePath);
    dag.addOperator("writer", writeOperator);
    
    //partition
    dag.getMeta(writeOperator).getAttributes().put(OperatorContext.PARTITIONER, new StatelessPartitioner<Sum<Integer>>(2));
    
    dag.setInputPortAttribute(writeOperator.input, PortContext.STREAM_CODEC, codec);
    
    // Connect ports
    StreamMeta sm = dag.addStream("stream1", generator.outputPort, writeOperator.input).setLocality(Locality.CONTAINER_LOCAL);
    
    //for log
    TupleWriteOperator<TestTuple> logOperator = new TupleWriteOperator<TestTuple>();
    logOperator.setFilePath(logFilePath);
    sm.persist(logOperator);
  }
  
}
