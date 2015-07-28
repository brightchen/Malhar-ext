package cg.dt.malharlib.streamlog;

import org.apache.hadoop.conf.Configuration;

import com.datatorrent.api.Context.PortContext;
import com.datatorrent.api.DAG;
import com.datatorrent.api.DAG.Locality;
import com.datatorrent.api.DAG.StreamMeta;
import com.datatorrent.api.StreamCodec;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.api.annotation.ApplicationAnnotation;
import com.datatorrent.lib.codec.KryoSerializableStreamCodec;

import cg.dt.malharlib.TupleWriteOperator;
import cg.dt.malharlib.util.POJOTupleGenerateOperator;
import cg.dt.malharlib.util.TestTuple;

@ApplicationAnnotation(name="StreamLogTestApp")
public class StreamLogApp implements StreamingApplication {
  public static class LogTestCodec extends KryoSerializableStreamCodec<TestTuple>
  {
    private static final long serialVersionUID = -2061989344770955107L;

    @Override
    public int getPartition(TestTuple tuple)
    {
      return (int)(tuple.getRowId()%10);
    }
  };
  
  @Override
  public void populateDAG(DAG dag, Configuration conf)
  {

    POJOTupleGenerateOperator<TestTuple> generator = new POJOTupleGenerateOperator<TestTuple>();
    generator.setTupleType(TestTuple.class);
    dag.addOperator("generator", generator);
    
    TupleWriteOperator<TestTuple> writeOperator = new TupleWriteOperator<TestTuple>();
    writeOperator.setFilePath("/tmp/sl/TupleWriter.out");
    dag.addOperator("writer", writeOperator);
    
    //partition
    StreamCodec<TestTuple> codec = new LogTestCodec();
    dag.setInputPortAttribute(writeOperator.input, PortContext.STREAM_CODEC, codec);
    
    // Connect ports
    StreamMeta sm = dag.addStream("stream1", generator.outputPort, writeOperator.input).setLocality(Locality.CONTAINER_LOCAL);
    
    //for log
    TupleWriteOperator<TestTuple> logOperator = new TupleWriteOperator<TestTuple>();
    logOperator.setFilePath("/tmp/sl/StreamLog.out");
    sm.persistStream(logOperator);
  }
  
}
