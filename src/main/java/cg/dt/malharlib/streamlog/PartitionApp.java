package cg.dt.malharlib.streamlog;

import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;

import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.Context.PortContext;
import com.datatorrent.api.DAG;
import com.datatorrent.api.DAG.Locality;
import com.datatorrent.api.DAG.StreamMeta;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.Operator.InputPort;
import com.datatorrent.api.Partitioner.PartitionKeys;
import com.datatorrent.api.annotation.ApplicationAnnotation;
import com.datatorrent.api.StreamCodec;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.common.util.BaseOperator;
import com.datatorrent.lib.codec.KryoSerializableStreamCodec;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import cg.dt.malharlib.PartitionableTupleCacheOutputOperator;
import cg.dt.malharlib.TupleCacheOutputOperator;
import cg.dt.malharlib.TupleWriteOperator;
import cg.dt.malharlib.util.POJOTupleGenerateOperator;
import cg.dt.malharlib.util.SimpleTuple;


@ApplicationAnnotation(name="PartitionApp")
@SuppressWarnings({"rawtypes", "unchecked"})
public class PartitionApp implements StreamingApplication {

  public static class LogTestCodec extends KryoSerializableStreamCodec
  {
    private static final long serialVersionUID = -2061989344770955107L;
    private int index=0;
    @Override
    public int getPartition(Object tuple)
    {
      return tuple.hashCode();
      //return index++;
    }
  };
  
  
  protected final StreamCodec codec = new LogTestCodec();
  
  
  public static final int TUPLE_SIZE = 1000;
  protected final Class tupleClass = SimpleTuple.class; //TestTuple.class;
  protected final String logFilePath = "/tmp/sl/StreamLog.out";
  protected final String writeFilePath = "/tmp/sl/TupleWriter.out";
  
  protected boolean applyPartition = false;
  
  protected boolean useWriteOperator = false;
  protected TupleWriteOperator writeOperator = null;
  protected TupleCacheOutputOperator outputOperator = null;
  
  
  @Override
  public void populateDAG(DAG dag, Configuration conf)
  {
    POJOTupleGenerateOperator generator = new POJOTupleGenerateOperator();
    generator.setBlockTime(2);
    generator.setTupleNum(TUPLE_SIZE);
    generator.setTupleType(tupleClass);
    dag.addOperator("generator", generator);
    
    StreamMeta sm = null;
    
    if(useWriteOperator)
    {
      writeOperator = new TupleWriteOperator();
      writeOperator.setFilePath(writeFilePath);
      writeOperator.setName("Writer");
      sm = addWorkOperator( dag, generator.outputPort, writeOperator, writeOperator.input, applyPartition );
    }
    else
    {
      outputOperator = new PartitionableTupleCacheOutputOperator();   //new TupleCacheOutputOperator();
      outputOperator.setName("Output");
      sm = addWorkOperator( dag, generator.outputPort, outputOperator, outputOperator.inputPort, applyPartition );
    }
    
  }
  
  protected StreamMeta addWorkOperator( DAG dag, DefaultOutputPort upstreamOutputPort, BaseOperator operator, DefaultInputPort operatorInputPort, boolean applyPartition  )
  {
    dag.addOperator("writer", operator);

    dag.setInputPortAttribute(operatorInputPort, PortContext.STREAM_CODEC, codec);    
    // Connect ports
    StreamMeta sm = dag.addStream("stream1", upstreamOutputPort, operatorInputPort).setLocality(Locality.CONTAINER_LOCAL);

    if( applyPartition )
    {
      List<Map<InputPort<?>, PartitionKeys>> partionKeysList = Lists.newArrayList();
      {
        Map<InputPort<?>, PartitionKeys> partionKeys = Maps.newHashMap();
        partionKeys.put(operatorInputPort, new PartitionKeys(0x03, Sets.newHashSet(3)));
        partionKeysList.add(partionKeys);
      }
  //    {
  //      Map<InputPort<?>, PartitionKeys> partionKeys = Maps.newHashMap();
  //      partionKeys.put(operatorInputPort, new PartitionKeys(0x03, Sets.newHashSet(1)));
  //      partionKeysList.add(partionKeys);
  //    }
      
      //partition
      dag.getMeta(operator).getAttributes().put(OperatorContext.PARTITIONER, new Partitioner1( partionKeysList ) ); //new StatelessPartitioner<Sum<Integer>>(2));
    }
//    else
//      dag.getMeta(operator).getAttributes().put(OperatorContext.PARTITIONER, (Partitioner)operator );

    return sm;
  }
}