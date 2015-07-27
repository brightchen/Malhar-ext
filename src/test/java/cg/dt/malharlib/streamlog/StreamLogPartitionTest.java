package cg.dt.malharlib.streamlog;

import org.apache.hadoop.conf.Configuration;
import org.junit.Test;

import com.datatorrent.api.DAG;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.LocalMode;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.contrib.util.POJOTupleGenerateOperator;

import cg.dt.malharlib.TupleWriteOperator;
import cg.dt.malharlib.util.TestTuple;
import cg.dt.malharlib.util.TupleGenerator;

import com.datatorrent.api.DAG.Locality;
import com.datatorrent.api.DAG.StreamMeta;

public class StreamLogPartitionTest {
  @Test
  public void test() throws Exception
  {
    // Create DAG for testing.
    LocalMode lma = LocalMode.newInstance();

    StreamingApplication app = new StreamingApplication() {
      @Override
      public void populateDAG(DAG dag, Configuration conf)
      {
      }
    };

    DAG dag = lma.getDAG();

    POJOTupleGenerateOperator<TestTuple> generator = new POJOTupleGenerateOperator<TestTuple>();
    generator.setTupleType(TestTuple.class);
    dag.addOperator("generator", generator);
    
    TupleWriteOperator<TestTuple> writeOperator = new TupleWriteOperator<TestTuple>();
    writeOperator.setFilePath("/tmp/sl/TupleWriter.out");
    dag.addOperator("writer", writeOperator);
    
    // Connect ports
    StreamMeta sm = dag.addStream("stream1", generator.outputPort, writeOperator.input).setLocality(Locality.CONTAINER_LOCAL);
    
    //for log
    TupleWriteOperator<TestTuple> logOperator = new TupleWriteOperator<TestTuple>();
    logOperator.setFilePath("/tmp/sl/StreamLog.out");
    sm.persistStream(logOperator);

    Configuration conf = new Configuration(false);
    lma.prepareDAG(app, conf);

    // Create local cluster
    final LocalMode.Controller lc = lma.getController();
    lc.runAsync();
    
    Thread.sleep(60000);
    lc.shutdown();
  }
  
}
