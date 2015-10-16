package cg.dt.malharlib.app;

import java.util.Iterator;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.conf.Configuration;

import com.datatorrent.api.DAG;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.api.annotation.ApplicationAnnotation;
import com.datatorrent.lib.io.ConsoleOutputOperator;
import com.datatorrent.lib.testbench.RandomEventGenerator;

import cg.dt.malharlib.conf.DefaultConf;
import cg.dt.malharlib.op.DumpInfoOperator;

@ApplicationAnnotation(name = "Test1")
public class Test1 implements StreamingApplication
{
  private static final transient Logger logger = LoggerFactory.getLogger(Test1.class);

  @Override
  public void populateDAG(DAG dag, Configuration conf)
  {
    String host = conf.get("Test1.host");
    DefaultConf.instance.setHost(host);
    
    Iterator<Map.Entry<String, String>> iter = conf.iterator();
    StringBuilder confInfo = new StringBuilder();
    while(iter.hasNext())
    {
      Map.Entry<String, String> nameValue = iter.next();
      confInfo.append(String.format("%s=%s", nameValue.getKey(), nameValue.getValue())).append("; ");
    }
    
    logger.error(confInfo.toString());

    
    RandomEventGenerator rand = dag.addOperator("rand", RandomEventGenerator.class);
    rand.setMaxvalue(100);
    rand.setTuplesBlast(2);
    rand.setTuplesBlastIntervalMillis(100);
    

    DumpInfoOperator dumpOperator = new DumpInfoOperator();
    dumpOperator.setOtherInfo(host);    //confInfo.toString());
    dag.addOperator("dump", dumpOperator);
    
    dag.addStream("stream2", rand.integer_data, dumpOperator.input);
  }

}
