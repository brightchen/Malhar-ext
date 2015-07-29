package cg.dt.malharlib.streamlog;

import org.apache.hadoop.conf.Configuration;

import com.datatorrent.api.DAG;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.annotation.ApplicationAnnotation;
import com.datatorrent.common.partitioner.StatelessPartitioner;
import com.datatorrent.lib.io.ConsoleOutputOperator;
import com.datatorrent.lib.math.Sum;
import com.datatorrent.lib.testbench.RandomEventGenerator;

@ApplicationAnnotation(name="SampleApp")
public class SampleApp implements StreamingApplication {
  @Override
  public void populateDAG(DAG dag, Configuration conf)
  {
    // Create application dag.
    dag.setAttribute(DAG.APPLICATION_NAME, "PartitionMathSumApplication");
    dag.setAttribute(DAG.DEBUG, true);

    // Add random integer generator operator
    RandomEventGenerator rand = dag.addOperator("rand",
        RandomEventGenerator.class);
    rand.setMaxvalue(100);
    rand.setTuplesBlast(2);
    rand.setTuplesBlastIntervalMillis(100);

    Sum<Integer> sum = dag.addOperator("sum", Sum.class);
    dag.addStream("stream1", rand.integer_data, sum.data);
    dag.getMeta(sum).getAttributes().put(OperatorContext.PARTITIONER, new StatelessPartitioner<Sum<Integer>>(2));
    dag.getMeta(sum).getAttributes()
        .put(OperatorContext.APPLICATION_WINDOW_COUNT, 20);

    // Connect to output console operator
    ConsoleOutputOperator console = dag.addOperator("console",
        new ConsoleOutputOperator());
    dag.addStream("stream2", sum.sum, console.input);

    // done
  }
}
