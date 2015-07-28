package cg.dt.malharlib;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.api.StreamCodec;
import com.datatorrent.lib.codec.KryoSerializableStreamCodec;
import com.datatorrent.netlet.util.Slice;

import cg.dt.malharlib.util.TestTuple;
import cg.dt.malharlib.util.TupleGenerator;

public class SimpleFileWriterTest {
  private static final Logger logger = LoggerFactory.getLogger(SimpleFileWriterTest.class);
  
  public final int COUNT = 100000;
  public final int BLOCK_SIZE = 10000;

  private TupleGenerator<TestTuple> generator = new TupleGenerator<TestTuple>();
  private StreamCodec<TestTuple> codec = new KryoSerializableStreamCodec<TestTuple>();

  private String filePath = "/tmp/test";


  @Test
  public void test() {

  }

  public Slice getNext() {
    TestTuple tuple = generator.getNextTuple();
    return codec.toByteArray(tuple);
  }
}
