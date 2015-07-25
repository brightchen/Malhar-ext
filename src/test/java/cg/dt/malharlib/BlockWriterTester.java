package cg.dt.malharlib;

import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.api.StreamCodec;
import com.datatorrent.lib.codec.KryoSerializableStreamCodec;
import com.datatorrent.netlet.util.Slice;

import cg.dt.malharlib.fs.FsSaver;
import cg.dt.malharlib.util.TestTuple;
import cg.dt.malharlib.util.TupleGenerator;


public class BlockWriterTester {
  public final int COUNT = 1000000;
  public final int BLOCK_SIZE = 10000;
  private static final Logger logger = LoggerFactory.getLogger(BlockWriterTester.class);
  private TupleGenerator<TestTuple> generator = new TupleGenerator<TestTuple>();
  private StreamCodec<TestTuple> codec = new KryoSerializableStreamCodec<TestTuple>();
  
  private String filePath = "/tmp/test";
  
  public static void main( String[] argvs )
  {
    BlockWriterTester tester = new BlockWriterTester();
    tester.setup();
    tester.test();
  }
  
  @Before
  public void setup()
  {
    generator.useTupleClass(TestTuple.class);
  }
  
  @Test
  public void test()
  {
    FsSaver fsSaver = new FsSaver(filePath);
    BlockWriter writer = new BlockWriter(BLOCK_SIZE, fsSaver);
    
    for(int count=0; count<COUNT; ++count)
    {
      Slice slice = getNext();
      writer.write(slice.buffer, slice.offset, slice.length);
    }
    writer.flush();
  }
  
  public Slice getNext()
  {
    TestTuple tuple = generator.getNextTuple();
    return codec.toByteArray(tuple);
  }
}
