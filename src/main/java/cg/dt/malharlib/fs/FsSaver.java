package cg.dt.malharlib.fs;

import java.io.IOException;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FsSaver implements Runnable
{
  private static final Logger LOG = LoggerFactory.getLogger(FsSaver.class);
  
  final private Lock lock = new ReentrantLock();
  final private Condition writing  = lock.newCondition(); 
  
  final private Configuration conf = new Configuration();
  
  private Path filePath;
  private FSDataOutputStream fsOutStream;
  private byte[] data;
  private int start;
  private int length;
  
  public FsSaver( String fileName )
  {
    filePath = new Path(fileName);
    try {
      FileSystem fs = FileSystem.get(conf);
      fsOutStream = fs.create(filePath);
    } catch (IOException e) {
      e.printStackTrace();
    }
  }
  

  public void run() {
    try {
      lock.lock();
      writing.await();
      lock.unlock();
      LOG.debug("writing data: size: ", data.length);
      fsOutStream.write(data, start, length);
    } catch (InterruptedException e) {
      e.printStackTrace();
      lock.unlock();
    } catch (IOException e) {
      e.printStackTrace();
    }
    
  }
  
  public void setData( byte[] data, int start, int length )
  {
    lock.lock();
    this.data = data;
    this.start = start;
    this.length = length;
    writing.signal();
    lock.unlock();
  }
}
