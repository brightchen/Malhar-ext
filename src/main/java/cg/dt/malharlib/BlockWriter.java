package cg.dt.malharlib;

import java.io.ByteArrayOutputStream;

import cg.dt.malharlib.fs.FsSaver;

/**
 * write data
 * @author bright
 *
 */
public class BlockWriter implements Writer{
  public static final int BLOCK_SIZE = 64*1024*1024;  //64M
  
  private int blockSize = BLOCK_SIZE;
  private byte[] buf1;
  private byte[] buf2;
  private byte[] currentBuf;
  private int currentBufOffset;

  private FsSaver fsSaver;
  private Thread saverThread;
  
  public BlockWriter()
  {
    this(BLOCK_SIZE);
  }
  
  public BlockWriter(int blockSize)
  {
    this.blockSize = blockSize;
    buf1 = new byte[blockSize];
    buf2 = new byte[blockSize];
    
    currentBuf = buf1;
    currentBufOffset = 0;
  }
  
  public BlockWriter(FsSaver fsSaver)
  {
    this();
    setFsSaver(fsSaver);
  }
  
  public BlockWriter(int blockSize, FsSaver fsSaver)
  {
    this(blockSize);
    setFsSaver(fsSaver);
  }
  
  public void write(byte[] data, int start, int length) {
    if( currentBufOffset + length > blockSize )
    {
      //should change os
      saveData( currentBuf );
      flipCurrentBuffer();
      currentBufOffset=0;
    }
    copyDataToBuffer(data, start, length);
  }
  
  protected void copyDataToBuffer(byte[] data, int start, int length)
  {
    if(length<=0)
      return;
    for(int i=start; i<start+length; ++i)
    {
      currentBuf[currentBufOffset++] = data[i];
    }
  }
  
  //flip current os, and return old current os
  protected final byte[] flipCurrentBuffer()
  {
    byte[] returnBuf = currentBuf;
    currentBuf = ( currentBuf == buf1 ) ? buf2 : buf1;
    return returnBuf;
  }
  
  public void flush() {
    saveData(currentBuf);
    
  }

  protected void saveData( byte[] data )
  {
    if(saverThread == null){
      saverThread = new Thread(fsSaver);
      saverThread.start();
    }
    fsSaver.setData(data, 0, currentBufOffset);
  }

  public void setFsSaver(FsSaver fsSaver) {
    this.fsSaver = fsSaver;
  }
  
  
}
