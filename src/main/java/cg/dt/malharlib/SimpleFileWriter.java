package cg.dt.malharlib;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;

/**
 * simplly write content to local file
 * @author bright
 *
 */
public class SimpleFileWriter implements Writer{

  private String filePath;
  private FileOutputStream fos;
  
  public SimpleFileWriter()
  {
  }

  public SimpleFileWriter( String filePath )
  {
    setFilePath( filePath );
  }

  
  public String getFilePath() {
    return filePath;
  }

  public void setFilePath(String filePath) {
    this.filePath = filePath;
  }

  @Override
  public void write(byte[] data, int start, int length) {
    if(fos==null)
    {
      createFileOutputStream();
    }
    try {
      fos.write(data, start, length);
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  @Override
  public void flush() {
    if(fos==null)
      return;
    
    try {
      fos.flush();
    } catch (IOException e) {
      e.printStackTrace();
    }
    
  }

  protected synchronized void createFileOutputStream()
  {
    if(fos!=null)
      return;
    try {
      fos = new FileOutputStream(filePath);
    } catch (FileNotFoundException e) {
      e.printStackTrace();
    }
  }
}
