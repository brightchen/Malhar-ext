package cg.dt.malharlib;

public interface Writer {
  public void write(byte[] data, int start, int length);
  public void flush();
}
