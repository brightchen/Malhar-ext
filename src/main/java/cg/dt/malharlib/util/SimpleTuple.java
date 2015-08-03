package cg.dt.malharlib.util;

import java.io.Serializable;

public class SimpleTuple implements Serializable{
  private static final long serialVersionUID = 1401771323484754627L;

  private String id;
  
  public SimpleTuple(){}
  
  public SimpleTuple(long id)
  {
    setId( String.valueOf(id) );
  }
  public SimpleTuple(String id)
  {
    setId(id);
  }

  public String getId() {
    return id;
  }

  public void setId(String id) {
    this.id = id;
  }
  
  @Override
  public int hashCode()
  {
    return Integer.valueOf(id);
  }
  
  @Override
  public String toString()
  {
    return id;
  }
}
