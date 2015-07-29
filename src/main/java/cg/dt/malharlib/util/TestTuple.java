package cg.dt.malharlib.util;

import java.io.Serializable;

public class TestTuple implements Serializable
{
  private static final long serialVersionUID = 2153417121590225192L;
  
  public static String getRowExpression()
  {
    return "row";
  }
  

  
  private Long rowId = null;
  private String name;
  private int age;
  private String address;

  public TestTuple(){}
  
  public TestTuple(long rowId)
  {
    this(rowId, "name" + rowId, (int) rowId, "address" + rowId);
  }

  public TestTuple(long rowId, String name, int age, String address)
  {
    this.rowId = rowId;
    setName(name);
    setAge(age);
    setAddress(address);
  }
  
  public TestTuple(String row)
  {
    setRow(row);
  }

  public String getRow()
  {
    return String.valueOf(rowId);
  }
  public void setRow( String row )
  {
    setRowId( Long.valueOf(row) );
  }
  public void setRowId( Long rowId )
  {
    this.rowId = rowId;
  }
  public Long getRowId()
  {
    return rowId;
  }
  public String getName()
  {
    return name;
  }

  public void setName(String name)
  {
    this.name = name;
  }

  public Integer getAge()
  {
    return age;
  }

  public void setAge( Integer age)
  {
    this.age = age;
  }

  public String getAddress()
  {
    return address;
  }

  public void setAddress(String address)
  {
    this.address = address;
  }
  
  @Override
  public boolean equals( Object obj )
  {
    if( obj == null )
      return false;
    if( !( obj instanceof TestTuple ) )
      return false;
    
    return completeEquals( (TestTuple)obj );
  }

  public boolean outputFieldsEquals( TestTuple other )
  {
    if( other == null )
      return false;
    if( !fieldEquals( getName(), other.getName() ) )
      return false;
    if( !fieldEquals( getAge(), other.getAge() ) )
      return false;
    if( !fieldEquals( getAddress(), other.getAddress() ) )
      return false;
    return true;
  }
  
  public boolean completeEquals( TestTuple other )
  {
    if( other == null )
      return false;
    if( !outputFieldsEquals( other ) )
      return false;
    if( !fieldEquals( getRow(), other.getRow() ) )
      return false;
    return true;
  }
  
  public <T> boolean fieldEquals( T v1, T v2 )
  {
    if( v1 == null && v2 == null )
      return true;
    if( v1 == null || v2 == null )
      return false;
    return v1.equals( v2 );
  }
  
  @Override
  public String toString()
  {
    return String.format( "id={%d}; name={%s}; age={%d}; address={%s}", rowId, name, age, address);
  }
}
