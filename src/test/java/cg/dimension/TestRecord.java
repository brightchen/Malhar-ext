package cg.dimension;

import cg.dimension.model.property.PropertyValue;
import cg.dimension.model.property.PropertyValueProvider;

public class TestRecord implements PropertyValueProvider
{
  public int id;
  public String name;
  public int age;
  public double salary;
  
  public TestRecord(int id)
  {
    this(id, "name"+id, id%100, id%1000);
  }
  
  public TestRecord(int id, String name, int age, float salary)
  {
    this.id = id;
    this.name = name;
    this.age = age;
    this.salary = salary;
  }
  
  @Override
  public void get(String propName, PropertyValue<?> value)
  {
    if(propName.equals("id"))
    {
      value = new PropertyValue<Integer>(Integer.class, id);
      return;
    }
    if(propName.equals("name"))
    {
      value = new PropertyValue<String>(String.class, name);
      return;
    }
    if(propName.equals("age"))
    {
      value = new PropertyValue<Integer>(Integer.class, age);
      return;
    }
    if(propName.equals("salary"))
    {
      value = new PropertyValue<Double>(Double.class, salary);
      return;
    }
  }
  
  
}
