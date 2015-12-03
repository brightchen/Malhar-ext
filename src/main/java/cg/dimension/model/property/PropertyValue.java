package cg.dimension.model.property;

public class PropertyValue<T>
{
  protected Class<T> type;
  protected T value;
  
  public PropertyValue(){}
  
  public PropertyValue(Class<T> type, T value)
  {
    setType(type);
    setValue(value);
  }

  public Class<T> getType()
  {
    return type;
  }

  public void setType(Class<T> type)
  {
    this.type = type;
  }

  public T getValue()
  {
    return value;
  }

  public void setValue(T value)
  {
    this.value = value;
  }
  

}
