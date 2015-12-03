package cg.dimension.model.criteria;

/**
 * provide value by the property of a bean
 * 
 * @author bright
 *
 */
public class BeanPropertyValueProvider<B, T> implements ValueProvider<T>
{
  protected B bean;
  protected String propertyName;
  
  @Override
  public T getValue()
  {
    // TODO Auto-generated method stub
    return null;
  }

  public B getBean()
  {
    return bean;
  }

  public void setBean(B bean)
  {
    this.bean = bean;
  }

  public String getPropertyName()
  {
    return propertyName;
  }

  public void setPropertyName(String propertyName)
  {
    this.propertyName = propertyName;
  }
  
  
}
