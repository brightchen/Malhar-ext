package cg.dimension.model.criteria;

import cg.dimension.model.property.BeanPropertyValueGenerator;

/**
 * The expected value set from the client code.
 * the comparing value is get from the bean ( the property value )
 * the matcher set from the client code
 * 
 * 
 * @author bright
 *
 */
public class PropertyCriteria<EV, B>
{
  protected BeanPropertyValueGenerator<B> valueProvider;
  protected EV expectedValue;
  protected Matcher<EV, Object> matcher;

  public PropertyCriteria(){}
  
  public PropertyCriteria(BeanPropertyValueGenerator<B> valueProvider, EV expectedValue, Matcher<EV, Object> matcher)
  {
    setValueProvider(valueProvider);
    setExpectedValue(expectedValue);
    setMatcher(matcher);
  }
  
  public boolean matches(B bean)
  {
    Object value = valueProvider.getPropertyValue(bean);
    return matcher.matches(expectedValue, value);
  }

  public BeanPropertyValueGenerator<B> getValueProvider()
  {
    return valueProvider;
  }

  public void setValueProvider(BeanPropertyValueGenerator<B> valueProvider)
  {
    this.valueProvider = valueProvider;
  }

  public EV getExpectedValue()
  {
    return expectedValue;
  }

  public void setExpectedValue(EV expectedValue)
  {
    this.expectedValue = expectedValue;
  }

  public Matcher<EV, Object> getMatcher()
  {
    return matcher;
  }

  public void setMatcher(Matcher<EV, Object> matcher)
  {
    this.matcher = matcher;
  }
  
  
}
