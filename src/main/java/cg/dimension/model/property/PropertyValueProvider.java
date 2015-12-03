package cg.dimension.model.property;

/**
 * The bean class can implement this class to provide property values.
 * no reflection required by this way
 * 
 * @author bright
 *
 */
public interface PropertyValueProvider
{
  public void get(String propName, PropertyValue<?> value);
}
