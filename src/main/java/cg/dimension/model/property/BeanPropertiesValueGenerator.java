package cg.dimension.model.property;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.datatorrent.lib.util.PojoUtils;
import com.datatorrent.lib.util.PojoUtils.Getter;

/**
 * This class keep the map from propertyExpression to the Getter/Setter methods.
 * So the client code no need to keep these information.
 * 
 * @author bright
 *
 * @param <T>
 *          The type of PropertyInfo
 */
public class BeanPropertiesValueGenerator<T extends PropertyInfo>
{
  public static interface PropertyValueHandler<T extends PropertyInfo>
  {
    public void handlePropertyValue(T propertyInfo, Object value);
  }

  // map of propertyExpression to propertyInfo
  protected Map<String, T> propertyInfoMap = new HashMap<String, T>();

  // map of propertyInfo to Getter method;
  protected Map<T, Getter<Object, Object>> propertyGetterMap = new HashMap<T, Getter<Object, Object>>();

  protected BeanPropertiesValueGenerator()
  {
  }

  @SuppressWarnings("unchecked")
  public static <T extends PropertyInfo> BeanPropertiesValueGenerator<T> getPropertyValueGenerator(final Class<?> clazz,
      List<T> propertyInfos)
  {
    return new BeanPropertiesValueGenerator<T>(clazz, propertyInfos);
  }

  protected BeanPropertiesValueGenerator(final Class<?> clazz, List<T> propertiesInfo)
  {
    for (T propertyInfo : propertiesInfo) {
      propertyInfoMap.put(propertyInfo.getPropertyExpression(), propertyInfo);

      Getter<Object, Object> getter = PojoUtils.createGetter(clazz, propertyInfo.getPropertyExpression(),
          propertyInfo.getType().getJavaType());
      propertyGetterMap.put(propertyInfo, getter);
    }

  }

  /**
   * use PropertyValueHandler handle the value
   * 
   * @param obj
   * @param propertyValueHandler
   * @return
   * @since 3.0.0
   */
  @SuppressWarnings({ "rawtypes", "unchecked" })
  public void handlePropertiesValue(Object obj, PropertyValueHandler propertyValueHandler)
  {
    for (Map.Entry<T, Getter<Object, Object>> entry : propertyGetterMap.entrySet()) {
      Getter<Object, Object> getter = entry.getValue();
      if (getter != null) {
        Object value = getter.get(obj);
        propertyValueHandler.handlePropertyValue(entry.getKey(), value);
      }
    }
  }

  public Object getPropertyValue(Object obj, String propertyExpression)
  {
    try {
      T propertyInfo = propertyInfoMap.get(propertyExpression);
      Getter<Object, Object> getter = propertyGetterMap.get(propertyInfo);

      return getter.get(obj);
    } catch (NullPointerException e) {
      throw new IllegalArgumentException("Can't find Getter for expression " + propertyExpression);
    }
  }

  public Map<String, Object> getPropertiesValueAsMap(Object obj)
  {
    Map<String, Object> propertiesValue = new HashMap<String, Object>();
    for (Map.Entry<T, Getter<Object, Object>> entry : propertyGetterMap.entrySet()) {
      Getter<Object, Object> getter = entry.getValue();
      if (getter != null) {
        Object value = getter.get(obj);
        propertiesValue.put(entry.getKey().getPropertyExpression(), value);
      }
    }
    return propertiesValue;
  }

}
