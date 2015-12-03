package cg.dimension;

import java.lang.reflect.Method;

public class ReflectionUtil
{
  public static String getGetterMethodName(String propertyName)
  {
    return "get" + propertyName.substring(0,1).toLowerCase() + propertyName.substring(1);
  }
  
  public static String getIsMethodName(String propertyName)
  {
    return "is" + propertyName.substring(0,1).toLowerCase() + propertyName.substring(1);
  }
  
  public static Class<?> getPropertyType(Class<?> beanClass, String propertyName)
  {
    Method method = null;
    try
    {
      method = beanClass.getMethod(getGetterMethodName(propertyName), null);
    }
    catch(Exception e)
    {
      try
      {
        method = beanClass.getMethod(getIsMethodName(propertyName), null);
      }
      catch(Exception e1)
      {
        throw new IllegalArgumentException("Can't get getter method of property " + propertyName);
      }
    }
    return method.getReturnType();
  }
}
