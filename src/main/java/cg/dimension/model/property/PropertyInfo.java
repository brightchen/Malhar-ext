package cg.dimension.model.property;

public class PropertyInfo
{
  // Expressions set by user to get field values from input tuple.
  private String propertyExpression;

  private SupportType type;

  public PropertyInfo()
  {
  }

  public PropertyInfo(String columnName, String pojoFieldExpression, SupportType type)
  {
    setPropertyExpression(pojoFieldExpression);
    setType(type);
  }

  /**
   * Java expressions that will generate the column value from the POJO.
   *
   */
  public String getPropertyExpression()
  {
    return propertyExpression;
  }

  /**
   * Java expressions that will generate the column value from the POJO.
   *
   */
  public void setPropertyExpression(String expression)
  {
    this.propertyExpression = expression;
  }


  /**
   * the Java type of the column
   */
  public SupportType getType()
  {
    return type;
  }

  /**
   * the Java type of the column
   * 
   * @omitFromUI
   */
  public void setType(SupportType type)
  {
    this.type = type;
  }

  public static enum SupportType {
      BOOLEAN(Boolean.class), SHORT(Short.class), INTEGER(Integer.class), LONG(Long.class), FLOAT(Float.class), DOUBLE(Double.class), STRING(String.class);

      private Class javaType;

      private SupportType(Class javaType)
      {
        this.javaType = javaType;
      }

      public Class getJavaType()
      {
        return javaType;
      }
  }
}
