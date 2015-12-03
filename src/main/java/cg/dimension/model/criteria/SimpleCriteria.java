package cg.dimension.model.criteria;

public class SimpleCriteria<T> //implements Criteria<T>
{
  //@Override
  public boolean matches(T value)
  {
    return equals(value);
  }

}
