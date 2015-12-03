package cg.dimension.model.criteria;

public class EqualsMatcher<EV> implements Matcher<EV, Object>
{
  @Override
  public boolean matches(EV expectedValue, Object value)
  {
    return expectedValue.equals(value);
  }

}
