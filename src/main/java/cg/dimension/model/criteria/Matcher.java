package cg.dimension.model.criteria;

public interface Matcher<EV, V>
{
  public boolean matches(EV expectedValue, V value);
}
