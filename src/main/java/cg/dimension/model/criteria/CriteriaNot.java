package cg.dimension.model.criteria;

public class CriteriaNot implements Criteria
{
  protected Criteria criteria;
  
  @Override
  public boolean matches()
  {
    return !criteria.matches();
  }

  public void set(Criteria key)
  {
    this.criteria = key;
  }
}