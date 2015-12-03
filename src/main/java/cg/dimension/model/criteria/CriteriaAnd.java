package cg.dimension.model.criteria;

public class CriteriaAnd<T> extends CriteriaList implements Criteria
{
  @Override
  public boolean matches()
  {
    if(criterias == null || criterias.isEmpty())
      return true;
    for(Criteria key : criterias)
    {
      if(!key.matches())
        return false;
    }
    return true;
  }

}
