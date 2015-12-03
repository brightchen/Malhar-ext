package cg.dimension.model.criteria;

public class CriteriaOr  extends CriteriaList implements Criteria
{
  @Override
  public boolean matches()
  {
    if(criterias == null || criterias.isEmpty())
      return true;
    for(Criteria criteria : criterias)
    {
      if(criteria.matches())
        return true;
    }
    return false;
  }

}

