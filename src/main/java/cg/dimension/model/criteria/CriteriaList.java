package cg.dimension.model.criteria;

import java.util.Collection;
import java.util.List;

import com.google.common.collect.Lists;

public class CriteriaList
{
  protected List<Criteria> criterias;

  public void setKeys(Criteria ... keys)
  {
    this.criterias = Lists.newArrayList(keys);
  }
  
  public void addKey(Criteria key)
  {
    if(key == null)
      return;
    if(criterias == null)
      criterias = Lists.newArrayList();
    criterias.add(key);
  }
  
  public void addKeys(Criteria ... keys)
  {
    if(keys == null)
      return;
    if(this.criterias == null)
      this.criterias = Lists.newArrayList();
    for(Criteria key : keys)
      this.criterias.add(key);
  }
  
  public void addKeys(Collection<Criteria> keys)
  {
    if(keys == null)
      return;
    if(this.criterias == null)
      this.criterias = Lists.newArrayList();
    this.criterias.addAll(keys);
  }
}
