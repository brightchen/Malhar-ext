package cg.dimension.model.criteria;

import cg.dimension.model.Range;

public class RangeCriteria<T> //implements Criteria<T>
{
  protected Range<T> range;
  
  //@Override
  public boolean matches(T value)
  {
    return range.isInRange(value);
  }

  public Range<T> getRange()
  {
    return range;
  }

  public void setRange(Range<T> range)
  {
    this.range = range;
  }

  
}
