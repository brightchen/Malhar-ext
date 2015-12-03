package cg.dimension.model.aggregate;

public class IncrementalAggregateSumDouble implements Aggregate<Double>
{
  protected double sum = 0.0;
  
  @Override
  public Double getValue()
  {
    return sum;
  }

  @Override
  public void addValue(Double value)
  {
    if(value == null)
      return;
    sum += value;
  }

}