package cg.dimension.model.aggregate;

public class HistoryValueAggregateSumDouble extends HistoryValueAggregateSum<Double>
{
  protected double sum = 0.0;
  
  @Override
  protected void sum(Double value)
  {
    sum += value;
  }

  @Override
  protected Double getSum()
  {
    return sum;
  }
}