package cg.dimension.model.aggregate;

public class HistoryValueAggregateSumLong extends HistoryValueAggregateSum<Long>
{
  protected long sum = 0;
  
  @Override
  protected void sum(Long value)
  {
    sum += value;
  }

  @Override
  protected Long getSum()
  {
    return sum;
  }
}
