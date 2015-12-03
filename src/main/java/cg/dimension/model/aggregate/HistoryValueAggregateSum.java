package cg.dimension.model.aggregate;

public abstract class HistoryValueAggregateSum<T> extends AbstractHistoryValueAggregate<T>
{
  @Override
  public T getValue()
  {
    rwLock.readLock().lock();
    try
    {
      for(T value : values)
        sum(value);
    }
    finally
    {
      rwLock.readLock().unlock();
    }
    return getSum();
  }

  protected abstract void sum(T value);
  protected abstract T getSum();
}
