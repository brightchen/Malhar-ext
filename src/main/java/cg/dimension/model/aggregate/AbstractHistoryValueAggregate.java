package cg.dimension.model.aggregate;

import java.util.List;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import com.google.common.collect.Lists;

/**
 * this implementation keep all the history value.
 * and calculate the value when query.
 * concurrency should take into consideration
 * 
 * @author bright
 *
 */
public abstract class AbstractHistoryValueAggregate<T> implements Aggregate<T>
{
  public static final int DEFAULT_INIT_CAPACITY = 1024;
  
  protected int initCapacity = DEFAULT_INIT_CAPACITY;
  protected List<T> values = Lists.newArrayListWithCapacity(initCapacity);
  
  protected ReadWriteLock rwLock = new ReentrantReadWriteLock();
  
  @Override
  public void addValue(T value)
  {
    rwLock.writeLock().lock();
    try
    {
      values.add(value);
    }
    finally
    {
      rwLock.writeLock().unlock();
    }
  }
}
