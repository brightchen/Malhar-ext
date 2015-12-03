package cg.dimension.compute;

import cg.dimension.ReflectionUtil;
import cg.dimension.model.aggregate.Aggregate;
import cg.dimension.model.aggregate.AggregateType;
import cg.dimension.model.aggregate.IncrementalAggregateSumDouble;
import cg.dimension.model.aggregate.IncrementalAggregateSumLong;
import cg.dimension.model.criteria.PropertyCriteria;
import cg.dimension.model.property.BeanPropertyValueGenerator;

public class DimensionComputation<T>
{
  protected PropertyCriteria criteria;
  protected BeanPropertyValueGenerator valueGenerator;
  protected Aggregate aggregate;
  
  public void addAggregator(PropertyCriteria criteria, BeanPropertyValueGenerator valueGenerator, Class aggregateValueType, AggregateType aggregateType)
  {
    this.criteria = criteria;
    this.valueGenerator = valueGenerator;
    aggregate = createDefaultAggregate(aggregateType, aggregateValueType);
  }
  
  public void processRecord(T record)
  {
    if(!criteria.matches(record))
      return;
    
    //calculate aggregate
    aggregate.addValue(valueGenerator.getPropertyValue(record));
  }
  
  protected Aggregate createDefaultAggregate(AggregateType aggregateType, Class aggregateValueType)
  {
    switch(aggregateType)
    {
      case SUM:
        if(aggregateValueType.equals(Integer.class) || aggregateValueType.equals(Long.class) || aggregateValueType.equals(Short.class))
          return new IncrementalAggregateSumLong();
        if(aggregateValueType.equals(Double.class) || aggregateValueType.equals(Float.class))
          return new IncrementalAggregateSumDouble();
    }
    throw new IllegalArgumentException("Unsupported.");
  }
  
  public Object getAggregateValue()
  {
    return aggregate.getValue();
  }
}
