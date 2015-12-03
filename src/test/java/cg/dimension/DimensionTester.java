package cg.dimension;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cg.dimension.compute.DimensionComputation;
import cg.dimension.model.aggregate.AggregateType;
import cg.dimension.model.criteria.EqualsMatcher;
import cg.dimension.model.criteria.PropertyCriteria;
import cg.dimension.model.property.BeanPropertyValueGenerator;

public class DimensionTester
{
  private static final Logger logger = LoggerFactory.getLogger(DimensionTester.class);
  
  protected int COUNT = 10000;
  
  @Test
  public void test()
  {
    DimensionComputation<TestRecord> computation = new DimensionComputation<TestRecord>();
    
    //1. sum of salary of age equals 30;
    {
      BeanPropertyValueGenerator valueGenerator = new BeanPropertyValueGenerator(TestRecord.class, "age", Integer.class);
      PropertyCriteria criteria = new PropertyCriteria(valueGenerator, 30, new EqualsMatcher());
      computation.addAggregator(criteria, new BeanPropertyValueGenerator(TestRecord.class, "salary", Double.class), Double.class, AggregateType.SUM );
    }
    
    //2. average age of the record received last 10 minutes;
    
    for(int i=0; i<COUNT; ++i)
      computation.processRecord(new TestRecord(i));
    
    logger.info("value is {}", computation.getAggregateValue());
  }
  
  
}
