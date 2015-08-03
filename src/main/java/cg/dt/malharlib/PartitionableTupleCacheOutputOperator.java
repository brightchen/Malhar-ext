package cg.dt.malharlib;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import com.datatorrent.api.DefaultPartition;
import com.datatorrent.api.Partitioner;
import com.datatorrent.api.Operator.InputPort;
import com.datatorrent.api.Partitioner.Partition;
import com.datatorrent.api.Partitioner.PartitionKeys;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

public class PartitionableTupleCacheOutputOperator<T> extends TupleCacheOutputOperator<T> implements Partitioner<T>{
  private static final long serialVersionUID = 5697785701926614411L;

  @Override
  public Collection<com.datatorrent.api.Partitioner.Partition<T>> definePartitions(
      Collection<com.datatorrent.api.Partitioner.Partition<T>> partitions,
      com.datatorrent.api.Partitioner.PartitioningContext context) {
    
    Partition<T> partition = partitions.iterator().next();
    T partitionInstance = partition.getPartitionedInstance();

    List<Map<InputPort<?>, PartitionKeys>> partitionKeysList = Lists.newArrayList();
    {
      Map<InputPort<?>, PartitionKeys> partionKeys = Maps.newHashMap();
      partionKeys.put(this.inputPort, new PartitionKeys(0x03, Sets.newHashSet(2,3)));
      partitionKeysList.add(partionKeys);
    }
    {
      Map<InputPort<?>, PartitionKeys> partionKeys = Maps.newHashMap();
      partionKeys.put(this.inputPort, new PartitionKeys(0x03, Sets.newHashSet(1)));
      partitionKeysList.add(partionKeys);
    }
    Collection<Partition<T>> newPartitions = new ArrayList<Partition<T>>(partitionKeysList.size());
    for (int i = 0; i < partitionKeysList.size(); ++i) {
      newPartitions.add(createPartition(partitionKeysList.get(i), partitionInstance));
    }

    return newPartitions;
  }

  @Override
  public void partitioned(Map<Integer, com.datatorrent.api.Partitioner.Partition<T>> partitions) {
    // TODO Auto-generated method stub
    
  }

  protected Partition<T> createPartition(Map<InputPort<?>, PartitionKeys> partitionKeys, T partitionInstance) {
    return new DefaultPartition<T>(partitionInstance, partitionKeys, 0, null);
  }
}
