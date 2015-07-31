package cg.dt.malharlib.streamlog;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.datatorrent.api.DefaultPartition;
import com.datatorrent.api.Operator;
import com.datatorrent.api.Operator.InputPort;
import com.datatorrent.api.Partitioner.Partition;
import com.datatorrent.api.Partitioner.PartitionKeys;
import com.datatorrent.api.Partitioner.PartitioningContext;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.datatorrent.api.Partitioner;

public class Partitioner1<T extends Operator> implements Partitioner<T>, Serializable {

  private static final long serialVersionUID = 1597726198768095962L;
  private List<Map<InputPort<?>, PartitionKeys>> partitionKeysList;

  public Partitioner1() {
  }

  public Partitioner1(List<Map<InputPort<?>, PartitionKeys>> partitionKeysList) {
    setPartitionKeysList(partitionKeysList);
  }

  public List<Map<InputPort<?>, PartitionKeys>> getPartitionKeysList() {
    return partitionKeysList;
  }

  public void setPartitionKeysList(List<Map<InputPort<?>, PartitionKeys>> partitionKeysList) {
    this.partitionKeysList = partitionKeysList;
  }

  @Override
  public Collection<Partition<T>> definePartitions(Collection<Partition<T>> partitions, PartitioningContext context) {

    Partition<T> partition = partitions.iterator().next();
    T partitionInstance = partition.getPartitionedInstance();

    if (partitions.iterator().next().getStats() == null) {
      // first call to define partitions

      Collection<Partition<T>> newPartitions = new ArrayList<Partition<T>>(partitionKeysList.size());
      for (int i = 0; i < partitionKeysList.size(); ++i) {
        newPartitions.add(createPartition(partitionKeysList.get(i), partitionInstance));
      }

      // partition the stream that was first connected in the DAG and send full
      // data to remaining input ports
      // this gives control over which stream to partition under default
      // partitioning to the DAG writer
      List<InputPort<?>> inputPortList = context.getInputPorts();
      if (inputPortList != null && !inputPortList.isEmpty()) {
        DefaultPartition.assignPartitionKeys(newPartitions, inputPortList.iterator().next());
      }
      return newPartitions;
    } 
    else {
      // define partitions is being called again
      Collection<Partition<T>> newPartitions = null;
      if (context.getParallelPartitionCount() != 0) {
        newPartitions = repartitionParallel(partitions, context);
      } else if (partition.getPartitionKeys().isEmpty()) {
        newPartitions = repartitionInputOperator(partitions);
      } else {
        newPartitions = repartition(partitions);
      }
      return newPartitions;
    }
    
  }


  /**
   * Change existing partitioning based on runtime state (load). Unlike
   * implementations of {@link Partitioner}), decisions are made
   * solely based on load indicator and operator state is not
   * considered in the event of partition split or merge.
   *
   * @param partitions
   * List of new partitions
   * @return The new operators.
   */
  public static <T extends Operator> Collection<Partition<T>> repartition(Collection<Partition<T>> partitions)
  {
    List<Partition<T>> newPartitions = new ArrayList<Partition<T>>();
    HashMap<Integer, Partition<T>> lowLoadPartitions = new HashMap<Integer, Partition<T>>();
    for (Partition<T> p: partitions) {
      int load = p.getLoad();
      if (load < 0) {
        // combine neighboring underutilized partitions
        PartitionKeys pks = p.getPartitionKeys().values().iterator().next(); // one port partitioned
        for (int partitionKey: pks.partitions) {
          // look for the sibling partition by excluding leading bit
          int reducedMask = pks.mask >>> 1;
          Partition<T> siblingPartition = lowLoadPartitions.remove(partitionKey & reducedMask);
          if (siblingPartition == null) {
            lowLoadPartitions.put(partitionKey & reducedMask, p);
          }
          else {
            // both of the partitions are low load, combine
            PartitionKeys newPks = new PartitionKeys(reducedMask, Sets.newHashSet(partitionKey & reducedMask));
            // put new value so the map gets marked as modified
            InputPort<?> port = siblingPartition.getPartitionKeys().keySet().iterator().next();
            siblingPartition.getPartitionKeys().put(port, newPks);
            // add as new partition
            newPartitions.add(siblingPartition);
            //LOG.debug("partition keys after merge {}", siblingPartition.getPartitionKeys());
          }
        }
      }
      else if (load > 0) {
        // split bottlenecks
        Map<InputPort<?>, PartitionKeys> keys = p.getPartitionKeys();
        Map.Entry<InputPort<?>, PartitionKeys> e = keys.entrySet().iterator().next();

        final int newMask;
        final Set<Integer> newKeys;

        if (e.getValue().partitions.size() == 1) {
          // split single key
          newMask = (e.getValue().mask << 1) | 1;
          int key = e.getValue().partitions.iterator().next();
          int key2 = (newMask ^ e.getValue().mask) | key;
          newKeys = Sets.newHashSet(key, key2);
        }
        else {
          // assign keys to separate partitions
          newMask = e.getValue().mask;
          newKeys = e.getValue().partitions;
        }

        for (int key: newKeys) {
          Partition<T> newPartition = new DefaultPartition<T>(p.getPartitionedInstance());
          newPartition.getPartitionKeys().put(e.getKey(), new PartitionKeys(newMask, Sets.newHashSet(key)));
          newPartitions.add(newPartition);
        }
      }
      else {
        // leave unchanged
        newPartitions.add(p);
      }
    }
    // put back low load partitions that could not be combined
    newPartitions.addAll(lowLoadPartitions.values());
    return newPartitions;
  }

  /**
   * Adjust the partitions of an input operator (operator with no connected input stream).
   *
   * @param <T> The operator type
   * @param partitions
   * @return The new operators.
   */
  public static <T extends Operator> Collection<Partition<T>> repartitionInputOperator(Collection<Partition<T>> partitions)
  {
    List<Partition<T>> newPartitions = new ArrayList<Partition<T>>();
    List<Partition<T>> lowLoadPartitions = new ArrayList<Partition<T>>();
    for (Partition<T> p: partitions) {
      int load = p.getLoad();
      if (load < 0) {
        if (!lowLoadPartitions.isEmpty()) {
          newPartitions.add(lowLoadPartitions.remove(0));
        }
        else {
          lowLoadPartitions.add(p);
        }
      }
      else if (load > 0) {
        newPartitions.add(new DefaultPartition<T>(p.getPartitionedInstance()));
        newPartitions.add(new DefaultPartition<T>(p.getPartitionedInstance()));
      }
      else {
        newPartitions.add(p);
      }
    }
    newPartitions.addAll(lowLoadPartitions);
    return newPartitions;
  }

  
  public static <T extends Operator> Collection<Partition<T>> repartitionParallel(Collection<Partition<T>> partitions,
      PartitioningContext context) {
    List<Partition<T>> newPartitions = Lists.newArrayList();
    newPartitions.addAll(partitions);

    int morePartitionsToCreate = context.getParallelPartitionCount() - newPartitions.size();
    if (morePartitionsToCreate < 0) {
      // Delete partitions
      Iterator<Partition<T>> partitionIterator = newPartitions.iterator();

      while (morePartitionsToCreate++ < 0) {
        partitionIterator.next();
        partitionIterator.remove();
      }
    } else {
      // Add more partitions
      T anOperator = newPartitions.iterator().next().getPartitionedInstance();

      while (morePartitionsToCreate-- > 0) {
        DefaultPartition<T> partition = new DefaultPartition<T>(anOperator);
        newPartitions.add(partition);
      }
    }
    return newPartitions;
  }

  protected Partition<T> createPartition(Map<InputPort<?>, PartitionKeys> partitionKeys, T partitionInstance) {
    return new DefaultPartition<T>(partitionInstance, partitionKeys, 0, null);
  }

  @Override
  public void partitioned(Map<Integer, com.datatorrent.api.Partitioner.Partition<T>> partitions) {
    // TODO Auto-generated method stub

  }

}
