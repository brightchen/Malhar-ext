package cg.dimension.model;

public class Range<T> {
  public final T from;
  public final T to;
  
  public Range(T from, T to)
  {
    this.from = from;
    this.to = to;
  }
  
  @SuppressWarnings({ "rawtypes", "unchecked" })
  public boolean isInRange(T value)
  {
    if(value instanceof Comparable)
    {
      int result = ((Comparable)value).compareTo(from);
      if(result == 0)
        return true;
      return (Math.signum(result) * Math.signum(((Comparable)value).compareTo(to)) > 0);
    }
    throw new IllegalStateException("The range is not comparable");
  }
}
