package cg.dt.malharlib.util;

import java.lang.reflect.Constructor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This is a copy from contrib, should be merged later.
 * 
 */
public class TupleGenerator<T>
{
  private static final Logger logger = LoggerFactory.getLogger( TupleGenerator.class );
      
  private volatile long rowId = 0;
  private Constructor<T> constructor;
  
  private static Class<?>[] paramTypes = new Class<?>[]{ Long.class, long.class };
  
  public TupleGenerator()
  {
  }
  
  public TupleGenerator( Class<T> tupleClass )
  {
    useTupleClass( tupleClass );
  }
  
  public void useTupleClass( Class<T> tupleClass )
  {
    for( Class<?> paramType : paramTypes )
    {
      constructor = tryGetConstructor( tupleClass, paramType );
      if( constructor != null )
        break;
    }
    if( constructor == null )
    {
      logger.error( "Not found proper constructor." );
      throw new RuntimeException( "Not found proper constructor." );
    }
  }
  
  protected Constructor<T> tryGetConstructor( Class<T> tupleClass, Class<?> parameterType )
  {
    try
    {
      return tupleClass.getConstructor( parameterType );
    }
    catch( Exception e )
    {
      return null;
    }
  }
  
  public void reset()
  {
    rowId = 0;
  }
  
  public T getNextTuple()
  {
    if( constructor == null )
      throw new RuntimeException( "Not found proper constructor." );
    
    long curRowId = ++rowId;
    try
    {
      return constructor.newInstance( curRowId );
    }
    catch( Exception e)
    {
      logger.error( "newInstance failed.", e );
      return null;
    }
  }

}