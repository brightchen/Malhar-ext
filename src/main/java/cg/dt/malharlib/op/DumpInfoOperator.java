package cg.dt.malharlib.op;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.common.util.BaseOperator;

public class DumpInfoOperator extends BaseOperator
{
  private static final Logger logger = LoggerFactory.getLogger(DumpInfoOperator.class);
  
  private String otherInfo;
  
  private DumpConf dumpConf;
  
  public DumpInfoOperator()
  {
    dumpConf = DumpConf.instance();
  }

  public final transient DefaultInputPort<Object> input = new DefaultInputPort<Object>()
  {
    @Override
    @SuppressWarnings("UseOfSystemOutOrSystemErr")
    public void process(Object t)
    {
      String s;
      if (stringFormat == null) {
        s = t.toString();
      }
      else {
        s = String.format(stringFormat, t);
      }
      if (!silent) {
        System.out.println(s);
      }
      if (debug) {
        logger.info(s);
      }
    }
  };
  public boolean silent = false;

  /**
   * @return the silent
   */
  public boolean isSilent()
  {
    return silent;
  }

  /**
   * @param silent the silent to set
   */
  public void setSilent(boolean silent)
  {
    this.silent = silent;
  }

  /**
   * When set to true, tuples are also logged at INFO level.
   */
  private boolean debug;
  /**
   * A formatter for {@link String#format}
   */
  private String stringFormat;

  public boolean isDebug()
  {
    return debug;
  }

  public void setDebug(boolean debug)
  {
    this.debug = debug;
  }

  public String getStringFormat()
  {
    return stringFormat;
  }

  public void setStringFormat(String stringFormat)
  {
    this.stringFormat = stringFormat;
  }
  
  
  /**
   * {@inheritDoc}
   */
  @Override
  public void setup(OperatorContext context)
  {
    logger.info("setup().");
    logger.info("other info --------------begin------------------");
    if(otherInfo != null)
      logger.info(otherInfo);
    logger.info("other info ----------- ---end-------------- ----");
    
    logger.info("dump conf --------------begin------------------");
    logger.info(dumpConf.host);
    logger.info("{}",dumpConf.port);
    logger.info(dumpConf.userName);
    logger.info("dump conf ----------- ---end-------------- ----");
    
    
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void beginWindow(long windowId)
  {
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void endWindow()
  {
  }
  

  /**
   * {@inheritDoc}
   */
  @Override
  public void teardown()
  {
    logger.info("teardown().");
  }

  public String getOtherInfo()
  {
    return otherInfo;
  }

  public void setOtherInfo(String otherInfo)
  {
    this.otherInfo = otherInfo;
  }

  
}