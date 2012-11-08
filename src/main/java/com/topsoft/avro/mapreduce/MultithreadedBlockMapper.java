package com.topsoft.avro.mapreduce;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.MapContext;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.StatusReporter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.map.MultithreadedMapper;
import org.apache.hadoop.mapreduce.lib.map.WrappedMapper;
import org.apache.hadoop.mapreduce.task.MapContextImpl;
import org.apache.hadoop.util.ReflectionUtils;

public class MultithreadedBlockMapper<K1,V1,K2,V2> extends Mapper<K1,V1,K2,V2> {
  
  private static final Log LOG = LogFactory
      .getLog(MultithreadedBlockMapper.class);
  public static String NUM_THREADS = "mapreduce.mapper.multithreadedmapper.threads";
  public static String MAP_CLASS = "mapreduce.mapper.multithreadedmapper.mapclass";
  public static String MAP_TASK_TIMEOUT = "mapred.task.timeout";
  
  private Class<? extends BlockMapper<K1,V1,K2,V2>> mapClass;
  private Context outer;
  private List<MapRunner> runners;
  
  /**
   * The number of threads in the thread pool that will run the map function.
   * 
   * @param job
   *          the job
   * @return the number of threads
   */
  public static int getNumberOfThreads(JobContext job) {
    return job.getConfiguration().getInt(NUM_THREADS, 10);
  }
  
  /**
   * The time of threads timeout.
   * 
   * @param job
   *          the job
   * @return the time out
   */
  public static void setThreadTimeOut(JobContext job, int timeout) {
    job.getConfiguration().setInt(MAP_TASK_TIMEOUT, timeout);
  }
  
  /**
   * Set the number of threads in the pool for running maps.
   * 
   * @param job
   *          the job to modify
   * @param threads
   *          the new number of threads
   */
  public static void setNumberOfThreads(Job job, int threads) {
    job.getConfiguration().setInt(NUM_THREADS, threads);
  }
  
  /**
   * Get the application's mapper class.
   * 
   * @param <K1>
   *          the map's input key type
   * @param <V1>
   *          the map's input value type
   * @param <K2>
   *          the map's output key type
   * @param <V2>
   *          the map's output value type
   * @param job
   *          the job
   * @return the mapper class to run
   */
  @SuppressWarnings("unchecked")
  public static <K1,V1,K2,V2> Class<BlockMapper<K1,V1,K2,V2>> getMapperClass(
      JobContext job) {
    return (Class<BlockMapper<K1,V1,K2,V2>>) job.getConfiguration().getClass(
        MAP_CLASS, BlockMapper.class);
  }
  
  /**
   * Set the application's mapper class.
   * 
   * @param <K1>
   *          the map input key type
   * @param <V1>
   *          the map input value type
   * @param <K2>
   *          the map output key type
   * @param <V2>
   *          the map output value type
   * @param job
   *          the job to modify
   * @param cls
   *          the class to use as the mapper
   */
  public static <K1,V1,K2,V2> void setMapperClass(Job job,
      Class<? extends BlockMapper<K1,V1,K2,V2>> cls) {
    if (MultithreadedBlockMapper.class.isAssignableFrom(cls)) {
      throw new IllegalArgumentException("Can't have recursive "
          + "MultithreadedMapper instances.");
    }
    job.getConfiguration().setClass(MAP_CLASS, cls, Mapper.class);
  }
  
  /**
   * Run the application's maps using a thread pool.
   */
  @Override
  public void run(Context context) throws IOException, InterruptedException {
    outer = context;
    int numberOfThreads = getNumberOfThreads(context);
    mapClass = getMapperClass(context);
    if (LOG.isDebugEnabled()) {
      LOG.debug("Configuring multithread runner to use " + numberOfThreads
          + " threads");
    }
    
    runners = new ArrayList<MapRunner>(numberOfThreads);
    for (int i = 0; i < numberOfThreads; ++i) {
      MapRunner thread = new MapRunner(context);
      thread.start();
      runners.add(i, thread);
    }
    
    // select a timeout that avoids a task timeout
    long timeout = context.getConfiguration().getInt("crawl.fetch.timeout",
        2 * 60 * 1000);
    LOG.info("fetch time out : " + timeout);
    long start = System.currentTimeMillis();
    int numblockthread = 0;
    ArrayList<K1> lst = new ArrayList<K1>();
    int runThread = numberOfThreads;
    int pages = 0;
    int errors = 0;
    int bytes = 0;
    while (runThread > 0) {
      LOG.warn(this.getClass().getSimpleName() + " is run with " + runThread
          + " threads");
      runThread = numberOfThreads;
      pages = 0;
      errors = 0;
      bytes = 0;
      for (int i = 0; i < numberOfThreads; i++) {
        MapRunner thread = runners.get(i);
        // LOG.warn("Thread " + i + " pages:"
        // +
        // thread.mapper.pages+" errors:"+thread.mapper.errors+" bytes:"+thread.mapper.bytes);
        pages += thread.mapper.pages;
        errors += thread.mapper.errors;
        bytes += thread.mapper.bytes;
        if (thread.isAlive() && !thread.mapper.isBlock) {
          long blocktime = thread.mapper.getBlockTime();
          // LOG.warn("Thread " + i + " block time : " + blocktime);
          if (blocktime > timeout) {
            thread.mapper.isBlock = true;
            thread.mapper.BlockRecord();
            thread.mapper.cleanup(thread.mapper.context);
            thread.interrupt();
            lst.add(thread.mapper.currentKey);
            // LOG.warn("Thread " + i + " block at key:"
            // + thread.mapper.currentKey);
            numblockthread++;
          }
        } else runThread--;
        
      }
      String status;
      synchronized (this) {
        long elapsed = (System.currentTimeMillis() - start) / 1000;
        status = pages + " pages, " + errors + " errors, "
            + Math.round(((float) pages * 10) / elapsed) / 10.0 + " pages/s, "
            + Math.round(((((float) bytes) * 8) / 1024) / elapsed) + " kb/s, ";
        
        LOG.info(status);
        context.setStatus(status);
      }
      Thread.sleep(5000);
    }
    cleanup(context);
    LOG.warn("Aborting with " + numblockthread
        + " block threads. and with block key:" + lst);
    
  }
  
  public int getActiveThread() {
    int iret = 0;
    for (MapRunner thread : runners) {
      if (thread.isAlive() && !thread.mapper.isBlock) iret++;
    }
    return iret;
  }
  
  private class SubMapRecordReader extends RecordReader<K1,V1> {
    private K1 key;
    private V1 value;
    private Configuration conf;
    
    @Override
    public void close() throws IOException {}
    
    @Override
    public float getProgress() throws IOException, InterruptedException {
      return 0;
    }
    
    @Override
    public void initialize(InputSplit split, TaskAttemptContext context)
        throws IOException, InterruptedException {
      conf = context.getConfiguration();
    }
    
    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
      synchronized (outer) {
        if (!outer.nextKeyValue()) {
          return false;
        }
        key = ReflectionUtils.copy(outer.getConfiguration(),
            outer.getCurrentKey(), key);
        value = ReflectionUtils.copy(conf, outer.getCurrentValue(), value);
        return true;
      }
    }
    
    public K1 getCurrentKey() {
      return key;
    }
    
    @Override
    public V1 getCurrentValue() {
      return value;
    }
  }
  
  private class SubMapRecordWriter extends RecordWriter<K2,V2> {
    
    @Override
    public void close(TaskAttemptContext context) throws IOException,
        InterruptedException {}
    
    @Override
    public void write(K2 key, V2 value) throws IOException,
        InterruptedException {
      synchronized (outer) {
        outer.write(key, value);
      }
    }
  }
  
  private class SubMapStatusReporter extends StatusReporter {
    
    @Override
    public Counter getCounter(Enum<?> name) {
      return outer.getCounter(name);
    }
    
    @Override
    public Counter getCounter(String group, String name) {
      return outer.getCounter(group, name);
    }
    
    @Override
    public void progress() {
      outer.progress();
    }
    
    @Override
    public void setStatus(String status) {
      outer.setStatus(status);
    }
    
  }
  
  private class MapRunner extends Thread {
    public BlockMapper<K1,V1,K2,V2> mapper;
    private Mapper<K1,V1,K2,V2>.Context subcontext;
    private Throwable throwable;
    
    MapRunner(Context context) throws IOException, InterruptedException {
      mapper = ReflectionUtils
          .newInstance(mapClass, context.getConfiguration());
      MapContext<K1,V1,K2,V2> mapContext = new MapContextImpl<K1,V1,K2,V2>(context.getConfiguration(), context.getTaskAttemptID(), new SubMapRecordReader(), new SubMapRecordWriter(), context.getOutputCommitter(), new SubMapStatusReporter(), context.getInputSplit());
      subcontext = new WrappedMapper<K1,V1,K2,V2>().getMapContext(mapContext);
//      subcontext = new Context(outer.getConfiguration(),
//          outer.getTaskAttemptID(), new SubMapRecordReader(),
//          new SubMapRecordWriter(), context.getOutputCommitter(),
//          new SubMapStatusReporter(), outer.getInputSplit());
      
    }
    
    public Throwable getThrowable() {
      return throwable;
    }
    
    @Override
    public void run() {
      try {
        mapper.run(subcontext);
      } catch (Throwable ie) {
        throwable = ie;
      }
    }
    
  }
  
  public static abstract class BlockMapper<KEYIN,VALUEIN,KEYOUT,VALUEOUT>
      extends Mapper<KEYIN,VALUEIN,KEYOUT,VALUEOUT> {
    
    private long lasttime = System.currentTimeMillis();
    public KEYIN currentKey;
    public VALUEIN currentValue;
    public boolean isBlock = false;
    public Context context;
    public long bytes; // total bytes fetched
    public int pages; // total pages fetched
    public int errors; // total pages errored
    
    @Override
    protected void setup(Context context) throws IOException,
        InterruptedException {
      this.context = context;
    };
    
    @Override
    public void run(Context context) throws IOException, InterruptedException {
      setup(context);
      while (context.nextKeyValue() && !isBlock) {
        lasttime = System.currentTimeMillis();
        currentKey = context.getCurrentKey();
        currentValue = context.getCurrentValue();
        map(currentKey, currentValue, context);
      }
      cleanup(context);
    }
    
    public long getBlockTime() {
      return System.currentTimeMillis() - lasttime;
    }
    
    @Override
    public void cleanup(Context context) throws IOException,
        InterruptedException {
      super.cleanup(context);
    }
    
    public abstract void BlockRecord() throws InterruptedException;
    
  }
  
}
