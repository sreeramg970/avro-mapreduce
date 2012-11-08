package com.topsoft.avro.mapreduce.output;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl;
import org.apache.hadoop.util.ReflectionUtils;

@SuppressWarnings({"rawtypes", "unchecked"})
public class MultipleOutputs {
  
  /**
   * Cache for the taskContexts
   */
  private Map<String,TaskAttemptContext> taskContexts = new HashMap<String,TaskAttemptContext>();
  
  private TaskAttemptContext context;
  private Map<String,NameOutput> namedOutputs;
  
  private Map<String,RecordWriter<?,?>> recordWriters;
  
  /**
   * Checks if a named output name is valid token.
   * 
   * @param namedOutput
   *          named output Name
   * @throws IllegalArgumentException
   *           if the output name is not valid.
   */
  private static void checkTokenName(String namedOutput) {
    if (namedOutput == null || namedOutput.length() == 0) {
      throw new IllegalArgumentException("Name cannot be NULL or emtpy");
    }
//    for (char ch : namedOutput.toCharArray()) {
//      if ((ch >= 'A') && (ch <= 'Z')) {
//        continue;
//      }
//      if ((ch >= 'a') && (ch <= 'z')) {
//        continue;
//      }
//      if ((ch >= '0') && (ch <= '9')) {
//        continue;
//      }
//      throw new IllegalArgumentException("Name cannot be have a '" + ch
//          + "' char");
//    }
  }
  
  /**
   * Checks if a named output name is valid.
   * 
   * @param namedOutput
   *          named output Name
   * @throws IllegalArgumentException
   *           if the output name is not valid.
   */
  private void checkNamedOutputName(String namedOutput, boolean alreadyDefined) {
    checkTokenName(namedOutput);
    if (alreadyDefined && namedOutputs.containsKey(namedOutput)) {
      throw new IllegalArgumentException("Named output '" + namedOutput
          + "' already alreadyDefined");
    } else if (!alreadyDefined && !namedOutputs.containsKey(namedOutput)) {
      throw new IllegalArgumentException("Named output '" + namedOutput
          + "' not defined");
    }
  }
  
  /**
   * Adds a named output for the job.
   * <p/>
   * 
   * @param namedOutput
   *          named output name, it has to be a word, letters and numbers only,
   *          cannot be the word 'part' as that is reserved for the default
   *          output.
   * @param outputFormatClass
   *          OutputFormat class.
   * @param keyClass
   *          key class
   * @param valueClass
   *          value class
   */
  public void addNamedOutput(String namedOutput,
      Class<? extends ExtFileOutputFormat> outputFormatClass,
      Class<?> keyClass, Class<?> valueClass) {
    try {
      checkNamedOutputName(namedOutput, true);
      
      namedOutputs.put(namedOutput, new NameOutput(outputFormatClass, keyClass,
          valueClass));
    } catch (Exception e) {}
  }
  
  // instance code, to be used from Mapper/Reducer code
  
  private class NameOutput {
    public NameOutput(Class<? extends ExtFileOutputFormat> outputFormatClass,
        Class<?> keyClass, Class<?> valueClass) {
      this.outputFormatClass = outputFormatClass;
      this.keyClass = keyClass;
      this.valueClass = valueClass;
    }
    
    Class<? extends ExtFileOutputFormat> outputFormatClass;
    Class<?> keyClass;
    Class<?> valueClass;
  }
  
  public MultipleOutputs(TaskAttemptContext context) {
    this.context = context;
    namedOutputs = new HashMap<String,NameOutput>();
    recordWriters = new HashMap<String,RecordWriter<?,?>>();
  }
  
  public <K,V> void write(String namedOutput, K key, V value)
      throws IOException, InterruptedException {
    write(namedOutput, key, value, namedOutput);
  }
  
  public <K,V> void write(
      Class<? extends ExtFileOutputFormat> outputFormatClass, K key, V value)
      throws IOException, InterruptedException {
    write(outputFormatClass,value.getClass().getSimpleName(),key,value);
//    addNamedOutput(value.getClass().getSimpleName(), outputFormatClass,
//        key.getClass(), value.getClass());
//    write(value.getClass().getSimpleName(), key, value, value.getClass()
//        .getSimpleName() + "/");
  }
  
  public <K,V> void write(
      Class<? extends ExtFileOutputFormat> outputFormatClass, String nameOutput,K key, V value)
      throws IOException, InterruptedException {
    addNamedOutput(nameOutput, outputFormatClass,
        key.getClass(), value.getClass());
    write(nameOutput, key, value, nameOutput + "/");
  }
  
  public <K,V> void write(String namedOutput, K key, V value,
      String baseOutputPath) throws IOException, InterruptedException {
    checkNamedOutputName(namedOutput, false);
    TaskAttemptContext taskContext = getContext(namedOutput);
    getRecordWriter(taskContext, baseOutputPath).write(key, value);
  }
  
  private synchronized RecordWriter getRecordWriter(
      TaskAttemptContext taskContext, String baseFileName) throws IOException,
      InterruptedException {
    
    // look for record-writer in the cache
    RecordWriter writer = recordWriters.get(baseFileName);
    
    // If not in cache, create a new one
    if (writer == null) {
      // get the record writer from context output format
      
      ExtFileOutputFormat.setOutputName(taskContext, baseFileName);
      try {
        writer = ((OutputFormat) ReflectionUtils.newInstance(
            taskContext.getOutputFormatClass(), taskContext.getConfiguration()))
            .getRecordWriter(taskContext);
      } catch (ClassNotFoundException e) {
        throw new IOException(e);
      }
      
      // add the record-writer to the cache
      recordWriters.put(baseFileName, writer);
    }
    return writer;
  }
  
  private TaskAttemptContext getContext(String nameOutput) throws IOException {
    
    TaskAttemptContext taskContext = taskContexts.get(nameOutput);
    
    if (taskContext != null) {
      return taskContext;
    }
    NameOutput out = namedOutputs.get(nameOutput);
    Job job = new Job(context.getConfiguration());
    job.setOutputFormatClass(out.outputFormatClass);
    job.setOutputKeyClass(out.keyClass);
    job.setOutputValueClass(out.valueClass);
    taskContext = new TaskAttemptContextImpl(job.getConfiguration(),
        context.getTaskAttemptID());
    
    taskContexts.put(nameOutput, taskContext);
    
    return taskContext;
  }
  
  public void close() throws IOException, InterruptedException {
    for (RecordWriter writer : recordWriters.values()) {
      writer.close(context);
    }
  }
}
