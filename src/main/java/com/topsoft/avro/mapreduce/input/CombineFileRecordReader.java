package com.topsoft.avro.mapreduce.input;

import java.io.IOException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

/**
 * A generic RecordReader that can hand out different recordReaders for each
 * chunk in a {@link CombineFileSplit}. A CombineFileSplit can combine data
 * chunks from multiple files. This class allows using different RecordReaders
 * for processing these data chunks from different files.
 * 
 * @see CombineFileSplit
 */

public class CombineFileRecordReader<K,V> extends RecordReader<K,V> {
  
  protected CombineFileSplit split;
  protected Class<RecordReader<K,V>> rrClass;
  protected FileSystem fs;
  
  protected int idx;
  protected long progress;
  protected RecordReader<K,V> curReader;
  TaskAttemptContext job;
  
  /**
   * return the amount of data processed
   */
  public long getPos() throws IOException {
    return progress;
  }
  
  public void close() throws IOException {
    if (curReader != null) {
      curReader.close();
      curReader = null;
    }
  }
  
  /**
   * return progress based on the amount of data processed so far.
   */
  public float getProgress() throws IOException {
    return Math.min(1.0f, progress / (float) (split.getLength()));
  }
  
  /**
   * A generic RecordReader that can hand out different recordReaders for each
   * chunk in the CombineFileSplit.
   */
  public CombineFileRecordReader(TaskAttemptContext job,
      CombineFileSplit split, Class<RecordReader<K,V>> rrClass)
      throws IOException {
    this.split = split;
    this.job = job;
    this.rrClass = rrClass;
    this.idx = 0;
    this.curReader = null;
    this.progress = 0;
    initNextRecordReader();
  }
  
  /**
   * Get the record reader for the next chunk in this CombineFileSplit.
   */
  protected boolean initNextRecordReader() throws IOException {
    
    if (curReader != null) {
      curReader.close();
      curReader = null;
      if (idx > 0) {
        progress += split.getLength(idx - 1); // done processing so far
      }
    }
    
    // if all chunks have been processed, nothing more to do.
    if (idx == split.getNumPaths()) {
      return false;
    }
    
    // get a record reader for the idx-th chunk
    try {
      curReader = rrClass.newInstance();
      FileSplit fsp = makeSplit(split.getPath(idx), split.getOffset(idx),
          split.getLength(idx), new String[0]);
      curReader.initialize(fsp, job);
      // setup some helper config variables.
      job.getConfiguration().set("map.input.file",
          split.getPath(idx).toString());
      job.getConfiguration().setLong("map.input.start", split.getOffset(idx));
      job.getConfiguration().setLong("map.input.length", split.getLength(idx));
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    idx++;
    return true;
  }
  
  /**
   * A factory that makes the split for this class. It can be overridden by
   * sub-classes to make sub-types
   */
  protected FileSplit makeSplit(Path file, long start, long length,
      String[] hosts) {
    return new FileSplit(file, start, length, hosts);
  }
  
  @Override
  public void initialize(InputSplit split, TaskAttemptContext context)
      throws IOException, InterruptedException {
    // TODO Auto-generated method stub
    
  }
  
  @Override
  public boolean nextKeyValue() throws IOException, InterruptedException {
    while ((curReader == null) || !curReader.nextKeyValue()) {
      if (!initNextRecordReader()) {
        return false;
      }
    }
    return true;
  }
  
  @Override
  public K getCurrentKey() throws IOException, InterruptedException {
    // TODO Auto-generated method stub
    return curReader.getCurrentKey();
  }
  
  @Override
  public V getCurrentValue() throws IOException, InterruptedException {
    // TODO Auto-generated method stub
    return curReader.getCurrentValue();
  }
}
