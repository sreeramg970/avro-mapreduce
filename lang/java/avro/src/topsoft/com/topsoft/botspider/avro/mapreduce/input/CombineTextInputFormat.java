package com.topsoft.botspider.avro.mapreduce.input;

import java.io.IOException;

import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;

/** An {@link org.apache.hadoop.mapred.InputFormat} for sequence files. */
public class CombineTextInputFormat<LongWritable,Text> extends
    CombineFileInputFormat<LongWritable,Text> {
  
  @SuppressWarnings({"unchecked", "rawtypes"})
  @Override
  public RecordReader<LongWritable,Text> createRecordReader(
      CombineFileSplit split, TaskAttemptContext context) throws IOException,
      InterruptedException {
    return new CombineFileRecordReader<LongWritable,Text>(context, split,
        (Class) LineRecordReader.class);
  }
  
}
