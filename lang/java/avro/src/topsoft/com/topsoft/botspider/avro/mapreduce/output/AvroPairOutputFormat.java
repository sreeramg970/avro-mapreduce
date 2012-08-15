package com.topsoft.botspider.avro.mapreduce.output;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;

import org.apache.avro.Schema;
import org.apache.avro.file.CodecFactory;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.reflect.ReflectData;
import org.apache.avro.reflect.ReflectDatumWriter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.topsoft.botspider.avro.mapreduce.FsInput;
import com.topsoft.botspider.io.MapAvroFile;
import com.topsoft.botspider.io.Pair;

public class AvroPairOutputFormat<K,V> extends ExtFileOutputFormat<K,V> {
  
  /** The file name extension for avro data files. */
  public final static String EXT = ".avro";
  
  /** The configuration key for Avro deflate level. */
  public static final String DEFLATE_LEVEL_KEY = "avro.mapred.deflate.level";
  
  /** Enable output compression using the deflate codec and specify its level. */
  public static void setDeflateLevel(Job job, int level) {
    FileOutputFormat.setCompressOutput(job, true);
    job.getConfiguration().setInt(DEFLATE_LEVEL_KEY, level);
  }
  
  @Override
  public RecordWriter<K,V> getRecordWriter(TaskAttemptContext job)
      throws IOException, InterruptedException {
    final DataFileWriter<Pair<K,V>> writer = new DataFileWriter<Pair<K,V>>(
        new ReflectDatumWriter<Pair<K,V>>());
    
    if (FileOutputFormat.getCompressOutput(job)) {
      int level = job.getConfiguration().getInt(DEFLATE_LEVEL_KEY,
          MapAvroFile.DEFAULT_DEFLATE_LEVEL);
      writer.setCodec(CodecFactory.deflateCodec(level));
      //writer.setCodec(CodecFactory.snappyCodec());
    }
    
    Path path = this.getDefaultWorkFile(job, EXT);
    final Schema keySchema = ReflectData.get().getSchema(
        job.getOutputKeyClass());
    final Schema valueSchema = ReflectData.get().getSchema(
        job.getOutputValueClass());
    writer.create(Pair.getPairSchema(keySchema, valueSchema), path
        .getFileSystem(job.getConfiguration()).create(path));
    
    return new RecordWriter<K,V>() {
      
      @Override
      public void write(K key, V value) throws IOException {
        writer.append(new Pair<K,V>(key, keySchema, value, valueSchema));
      }
      
      @Override
      public void close(TaskAttemptContext context) throws IOException,
          InterruptedException {
        writer.close();
      }
    };
  }
  
  @SuppressWarnings("unchecked")
  public static <K,V> DataFileReader<Pair<K,V>>[] getReaders(Path dir,
      Configuration conf) throws IOException {
    FileSystem fs = dir.getFileSystem(conf);
    Path[] names = FileUtil.stat2Paths(fs.listStatus(dir));
    
    // sort names, so that hash partitioning works
    Arrays.sort(names);
    
    ArrayList<DataFileReader<Pair<K,V>>> parts = new ArrayList<DataFileReader<Pair<K,V>>>();
    for (int i = 0; i < names.length; i++) {
      try {
        DataFileReader<Pair<K,V>> part = new DataFileReader<Pair<K,V>>(
            new FsInput(names[i], conf),
            MapAvroFile.Reader.getDatumReader(conf));
        
        parts.add(part);
      } catch (Exception e) {}
    }
    return parts.toArray(new DataFileReader[parts.size()]);
  }
}