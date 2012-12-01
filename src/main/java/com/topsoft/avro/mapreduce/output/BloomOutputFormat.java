package com.topsoft.avro.mapreduce.output;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;
import org.apache.hadoop.util.bloom.DynamicBloomFilter;
import org.apache.hadoop.util.bloom.Filter;
import org.apache.hadoop.util.bloom.Key;
import org.apache.hadoop.util.hash.Hash;

import com.topsoft.util.AvroUtils;

public class BloomOutputFormat extends ExtFileOutputFormat<Object,NullWritable> {
  public static final int HASH_COUNT = 5;
  public static final String BLOOM_FILE_NAME = "bloom";
  private int numKeys;
  private int vectorSize;
  private Key bloomKey = new Key(); 
  private DynamicBloomFilter bloomFilter;
  Path bloom;
  
  @Override
  public RecordWriter<Object,NullWritable> getRecordWriter(
      TaskAttemptContext job) throws IOException, InterruptedException {
    numKeys = job.getConfiguration().getInt("io.mapfile.bloom.size",
        1024 * 1024);
    // vector size should be <code>-kn / (ln(1 - c^(1/k)))</code> bits for
    // single key, where <code> is the number of hash functions,
    // <code>n</code> is the number of keys and <code>c</code> is the desired
    // max. error rate.
    // Our desired error rate is by default 0.005, i.e. 0.5%
    float errorRate = job.getConfiguration().getFloat(
        "io.mapfile.bloom.error.rate", 0.005f);
    vectorSize = (int) Math.ceil((double) (-HASH_COUNT * numKeys)
        / Math.log(1.0 - Math.pow(errorRate, 1.0 / HASH_COUNT)));
    bloomFilter = new DynamicBloomFilter(vectorSize, HASH_COUNT,
        Hash.getHashType(job.getConfiguration()), numKeys);
    bloom = this.getDefaultWorkFile(job, BLOOM_FILE_NAME);
    return new RecordWriter<Object,NullWritable>() {
      
      @Override
      public void write(Object key, NullWritable value) throws IOException,
          InterruptedException {
        DataOutputBuffer out = AvroUtils.serialize(key);
        byte[] bytes = new byte[out.getLength()];
        for(int i=0;i<out.getLength();i++)
          bytes[i] = out.getData()[i];
        bloomKey.set(bytes, 1.0);
        bloomFilter.add(bloomKey);
      }
      
      @Override
      public void close(TaskAttemptContext context) throws IOException,
          InterruptedException {
        
        FileSystem fs = FileSystem.get(context.getConfiguration());
        DataOutputStream out = fs.create(bloom, true);
        bloomFilter.write(out);
        out.flush();
        out.close();
      }
    };
  }
  
  public static Filter[] getFilters(Path input, Configuration conf)
      throws IOException {
    Filter[] filters = null;
    FileSystem fs = FileSystem.get(conf);
    if (fs.isDirectory(input)) {
      Path[] names = FileUtil.stat2Paths(fs.listStatus(input,
          getPassJobFileFilter(fs)));
      // sort names, so that hash partitioning works
      Arrays.sort(names);
      filters = new Filter[names.length];
      
      for (int i = 0; i < names.length; i++) {
        try {
          DataInputStream in = fs.open(names[i]);
          DynamicBloomFilter bloomFilter = new DynamicBloomFilter();
          bloomFilter.readFields(in);
          in.close();
          filters[i] = bloomFilter;
        } catch (IOException ioe) {
          // System.out.println("Can't open file: " + names[i] +
          // " BloomFilter: "
          // + ioe + ".");
          // bloomFilter = null;
          throw ioe;
        }
      }
      
    } else {
      try {
        filters = new Filter[1];
        DataInputStream in = fs.open(input);
        DynamicBloomFilter bloomFilter = new DynamicBloomFilter();
        bloomFilter.readFields(in);
        in.close();
        filters[0] = bloomFilter;
      } catch (IOException ioe) {
        // System.out.println("Can't open file: " + input + " BloomFilter: " +
        // ioe
        // + ".");
        // bloomFilter = null;
        throw ioe;
      }
    }
    return filters;
  }
  
  static Key bloomkey = new Key();
  
  @SuppressWarnings({"rawtypes", "unchecked"})
  public static <K> boolean filter(Filter[] filters, K key) throws IOException {
    int part = new HashPartitioner().getPartition(key, null, filters.length);
    DataOutputBuffer out = AvroUtils.serialize(key);
    byte[] bytes = new byte[out.getLength()];
    for(int i=0;i<out.getLength();i++)
      bytes[i] = out.getData()[i];
    bloomkey.set(bytes, 1.0);
    return filters[part].membershipTest(bloomkey);
    
  }
  
  public static PathFilter getPassJobFileFilter(final FileSystem fs) {
    return new PathFilter() {
      public boolean accept(final Path path) {
        try {
          if (fs.getFileStatus(path).isDirectory()) return false;
          if (path.getName().startsWith("_")) return false;
          return true;
        } catch (IOException ioe) {
          return false;
        }
      }
      
    };
  }
  
}
