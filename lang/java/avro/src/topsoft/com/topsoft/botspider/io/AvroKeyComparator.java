package com.topsoft.botspider.io;

import org.apache.avro.Schema;
import org.apache.avro.io.BinaryData;
import org.apache.avro.reflect.ReflectData;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.mapred.JobConf;

import com.topsoft.botspider.avro.mapreduce.AvroJob;

public class AvroKeyComparator<T> extends Configured implements
    RawComparator<T> {
  
  private Schema schema;
  
  @Override
  public void setConf(Configuration conf) {
    super.setConf(conf);
    if (conf != null) {
      
      try {
        Schema inputschema = AvroJob.getMapOutputSchema(conf);
        if (inputschema != null) schema = Pair.getKeySchema(AvroJob
            .getMapOutputSchema(conf));
      } catch (Exception e) {
        e.printStackTrace();
        schema = null;
      }
      if (schema == null) {
        schema = ReflectData.get().getSchema(
            new JobConf(conf).getMapOutputKeyClass());
        
      }
    }
    
  }
  
  public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
    return BinaryData.compare(b1, s1, b2, s2, schema);
  }
  
  public int compare(T x, T y) {
    //return SpecificData.get().compare(x, y, schema);
    return ReflectData.get().compare(x, y, schema);
  }
  
  public void setSchema(Schema keySchema) {
    this.schema = keySchema;
    
  }
  
}
