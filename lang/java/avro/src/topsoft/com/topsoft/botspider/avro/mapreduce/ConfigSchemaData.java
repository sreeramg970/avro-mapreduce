package com.topsoft.botspider.avro.mapreduce;

import org.apache.avro.Schema;
import org.apache.hadoop.conf.Configuration;

public interface ConfigSchemaData {
  public Schema getSchema(Configuration conf);
}
