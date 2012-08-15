/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.topsoft.botspider.io;

import java.util.ArrayList;
import java.util.List;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.reflect.ReflectData;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;

import com.topsoft.botspider.avro.mapreduce.ConfigSchemaData;

public class UnionData implements ConfigSchemaData {
  public static final Log LOG = LogFactory.getLog(UnionData.class);
  public final static String UNION_CLASS = "union.class";
  public Object datum;

  public UnionData(Object datum) {
    this.datum = datum;
  }
  
  public UnionData() {}
  
  public static void setParseClass(Configuration conf, Class... classez) {
    if (conf == null || classez == null) return;
    ArrayList<String> arrParse = new ArrayList<String>(classez.length);
    for (Class clzss : classez) {
      arrParse.add(clzss.getName());
    }
    conf.setStrings(UNION_CLASS, arrParse.toArray(new String[arrParse.size()]));
  }
  
  public static void setParseClass(Job job, Class[] parseClass) {
    if (job == null || parseClass == null) return;
    ArrayList<String> arrParse = new ArrayList<String>(parseClass.length);
    for (Class clzss : parseClass) {
      arrParse.add(clzss.getName());
    }
    job.getConfiguration().setStrings(UNION_CLASS,
        arrParse.toArray(new String[arrParse.size()]));
  }
  
  @Override
  @SuppressWarnings("rawtypes")
  public Schema getSchema(Configuration conf) {
    
    Class[] parseClass = null;
    if (conf != null) {
      parseClass = conf.getClasses(UNION_CLASS, null);
    }
    
    List<Schema> branches = new ArrayList<Schema>();
    branches.add(Schema.create(Schema.Type.NULL));
    if (parseClass != null) {
      for (Class branch : parseClass) {
        branches.add(ReflectData.get().getSchema(branch));
      }
      
    }
    Schema field = Schema.createUnion(branches);
    Schema schema = Schema.createRecord(UnionData.class.getName(), null, null,
        false);
    ArrayList<Field> fields = new ArrayList<Field>();
    fields.add(new Field("datum", field, null, null));
    schema.setFields(fields);
    return schema;
  }
  
  public static void main(String[] args) throws Exception {
    UnionData sd = new UnionData();
    System.out.println(sd.getSchema(null));
  }
}
