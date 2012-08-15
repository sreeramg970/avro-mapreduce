package com.topsoft.botspider.util;
/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


import java.io.InputStream;
import java.io.PrintStream;
import java.util.List;

import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.io.DatumReader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.topsoft.botspider.avro.mapreduce.output.AvroMapOutputFormat;
import com.topsoft.botspider.io.MapAvroFile;

/** Reads a data file to get its schema. */
public class MapAvroReadTool implements Tool {
  
  @Override
  public String getName() {
    return "map";
  }
  
  @Override
  public String getShortDescription() {
    return "Get a record from Map Avro data file by key or get total record number.";
  }
  
  @SuppressWarnings({"rawtypes", "unchecked"})
  @Override
  public int run(InputStream stdin, PrintStream out, PrintStream err,
      List<String> args) throws Exception {
    if (args.size() < 1) {
      err.println("Usage: input_file [-key key] [-count]");
      return 1;
    }
    
    String key = null;
    boolean bcount = false;
    
    for (int i = 0; i < args.size(); i++) {
      if ("-key".equals(args.get(i))) {
        key = args.get(i + 1);
        i++;
      } else if ("-count".equals(args.get(i))) {
        bcount = true;
      }
      
    }
    if (key == null) bcount = true;
    Configuration conf = new Configuration();
    conf.setClass(MapAvroFile.Reader.DATUM_READER_CLASS,
        GenericDatumReader.class, DatumReader.class);
    FileSystem fs = FileSystem.get(conf);
    Path input = new Path(args.get(0));
    Path datafile = new Path(input, MapAvroFile.DATA_FILE_NAME);
    Path indexfile = new Path(input, MapAvroFile.INDEX_FILE_NAME);
    MapAvroFile.Reader[] readers = null;
    if (fs.exists(datafile) && fs.exists(indexfile)) {
      // readers = new
      readers = new MapAvroFile.Reader[] {new MapAvroFile.Reader(fs,
          input.toString(), conf)};
    } else readers = AvroMapOutputFormat
        .getReaders(new Path(args.get(0)), conf);
    try {
      if (key != null) {
        Object obj = AvroMapOutputFormat.getEntry(readers, key);
        out.println(AvroUtils.toAvroString(obj));
      }
      if (bcount) {
        int total = 0;
        for (MapAvroFile.Reader reader : readers) {
          total += reader.size();
        }
        out.println("Total record : " + total);
      }
    } finally {
      for (int i = 0; i < readers.length; i++) {
        try {
          readers[i].close();
        } catch (Exception e) {

        }
      }
    }
    return 0;
  }
}
