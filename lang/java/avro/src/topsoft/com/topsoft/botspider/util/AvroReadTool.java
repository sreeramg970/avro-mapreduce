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

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.FileReader;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.topsoft.botspider.avro.mapreduce.FsInput;
import com.topsoft.botspider.io.MapAvroFile;

/** Reads a data file and dumps to JSON */
public class AvroReadTool implements Tool {
  
  @Override
  public String getName() {
    return "tojson";
  }
  
  @Override
  public String getShortDescription() {
    return "Dumps an Avro data file as JSON, one record per line.";
  }
  
  @Override
  public int run(InputStream stdin, PrintStream out, PrintStream err,
      List<String> args) throws Exception {
    if (args.size() < 1) {
      // Unlike other commands, "-" can't be used for stdin, because
      // we can only use seekable files.
      err.println("Usage: input_file [-num numToRead].");
      return 1;
    }
    int numToRead = Integer.MIN_VALUE;
    
    for (int i = 0; i < args.size(); i++) {
      if ("-num".equals(args.get(i))) {
        numToRead = Integer.parseInt(args.get(i + 1));
        i++;
      }
    }
    Configuration conf = new Configuration();
    FileSystem fs = FileSystem.get(conf);
    Path inputFile = new Path(args.get(0));
    if (fs.isDirectory(inputFile)) {
      inputFile = new Path(inputFile, MapAvroFile.DATA_FILE_NAME);
    }
    
    GenericDatumReader<Object> reader = new GenericDatumReader<Object>();
    FileReader<Object> fileReader = new DataFileReader<Object>(new FsInput(
        inputFile, conf), reader);
    try {
      Schema schema = fileReader.getSchema();
      DatumWriter<Object> writer = new GenericDatumWriter<Object>(schema);
      Encoder encoder = EncoderFactory.get().jsonEncoder(schema, out);
      for (Object datum : fileReader) {
        if (Integer.MIN_VALUE == numToRead || numToRead-- > 0) {
          //encoder.init(out);
          writer.write(datum, encoder);
          encoder.flush();
          //out.println();
        } else break;
      }
      out.flush();
    } finally {
      fileReader.close();
    }
    return 0;
  }
  
}
