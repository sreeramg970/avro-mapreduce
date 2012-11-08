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
package com.topsoft.util;

import java.io.InputStream;
import java.io.PrintStream;
import java.util.List;

import org.apache.avro.file.DataFileReader;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.topsoft.avro.mapreduce.FsInput;
import com.topsoft.io.MapAvroFile;

/** Reads a data file to get its schema. */
public class GetSchemaTool implements Tool {
  
  @Override
  public String getName() {
    return "getschema";
  }
  
  @Override
  public String getShortDescription() {
    return "Prints out schema of an Avro data file.";
  }
  
  @Override
  public int run(InputStream stdin, PrintStream out, PrintStream err,
      List<String> args) throws Exception {
    if (args.size() != 1) {
      err.println("Usage: input_file");
      return 1;
    }
    Configuration conf = new Configuration();
    FileSystem fs = FileSystem.get(conf);
    Path inpitFile = new Path(args.get(0));
    if (fs.isDirectory(inpitFile)) {
      inpitFile = new Path(inpitFile, MapAvroFile.DATA_FILE_NAME);
    }
    DataFileReader<Void> reader = new DataFileReader<Void>(new FsInput(
        inpitFile, conf), new GenericDatumReader<Void>());
    try {
      
      out.println(reader.getSchema().toString(true));
      
    } finally {
      reader.close();
    }
    return 0;
  }
}
