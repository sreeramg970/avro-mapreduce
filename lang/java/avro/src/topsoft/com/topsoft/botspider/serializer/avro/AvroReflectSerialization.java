package com.topsoft.botspider.serializer.avro;

import java.util.Set;

import org.apache.avro.AvroTypeException;
import org.apache.avro.Schema;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.reflect.ReflectData;
import org.apache.avro.reflect.ReflectDatumReader;
import org.apache.avro.reflect.ReflectDatumWriter;

import com.topsoft.botspider.avro.mapreduce.ConfigSchemaData;

@SuppressWarnings("unchecked")
public class AvroReflectSerialization extends AvroSerialization<Object> {
  
  private Set<String> packages;
  
  @Override
  public synchronized boolean accept(Class<?> c) {
    if (packages != null && packages.contains(c.getName())) return true;
    
    boolean bacept = false;
    try {
      Schema schema = getSchema(c);
      if (schema != null) {
        packages.add(c.getName());
        bacept = true;
      }
    } catch (AvroTypeException e) {
      e.printStackTrace();
      bacept = false;
    }
    return bacept;
    
  }
  
  @Override
  public DatumReader getReader(Class<Object> clazz) {
    try {
      return new ReflectDatumReader(getSchema(clazz));
      
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
  
  @Override
  public Schema getSchema(Object t) {
    if (ConfigSchemaData.class.isAssignableFrom(t.getClass())) return ((ConfigSchemaData) t)
        .getSchema(getConf());
    return ReflectData.get().getSchema(t.getClass());
  }
  
  public Schema getSchema(Class clazz) {
    if (ConfigSchemaData.class.isAssignableFrom(clazz)) try {
      return ((ConfigSchemaData) clazz.newInstance()).getSchema(getConf());
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    
    return ReflectData.get().getSchema(clazz);
  }
  
  @Override
  public DatumWriter getWriter(Class<Object> clazz) {
    return new ReflectDatumWriter();
  }
  
}
