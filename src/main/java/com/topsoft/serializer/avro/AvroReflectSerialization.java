package com.topsoft.serializer.avro;

import java.util.Set;

import org.apache.avro.AvroTypeException;
import org.apache.avro.Schema;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.reflect.ReflectData;
import org.apache.avro.reflect.ReflectDatumReader;
import org.apache.avro.reflect.ReflectDatumWriter;

import com.topsoft.avro.mapreduce.ConfigSchemaData;

@SuppressWarnings("unchecked")
public class AvroReflectSerialization extends AvroSerialization<Object> {
  
  // @InterfaceAudience.Private
  // public static final String AVRO_REFLECT_PACKAGES = "avro.reflect.pkgs";
  //
   private Set<String> packages;
  
  @Override
  public synchronized boolean accept(Class<?> c) {
    // if (packages == null) {
    // getPackages();
    // }
    boolean bacept = false;
    try {
      Schema schema = getSchema(c);
      if (schema != null) bacept = true;
    } catch (AvroTypeException e) {
      e.printStackTrace();
      bacept = false;
    }
    return bacept;
    // if(bacept)
    // return true;
    // return AvroReflectSerializable.class.isAssignableFrom(c)
    // || packages.contains(c.getPackage().getName());
  }
  
  // private void getPackages() {
  // String[] pkgList = getConf().getStrings(AVRO_REFLECT_PACKAGES);
  // packages = new HashSet<String>();
  // if (pkgList != null) {
  // for (String pkg : pkgList) {
  // packages.add(pkg.trim());
  // }
  // }
  // }
  
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
