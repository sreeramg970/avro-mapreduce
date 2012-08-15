package com.topsoft.botspider.util;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.io.JsonEncoder;
import org.apache.avro.reflect.ReflectData;
import org.apache.avro.reflect.ReflectDatumReader;
import org.apache.avro.reflect.ReflectDatumWriter;

public class AvroUtils {
  static ByteArrayOutputStream out = new ByteArrayOutputStream();
  //static ByteArrayOutputStream arrout = new ByteArrayOutputStream();
  public static <V> V clone(V v) throws IOException {
    out.reset();
    BinaryEncoder encoder = EncoderFactory.get().directBinaryEncoder(out, null);
    Schema schema;
    if (IndexedRecord.class.isAssignableFrom(v.getClass())) schema = ((IndexedRecord) v)
        .getSchema();
    else schema = ReflectData.get().getSchema(v.getClass());
    
    GenericDatumWriter<V> writer = new ReflectDatumWriter<V>(schema);
    GenericDatumReader<V> reader = new ReflectDatumReader<V>(schema);
    writer.write(v, encoder);
    ByteArrayInputStream in = new ByteArrayInputStream(out.toByteArray());
    BinaryDecoder decoder = DecoderFactory.get().directBinaryDecoder(in, null);
    return reader.read(null, decoder);
    
  }
  
  public static <V> byte[] serialize(V v) throws IOException
  {
    out.reset();
    BinaryEncoder encoder = EncoderFactory.get().directBinaryEncoder(out, null);
    Schema schema;
    if (IndexedRecord.class.isAssignableFrom(v.getClass())) schema = ((IndexedRecord) v)
        .getSchema();
    else schema = ReflectData.get().getSchema(v.getClass());
    
    GenericDatumWriter<V> writer = new ReflectDatumWriter<V>(schema);
    writer.write(v, encoder);
    return out.toByteArray();
  }
  
  public static <V> String toString(V v) {
    StringBuilder buffer = new StringBuilder();
    Schema schema;
    if (IndexedRecord.class.isAssignableFrom(v.getClass())) schema = ((IndexedRecord) v)
        .getSchema();
    else schema = ReflectData.get().getSchema(v.getClass());
    toString(v, buffer, schema);
    
    return buffer.toString();
  }
  
  
  
  public static <V> String toAvroString(V v) {
    if (v == null) return null;
    // ByteArrayOutputStream out = new ByteArrayOutputStream();
    out.reset();
    Schema schema;
    if (IndexedRecord.class.isAssignableFrom(v.getClass())) schema = ((IndexedRecord) v)
        .getSchema();
    else schema = ReflectData.get().getSchema(v.getClass());
    if (schema == null) {
      return "null shcema can not be toString!";
    }
    JsonEncoder encoder;
    try {
      encoder = EncoderFactory.get().jsonEncoder(schema, out);
      
      GenericDatumWriter<V> writer = new ReflectDatumWriter<V>(schema);
      // encoder.init(out);
      writer.write(v, encoder);
      encoder.flush();
      // out.flush();
      return out.toString("utf-8");
    } catch (IOException e) {
      e.printStackTrace();
      return e.toString();
    }
    
  }
  
  protected static void toString(Object datum, StringBuilder buffer,
      Schema schema) {
    if (datum == null) {
      buffer.append("{null}");
      return;
    }
    if (schema.getType() == Schema.Type.RECORD) {
      buffer.append("{");
      int count = 0;
      
      for (Field f : schema.getFields()) {
        toString(f.name(), buffer, Schema.create(Schema.Type.STRING));
        buffer.append(": ");
        toString(ReflectData.get().getField(datum, f.name(), count), buffer,
            f.schema());
        // toString(record.get(f.pos()), buffer);
        if (++count < schema.getFields().size()) buffer.append(", ");
      }
      buffer.append("}");
    } else if (schema.getType() == Schema.Type.ARRAY) {
      
      if (datum instanceof Collection) {
        Collection<?> array = (Collection<?>) datum;
        buffer.append("[");
        long last = array.size() - 1;
        int i = 0;
        for (Object element : array) {
          toString(element, buffer, schema.getElementType());
          if (i++ < last) buffer.append(", ");
        }
        buffer.append("]");
      } else if (datum != null && datum.getClass().isArray()) {
        buffer.append("[");
        long last = ((Object[]) datum).length - 1;
        int i = 0;
        for (Object element : ((Object[]) datum)) {
          toString(element, buffer, schema.getElementType());
          if (i++ < last) buffer.append(", ");
        }
        buffer.append("]");
      }
    } else if (schema.getType() == Schema.Type.MAP) {
      buffer.append("{");
      int count = 0;
      @SuppressWarnings(value = "unchecked")
      Map<Object,Object> map = (Map<Object,Object>) datum;
      for (Map.Entry<Object,Object> entry : map.entrySet()) {
        toString(entry.getKey(), buffer, schema.getValueType());
        buffer.append(": ");
        toString(entry.getValue(), buffer, schema.getValueType());
        if (++count < map.size()) buffer.append(", ");
      }
      buffer.append("}");
    } else if (schema.getType() == Schema.Type.STRING) {
      buffer.append("\"");
      buffer.append(datum); // TODO: properly escape!
      buffer.append("\"");
    } else if (schema.getType() == Schema.Type.FIXED
        || schema.getType() == Schema.Type.BYTES) {
      if (datum instanceof ByteBuffer) {
        buffer.append("{\"bytes\": \"");
        ByteBuffer bytes = (ByteBuffer) datum;
        for (int i = bytes.position(); i < bytes.limit(); i++)
          buffer.append((char) bytes.get(i));
        buffer.append("\"}");
      } else buffer.append(datum);
    } else if (schema.getType() == Schema.Type.UNION) {
      int index = GenericData.get().resolveUnion(schema, datum);
      toString(datum, buffer, schema.getTypes().get(index));
    } else {
      buffer.append(datum);
    }
  }
  
  public static void main(String[] args) throws Exception {
    HashMap<String,Integer> hm = new HashMap<String,Integer>();
    hm.put("1", 1);
    hm.put("2", 2);
    HashMap<String,Integer> tmp = AvroUtils.clone(hm);
    
    System.out.println(tmp);
  }
}
