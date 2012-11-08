package com.topsoft.io;

import java.io.EOFException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Map;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Type;
import org.apache.avro.file.CodecFactory;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericContainer;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.reflect.ReflectData;
import org.apache.avro.reflect.ReflectDatumReader;
import org.apache.avro.reflect.ReflectDatumWriter;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.util.Progressable;

import com.topsoft.avro.mapreduce.FsInput;
import com.topsoft.util.AvroUtils;

/**
 * A file-based map from keys to values.
 * 
 * <p>
 * A map is a directory containing two files, the <code>data</code> file,
 * containing all keys and values in the map, and a smaller <code>index</code>
 * file, containing a fraction of the keys. The fraction is determined by
 * {@link Writer#getIndexInterval()}.
 * 
 * <p>
 * The index file is read entirely into memory. Thus key implementations should
 * try to keep themselves small.
 * 
 * <p>
 * Map files are created by adding entries in-order. To maintain a large
 * database, perform updates by copying the previous version of a database and
 * merging in a sorted change list, to create a new version of the database in a
 * new file.
 */

public class MapAvroFile {
  private static final Log LOG = LogFactory.getLog(MapAvroFile.class);
  
  /** The name of the index file. */
  public static final String INDEX_FILE_NAME = "index.avro";
  
  /** The name of the data file. */
  public static final String DATA_FILE_NAME = "data.avro";
  
  public static final String DEFLATE_LEVEL_KEY = "avro.mapred.deflate.level";
  /** The default deflate level. */
  public static final int DEFAULT_DEFLATE_LEVEL = 8;
  
  protected MapAvroFile() {} // no public ctor
  
  /** Writes a new map. */
  public static class Writer<K,V> implements java.io.Closeable {
    private DataFileWriter<Pair<K,V>> data;
    private DataFileWriter<Pair<K,Long>> index;
    
    final public static String INDEX_INTERVAL = "io.map.index.interval";
    private int indexInterval = 128;
    
    private long size;
    
    // the following fields are used only for checking key order
    private AvroKeyComparator<K> comparator;
    private DataInputBuffer inBuf = new DataInputBuffer();
    private DataOutputBuffer outBuf = new DataOutputBuffer();
    private K lastKey;
    
    private Schema keySchema;
    private Schema valueSchema;
    
    /** What's the position (in bytes) we wrote when we got the last index */
    private long lastIndexPos = -1;
    
    /**
     * What was size when we last wrote an index. Set to MIN_VALUE to ensure
     * that we have an index at position zero -- midKey will throw an exception
     * if this is not the case
     */
    private long lastIndexKeyCount = Long.MIN_VALUE;
    
    public Writer(Configuration conf, FileSystem fs, String dirName,
        Schema keySchema, Schema valueSchema) throws IOException {
      this(conf, fs, dirName, keySchema, valueSchema, conf.getInt(
          INDEX_INTERVAL, 128), null, DEFAULT_DEFLATE_LEVEL);
      
    }
    
    public Writer(Configuration conf, FileSystem fs, String dirName,
        Schema keySchema, Schema valueSchema, Progressable progress,
        int deflateLevel) throws IOException {
      this(conf, fs, dirName, keySchema, valueSchema, conf.getInt(
          INDEX_INTERVAL, 128), progress, deflateLevel);
      
    }
    
    public Writer(Configuration conf, FileSystem fs, String dirName,
        Schema keySchema, Schema valueSchema, int interval,
        @SuppressWarnings("deprecation") Progressable progress)
        throws IOException {
      this(conf, fs, dirName, keySchema, valueSchema, interval, progress,
          DEFAULT_DEFLATE_LEVEL);
      
    }
    
    public Writer(Configuration conf, FileSystem fs, String dirName,
        Schema keySchema, Schema valueSchema, Progressable progress)
        throws IOException {
      this(conf, fs, dirName, keySchema, valueSchema, conf.getInt(
          INDEX_INTERVAL, 128), progress, DEFAULT_DEFLATE_LEVEL);
    }
    
    public Writer(Configuration conf, FileSystem fs, String dirName,
        Schema keySchema, Schema valueSchema, int interval,
        Progressable progress, int deflateLevel) throws IOException {
      this.keySchema = keySchema;
      this.valueSchema = valueSchema;
      comparator = new AvroKeyComparator<K>();
      comparator.setSchema(keySchema);
      this.indexInterval = interval;
      Path dir = new Path(dirName);
      if (!fs.mkdirs(dir)) {
        throw new IOException("Mkdirs failed to create directory "
            + dir.toString());
      }
      Path dataFile = new Path(dir, DATA_FILE_NAME);
      Path indexFile = new Path(dir, INDEX_FILE_NAME);
      
      data = new DataFileWriter<Pair<K,V>>(new ReflectDatumWriter<Pair<K,V>>());
      
      data.setCodec(CodecFactory.deflateCodec(deflateLevel));
      // data.setCodec(CodecFactory.snappyCodec());
      data.create(
          new Pair<K,V>(Pair.getPairSchema(keySchema, valueSchema)).getSchema(),
          dataFile.getFileSystem(conf).create(dataFile, progress));
      
      index = new DataFileWriter<Pair<K,Long>>(
          new ReflectDatumWriter<Pair<K,Long>>());
      
      index.setCodec(CodecFactory.deflateCodec(deflateLevel));
      
      // copy metadata from job
      index.setMeta(INDEX_INTERVAL, this.indexInterval);
      
      index
          .create(
              new Pair<K,V>(Pair.getPairSchema(keySchema,
                  Schema.create(Type.LONG))).getSchema(), indexFile
                  .getFileSystem(conf).create(indexFile, progress));
      
    }
    
    /** The number of entries that are added before an index entry is added. */
    public int getIndexInterval() {
      return indexInterval;
    }
    
    /**
     * Sets the index interval and stores it in conf
     * 
     * @see #getIndexInterval()
     */
    public static void setIndexInterval(Configuration conf, int interval) {
      conf.setInt(INDEX_INTERVAL, interval);
    }
    
    /** Close the map. */
    public synchronized void close() throws IOException {
      data.close();
      index.close();
    }
    
    /**
     * Append a key/value pair to the map. The key must be greater or equal to
     * the previous key added to the map.
     */
    public synchronized void append(K key, V val) throws IOException {
      
      checkKey(key);
      
      long pos = data.sync();
      // Only write an index if we've changed positions. In a block
      // compressed
      // file, this means we write an entry at the start of each block
      if (size >= lastIndexKeyCount + indexInterval && pos > lastIndexPos) {
        // position.set(pos); // point to current eof
        index.append(new Pair<K,Long>(key, keySchema, pos, Schema
            .create(Type.LONG)));
        lastIndexPos = pos;
        lastIndexKeyCount = size;
      }
      
      data.append(new Pair<K,V>(key, keySchema, val, valueSchema)); // append
      // key/value
      // to
      // data
      size++;
    }
    
    private void checkKey(K key) throws IOException {
      // check that keys are well-ordered
      if (size != 0 && comparator.compare(lastKey, key) > 0) throw new IOException(
          "key out of order: " + key + " after " + lastKey);
      
      // update lastKey with a copy of key by writing and reading
      BinaryEncoder encoder = EncoderFactory.get().directBinaryEncoder(outBuf,
          null);
      Schema schema;
      if (GenericContainer.class.isAssignableFrom(key.getClass())) schema = ((GenericContainer) key)
          .getSchema();
      else schema = ReflectData.get().getSchema(key.getClass());
      
      GenericDatumWriter<K> writer = new ReflectDatumWriter<K>(schema);
      GenericDatumReader<K> reader = new ReflectDatumReader<K>(schema);
      writer.write(key, encoder);
      inBuf.reset(outBuf.getData(), outBuf.getLength());
      BinaryDecoder decoder = DecoderFactory.get().directBinaryDecoder(inBuf,
          null);
      lastKey = reader.read(null, decoder);
    }
  }
  
  /** Provide access to an existing map. */
  public static class Reader<K,V> implements java.io.Closeable {
    
    /**
     * Number of index entries to skip between each entry. Zero by default.
     * Setting this to values larger than zero can facilitate opening large map
     * files using less memory.
     */
    public static final String DATUM_READER_CLASS = "avro.datum.reader.class";
    private int INDEX_SKIP = 0;
    
    private AvroKeyComparator<K> comparator;
    
    private K nextKey;
    private long firstPosition;
    private K finalKey = null;
    private K firstKey = null;
    // the data, on disk
    private DataFileReader<Pair<K,V>> data;
    private DataFileReader<Pair<K,Long>> index;
    // whether the index Reader was closed
    private boolean indexClosed = false;
    private Pair<K,V> pair;
    // the index, in memory
    private int count = -1;
    private K[] keys;
    private long[] positions;
    
    /** Construct a map reader for the named map. */
    public Reader(FileSystem fs, String dirName, Configuration conf)
        throws IOException {
      // this.keySchema = keySchema;
      // this.valueSchema = valueSchema;
      INDEX_SKIP = conf.getInt("io.map.index.skip", 0);
      open(fs, dirName, conf);
    }
    
    @SuppressWarnings("rawtypes")
    public static DatumReader getDatumReader(Configuration conf)
        throws IOException {
      try {
        return conf.getClass(DATUM_READER_CLASS, ReflectDatumReader.class,
            DatumReader.class).newInstance();
      } catch (Exception e) {
        throw new IOException(e);
        // e.printStackTrace();
      }
    }
    
    @SuppressWarnings("rawtypes")
    public static void setDatumReader(Configuration conf,
        Class<? extends DatumReader> classzz) {
      conf.setClass(DATUM_READER_CLASS, classzz, DatumReader.class);
      
    }
    
    @SuppressWarnings("unchecked")
    protected synchronized void open(FileSystem fs, String dirName,
        Configuration conf) throws IOException {
      Path dir = new Path(dirName);
      Path dataFile = new Path(dir, DATA_FILE_NAME);
      Path indexFile = new Path(dir, INDEX_FILE_NAME);
      
      // open the data
      // this.data = new DataFileReader<Pair<K, V>>(new FsInput(dataFile,
      // conf), new ReflectDatumReader<Pair<K, V>>());
      this.data = new DataFileReader<Pair<K,V>>(new FsInput(dataFile, conf),
          getDatumReader(conf));
      
      this.firstPosition = data.previousSync();
      
      comparator = new AvroKeyComparator<K>();
      comparator.setSchema(Pair.getKeySchema(data.getSchema()));
      pair = new Pair<K,V>(data.getSchema());
      // open the index
      // this.index = new DataFileReader<Pair<K, Long>>(new FsInput(
      // indexFile, conf), new ReflectDatumReader<Pair<K, Long>>());
      this.index = new DataFileReader<Pair<K,Long>>(
          new FsInput(indexFile, conf), getDatumReader(conf));
    }
    
    public Schema getKeySchema() {
      return Pair.getKeySchema(data.getSchema());
    }
    
    public Schema getValueSchema() {
      return Pair.getValueSchema(data.getSchema());
    }
    
    @SuppressWarnings("unchecked")
    private void readIndex() throws IOException {
      // read the index entirely into memory
      if (this.keys != null) return;
      this.count = 0;
      this.positions = new long[1024];
      
      try {
        int skip = INDEX_SKIP;
        long position = 0;
        K lastKey = null;
        long lastIndex = -1;
        ArrayList<K> keyBuilder = new ArrayList<K>(1024);
        while (index.hasNext()) {
          Pair<K,Long> pair = new Pair<K,Long>(index.getSchema());
          
          pair = index.next(pair);
          
          // check order to make sure comparator is compatible
          if (lastKey != null && comparator.compare(lastKey, pair.key()) > 0) throw new IOException(
              "key out of order: " + pair.key() + " after " + lastKey);
          if (firstKey == null) firstKey = pair.key();
          lastKey = pair.key();
          position = pair.value();
          if (skip > 0) {
            skip--;
            continue; // skip this entry
          } else {
            skip = INDEX_SKIP; // reset skip
          }
          
          // don't read an index that is the same as the previous one.
          // Block
          // compressed map files used to do this (multiple entries
          // would point
          // at the same block)
          if (position == lastIndex) continue;
          
          if (count == positions.length) {
            positions = Arrays.copyOf(positions, positions.length * 2);
          }
          
          keyBuilder.add(pair.key());
          positions[count] = position;
          count++;
        }
        
        this.keys = (K[]) keyBuilder.toArray(new Object[count]);
        positions = Arrays.copyOf(positions, count);
      } catch (EOFException e) {
        LOG.warn("Unexpected EOF reading " + index + " at entry #" + count
            + ".  Ignoring.");
      } finally {
        indexClosed = true;
        index.close();
      }
    }
    
    /** Re-positions the reader before its first key. */
    public synchronized void reset() throws IOException {
      data.seek(firstPosition);
    }
    
    /**
     * Get the key at approximately the middle of the file. Or null if the file
     * is empty.
     */
    public synchronized K midKey() throws IOException {
      
      readIndex();
      if (count == 0) {
        return null;
      }
      
      return keys[(count - 1) / 2];
    }
    
    private int totalcount = -1;
    
    /**
     * number block in this file.
     * 
     * @param key
     *          key to read into
     */
    
    public synchronized long size() throws IOException {
      
      if (totalcount != -1) return totalcount;
      long originalPosition = data.previousSync(); // save position
      try {
        readIndex(); // make sure index is valid
        if (count > 0) {
          data.seek(positions[count - 1]); // skip to last indexed
          // entry
          totalcount = (int) ((count - 1) * index
              .getMetaLong(MapAvroFile.Writer.INDEX_INTERVAL));
        } else {
          reset(); // start at the beginning
        }
        
        while (data.hasNext()) {
          totalcount++;
          finalKey = AvroUtils.clone(data.next(pair).key());
        } // scan to eof
        
      } finally {
        data.seek(originalPosition); // restore position
      }
      return totalcount;
    }
    
    /**
     * Reads the final key from the file.
     * 
     * @param key
     *          key to read into
     */
    public synchronized K finalKey() throws IOException {
      
      if (finalKey != null) return finalKey;
      long originalPosition = data.previousSync(); // save position
      try {
        readIndex(); // make sure index is valid
        if (count > 0) {
          data.seek(positions[count - 1]); // skip to last indexed
          // entry
          totalcount = (int) ((count - 1) * index
              .getMetaLong(MapAvroFile.Writer.INDEX_INTERVAL));
        } else {
          totalcount = 0;
          reset(); // start at the beginning
        }
        while (data.hasNext()) {
          totalcount++;
          finalKey = AvroUtils.clone(data.next(pair).key());
        } // scan to eof
        
      } finally {
        data.seek(originalPosition); // restore position
      }
      return finalKey;
    }
    
    /**
     * Positions the reader at the named key, or if none such exists, at the key
     * that falls just before or just after dependent on how the
     * <code>before</code> parameter is set.
     * 
     * @param before
     *          - IF true, and <code>key</code> does not exist, position file at
     *          entry that falls just before <code>key</code>. Otherwise,
     *          position file at record that sorts just after.
     * @return 0 - exact match found < 0 - positioned at next record 1 - no more
     *         records in file
     */
    private int seekIndex = -1;
    private long seekPosition = -1;
    
    private synchronized Pair<K,V> seekInternal(K key) throws IOException {
      readIndex(); // make sure index is read
      
      if (seekIndex != -1 // seeked before
          && seekIndex + 1 < count
          && comparator.compare(key, keys[seekIndex + 1]) < 0 // before
          // next
          // indexed
          && comparator.compare(key, nextKey) >= 0) { // but after
        // last seeked
        // do nothing
      } else {
        seekIndex = binarySearch(key);
        if (seekIndex < 0) // decode insertion point
        seekIndex = -seekIndex - 2;
        if (seekIndex == -1) // belongs before first entry
        seekPosition = firstPosition; // use beginning of file
        else seekPosition = positions[seekIndex]; // else use index
      }
      
      data.seek(seekPosition);
      
      // If we're looking for the key before, we need to keep track
      // of the position we got the current key as well as the position
      // of the key before it.
      long nextPosition;
      if (seekIndex + 1 < positions.length) nextPosition = positions[seekIndex + 1];
      else nextPosition = Long.MAX_VALUE;
      // Pair<K, V> pair = new Pair<K,V>(pairSchema);
      while (data.hasNext()) {
        pair = data.next(pair);
        nextKey = AvroUtils.clone(pair.key());
        int c = comparator.compare(key, pair.key());
        if (c == 0) return pair;
        else if (data.previousSync() > nextPosition) {
          return null;
          // else if (data.tell() > nextPosition) {
          // return null;
        }
      }
      return null;
    }
    
    private int binarySearch(K key) {
      int low = 0;
      int high = count - 1;
      
      while (low <= high) {
        int mid = (low + high) >>> 1;
        K midVal = keys[mid];
        int cmp = comparator.compare(midVal, key);
        
        if (cmp < 0) low = mid + 1;
        else if (cmp > 0) high = mid - 1;
        else return mid; // key found
      }
      return -(low + 1); // key not found.
    }
    
    public synchronized boolean hasNext() throws IOException {
      return data.hasNext();
    }
    
    /**
     * Read the next key/value pair in the map into <code>key</code> and
     * <code>val</code>. Returns true if such a pair exists and false when at
     * the end of the map
     */
    public synchronized Pair<K,V> next(Pair<K,V> record) throws IOException {
      if (record == null) return this.next();
      else {
        record.setSchema(data.getSchema());
        return data.next(record);
      }
    }
    
    /**
     * Read the next key/value pair in the map into <code>key</code> and
     * <code>val</code>. Returns true if such a pair exists and false when at
     * the end of the map
     */
    public synchronized Pair<K,V> next() throws IOException {
      Pair<K,V> p = new Pair<K,V>(data.getSchema());
      return this.next(p);
      // return data.next(pair);
    }
    
    /** Return the value for the named key, or null if none exists. */
    public synchronized V get(K key) throws IOException {
      Pair<K,V> pair = seekInternal(key);
      if (pair != null) {
        
        return pair.value();
      } else return null;
    }
    
    /** Close the map. */
    public synchronized void close() throws IOException {
      if (!indexClosed) {
        index.close();
      }
      data.close();
    }
    
  }
  
  /** Renames an existing map directory. */
  public static void rename(FileSystem fs, String oldName, String newName)
      throws IOException {
    Path oldDir = new Path(oldName);
    Path newDir = new Path(newName);
    if (!fs.rename(oldDir, newDir)) {
      throw new IOException("Could not rename " + oldDir + " to " + newDir);
    }
  }
  
  /** Deletes the named map file. */
  public static void delete(FileSystem fs, String name) throws IOException {
    Path dir = new Path(name);
    Path data = new Path(dir, DATA_FILE_NAME);
    Path index = new Path(dir, INDEX_FILE_NAME);
    
    fs.delete(data, true);
    fs.delete(index, true);
    fs.delete(dir, true);
  }
  
  /**
   * This method attempts to fix a corrupt MapFile by re-creating its index.
   * 
   * @param <K>
   * 
   * @param fs
   *          filesystem
   * @param dir
   *          directory containing the MapFile data and index
   * @param keyClass
   *          key class (has to be a subclass of Writable)
   * @param valueClass
   *          value class (has to be a subclass of Writable)
   * @param dryrun
   *          do not perform any changes, just report what needs to be done
   * @return number of valid entries in this MapFile, or -1 if no fixing was
   *         needed
   * @throws Exception
   */
  public static <K,V> long fix(FileSystem fs, Path dir, Schema keySchema,
      Schema valueSchema, boolean dryrun, Configuration conf) throws Exception {
    String dr = (dryrun ? "[DRY RUN ] " : "");
    Path data = new Path(dir, DATA_FILE_NAME);
    Path index = new Path(dir, INDEX_FILE_NAME);
    int indexInterval = conf.getInt(MapAvroFile.Writer.INDEX_INTERVAL, 128);
    if (!fs.exists(data)) {
      // there's nothing we can do to fix this!
      throw new Exception(dr + "Missing data file in " + dir
          + ", impossible to fix this.");
    }
    if (fs.exists(index)) {
      // no fixing needed
      return -1;
    }
    DataFileReader<Pair<K,V>> dataReader = new DataFileReader<Pair<K,V>>(
        new FsInput(data, conf), new SpecificDatumReader<Pair<K,V>>(
            Schema.create(Type.NULL)));
    ;
    
    long cnt = 0L;
    DataFileWriter<Pair<K,Long>> indexWriter = null;
    if (!dryrun) {
      indexWriter = new DataFileWriter<Pair<K,Long>>(
          new SpecificDatumWriter<Pair<K,Long>>());
      
      int level = conf.getInt(DEFLATE_LEVEL_KEY,
          MapAvroFile.DEFAULT_DEFLATE_LEVEL);
      indexWriter.setCodec(CodecFactory.deflateCodec(level));
      // indexWriter.setCodec(CodecFactory.snappyCodec());
      
      // copy metadata from job
      for (Map.Entry<String,String> e : conf) {
        // if (e.getKey().startsWith(AvroJob.TEXT_PREFIX)) indexWriter.setMeta(e
        // .getKey().substring(AvroJob.TEXT_PREFIX.length()), e.getValue());
        // if (e.getKey().startsWith(AvroJob.BINARY_PREFIX))
        // indexWriter.setMeta(e
        // .getKey().substring(AvroJob.BINARY_PREFIX.length()), URLDecoder
        // .decode(e.getValue(), "ISO-8859-1").getBytes("ISO-8859-1"));
      }
      
      indexWriter
          .create(
              new Pair<K,V>(Pair.getPairSchema(keySchema,
                  Schema.create(Type.LONG))).getSchema(),
              index.getFileSystem(conf).create(index));
    }
    try {
      long pos = 0L;
      while (dataReader.hasNext()) {
        Pair<K,V> pair = dataReader.next();
        cnt++;
        pos = dataReader.previousSync();
        if (cnt % indexInterval == 0) {
          if (!dryrun) indexWriter.append(new Pair<K,Long>(pair.key(),
              keySchema, pos, Schema.create(Type.LONG)));
        }
      }
    } catch (Throwable t) {
      // truncated data file. swallow it.
    }
    dataReader.close();
    if (!dryrun) indexWriter.close();
    return cnt;
  }
}
