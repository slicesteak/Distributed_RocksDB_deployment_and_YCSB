package com.ukchukx.rocksdbexample.repository;

import lombok.extern.slf4j.Slf4j;

import org.springframework.stereotype.Repository;
import org.springframework.util.SerializationUtils;

import javax.annotation.PostConstruct;

// ------------------------------------------------------------------------
import java.util.*;
// import site.ycsb.*;
import java.util.concurrent.ConcurrentHashMap;
// import net.jcip.annotations.GuardedBy;
import org.rocksdb.*;
// import org.slf4j.Logger;
// import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import static java.nio.charset.StandardCharsets.UTF_8;
// ------------------------------------------------------------------------

@Slf4j
@Repository
public class RocksDBRepository implements KVRepository<String, Object> {
  
    // 这里手动设置
  static final String PROPERTY_ROCKSDB_DIR = "/tmp/rocksdb_tmp";
  static final String PROPERTY_ROCKSDB_OPTIONS_FILE = "";
  static final String FILE_NAME = "spring-boot-db";
  private static final String COLUMN_FAMILY_NAMES_FILENAME = "CF_NAMES";

  private static Path rocksDbDir = null;
  private static Path optionsFile = null;
  private static RocksObject dbOptions = null;
  private static RocksDB rocksDb;
  private static int references = 0;

  private static final ConcurrentMap<String, ColumnFamily> COLUMN_FAMILIES = new ConcurrentHashMap<>();
  private static final ConcurrentMap<String, Lock> COLUMN_FAMILY_LOCKS = new ConcurrentHashMap<>();

  // ------------------------------------------------------------

  @PostConstruct // execute after the application starts.
  void initialize() {
    // RocksDB.loadLibrary();
    // final Options options = new Options();
    // options.setCreateIfMissing(true);
    // File baseDir;
    // baseDir = new File(PROPERTY_ROCKSDB_DIR, FILE_NAME);

    // try {
    //   Files.createDirectories(baseDir.getParentFile().toPath());
    //   Files.createDirectories(baseDir.getAbsoluteFile().toPath());
    //   rocksDb = RocksDB.open(options, baseDir.getAbsolutePath());

    //   log.info("RocksDB initialized");
    // } catch(IOException | RocksDBException e) {
    //   log.error("Error initializng RocksDB. Exception: '{}', message: '{}'", e.getCause(), e.getMessage(), e);
    // }
  }

  private static final class ColumnFamily {
    private final ColumnFamilyHandle handle;
    private final ColumnFamilyOptions options;

    private ColumnFamily(final ColumnFamilyHandle handle, final ColumnFamilyOptions options) {
      this.handle = handle;
      this.options = options;
    }

    public ColumnFamilyHandle getHandle() {
      return handle;
    }

    public ColumnFamilyOptions getOptions() {
      return options;
    }
  }

  @Override
  public synchronized boolean init() throws DBException {
    references++;
    if(references > 1)
        return SUCCESS;
    
    synchronized(RocksDBRepository.class) {
        rocksDbDir = Paths.get(PROPERTY_ROCKSDB_DIR);
        optionsFile = Paths.get(PROPERTY_ROCKSDB_OPTIONS_FILE);

        log.info("RocksDB data dir: " + rocksDbDir);
        try {
            if (Files.exists(optionsFile) && !PROPERTY_ROCKSDB_OPTIONS_FILE.equals("")) {
                log.info("RocksDB options file: " + optionsFile);
                log.info("step into option files");
                initRocksDBWithOptionsFile();
            } else {
                log.info("init without option file");
                initRocksDB();
            }
        } catch (final IOException | RocksDBException e) {
          throw new DBException(e);
        }
        references++;
      }
    System.out.println("success init");
    return SUCCESS;
  }

  private void initRocksDBWithOptionsFile() throws IOException, RocksDBException {
    if(!Files.exists(rocksDbDir)) {
      Files.createDirectories(rocksDbDir);
    }

    final DBOptions options = new DBOptions();
    final List<ColumnFamilyDescriptor> cfDescriptors = new ArrayList<>();
    final List<ColumnFamilyHandle> cfHandles = new ArrayList<>();

    RocksDB.loadLibrary();
    OptionsUtil.loadOptionsFromFile(optionsFile.toAbsolutePath().toString(), Env.getDefault(), options, cfDescriptors);
    dbOptions = options;

    rocksDb = RocksDB.open(options, rocksDbDir.toAbsolutePath().toString(), cfDescriptors, cfHandles);

    for(int i = 0; i < cfDescriptors.size(); i++) {
      String cfName = new String(cfDescriptors.get(i).getName());
      final ColumnFamilyHandle cfHandle = cfHandles.get(i);
      final ColumnFamilyOptions cfOptions = cfDescriptors.get(i).getOptions();

      COLUMN_FAMILIES.put(cfName, new ColumnFamily(cfHandle, cfOptions));
    }
  }

  @Override
  public synchronized void cleanup(){
    synchronized (RocksDBRepository.class) {
      try {
        if (references == 1) {
          for (final ColumnFamily cf : COLUMN_FAMILIES.values()) {
            cf.getHandle().close();
          }

          rocksDb.close();
          rocksDb = null;

          dbOptions.close();
          dbOptions = null;

          for (final ColumnFamily cf : COLUMN_FAMILIES.values()) {
            cf.getOptions().close();
          }
          saveColumnFamilyNames();
          COLUMN_FAMILIES.clear();

          rocksDbDir = null;
        }

      } catch (final IOException e) {
        log.error("error in RocksDBRepository.java", e);
      } finally {
        references--;
      }
    }
    System.out.println("success clean up");
  }
  

  @Override
  public synchronized boolean save(String key, Object value) {
    log.info("saving value '{}' with key '{}'", value, key);

    try {
    //   log.info("value:"+toString(value));
      rocksDb.put(key.getBytes(), SerializationUtils.serialize(value));
    } catch (RocksDBException e) {
      log.error("Error saving entry. Cause: '{}', message: '{}'", e.getCause(), e.getMessage());

      return false;
    }

    return true;
  }

  @Override
  public synchronized Optional<Object> find(String key) {
    Object value = null;

    try {
      byte[] bytes = rocksDb.get(key.getBytes());
      if (bytes != null) value = SerializationUtils.deserialize(bytes);
    } catch (RocksDBException e) {
      log.error(
        "Error retrieving the entry with key: {}, cause: {}, message: {}",
        key,
        e.getCause(),
        e.getMessage()
      );
    }

    log.info("finding key '{}' returns '{}'", key, value);

    return value != null ? Optional.of(value) : Optional.empty();
  }

  @Override
  public synchronized boolean delete(String key) {
    log.info("deleting key '{}'", key);

    try {
      rocksDb.delete(key.getBytes());
    } catch (RocksDBException e) {
      log.error("Error deleting entry, cause: '{}', message: '{}'", e.getCause(), e.getMessage());

      return false;
    }

    return true;
  }

  // 
  @Override
  public synchronized Map<String, ByteIterator> read(String table,  String key, Set<String> fields, Map<String, ByteIterator> result){
    
    try {
        if (!COLUMN_FAMILIES.containsKey(table)) {
          createColumnFamily(table);
        }
  
        final ColumnFamilyHandle cf = COLUMN_FAMILIES.get(table).getHandle();
        final byte[] values = rocksDb.get(cf, key.getBytes(UTF_8));
        if(values == null) {
          log.info("not found");
          return result;
        }
        log.info("read "+ table + ":"+key);
        
        result = deserializeValues(values, fields, result);
        
        return result;
      } catch(final RocksDBException e) {
        log.error(e.getMessage(), e);
        return result;
    }
  }

  @Override
  public synchronized boolean scan(String table, String startkey, int recordcount, Set<String> fields, Vector<HashMap<String, ByteIterator>> result){
    log.info("scan "+ table + ", start key:" + startkey);

    try {
        if (!COLUMN_FAMILIES.containsKey(table)) {
          createColumnFamily(table);
        }
  
        final ColumnFamilyHandle cf = COLUMN_FAMILIES.get(table).getHandle();
        try(final RocksIterator iterator = rocksDb.newIterator(cf)) {
          int iterations = 0;
          for (iterator.seek(startkey.getBytes(UTF_8)); iterator.isValid() && iterations < recordcount;
               iterator.next()) {
            final HashMap<String, ByteIterator> values = new HashMap<>();
            deserializeValues(iterator.value(), fields, values);
            result.add(values);
            iterations++;
          }
        }
  
        return SUCCESS;
      } catch(final RocksDBException e) {
        log.error(e.getMessage(), e);
        return FAIL;
      }
  }

  @Override
  public synchronized boolean update(String table, String key, Map<String, ByteIterator> values){
    log.info("update "+ table + "key:" + key);
    try {
        if (!COLUMN_FAMILIES.containsKey(table)) {
          createColumnFamily(table);
        }
  
        final ColumnFamilyHandle cf = COLUMN_FAMILIES.get(table).getHandle();
        final Map<String, ByteIterator> result = new HashMap<>();
        final byte[] currentValues = rocksDb.get(cf, key.getBytes(UTF_8));
        if(currentValues == null) {
          return FAIL;
        }
        deserializeValues(currentValues, null, result);
  
        //update
        result.putAll(values);
  
        //store
        rocksDb.put(cf, key.getBytes(UTF_8), serializeValues(result));
  
        return SUCCESS;
  
      } catch(final RocksDBException | IOException e) {
        log.error(e.getMessage(), e);
        return FAIL;
      }
  }

  @Override
  public synchronized boolean insert(String table, String key, Map<String, ByteIterator> values){
    try {
        if (!COLUMN_FAMILIES.containsKey(table)) {
          createColumnFamily(table);
        }
        final ColumnFamilyHandle cf = COLUMN_FAMILIES.get(table).getHandle();
        Map<String, ByteIterator> debug_output = values;
        rocksDb.put(cf, key.getBytes(UTF_8), serializeValues(values));
        
        log.info("insert key:" + key);
        return SUCCESS;
      } catch(final RocksDBException | IOException e) {
        log.error(e.getMessage(), e);
        return FAIL;
      }
  }

  @Override
  public synchronized boolean delete(String table, String key){
    try {
        if (!COLUMN_FAMILIES.containsKey(table)) {
          createColumnFamily(table);
        }
  
        final ColumnFamilyHandle cf = COLUMN_FAMILIES.get(table).getHandle();
        rocksDb.delete(cf, key.getBytes(UTF_8));
        log.info("delete " + table + ":" + key);

        return SUCCESS;
      } catch(final RocksDBException e) {
        log.error(e.getMessage(), e);
        return FAIL;
      }
  }


  private void initRocksDB() throws IOException, RocksDBException {
    if(!Files.exists(rocksDbDir)) {
      System.out.println("create rocksDB Dir");
      Files.createDirectories(rocksDbDir);
    }

    final List<String> cfNames = loadColumnFamilyNames();
    final List<ColumnFamilyOptions> cfOptionss = new ArrayList<>();
    final List<ColumnFamilyDescriptor> cfDescriptors = new ArrayList<>();

    for(final String cfName : cfNames) {
      final ColumnFamilyOptions cfOptions = new ColumnFamilyOptions()
          .optimizeLevelStyleCompaction();
      final ColumnFamilyDescriptor cfDescriptor = new ColumnFamilyDescriptor(
          cfName.getBytes(UTF_8),
          cfOptions
      );
      cfOptionss.add(cfOptions);
      cfDescriptors.add(cfDescriptor);
    }

    final int rocksThreads = Runtime.getRuntime().availableProcessors() * 2;

    if(cfDescriptors.isEmpty()) {
      final Options options = new Options()
          .optimizeLevelStyleCompaction()
          .setCreateIfMissing(true)
          .setCreateMissingColumnFamilies(true)
          .setIncreaseParallelism(rocksThreads)
          .setMaxBackgroundCompactions(rocksThreads)
          .setInfoLogLevel(InfoLogLevel.INFO_LEVEL);
      dbOptions = options;
      rocksDb = RocksDB.open(options, rocksDbDir.toAbsolutePath().toString());
    } else {
      final DBOptions options = new DBOptions()
          .setCreateIfMissing(true)
          .setCreateMissingColumnFamilies(true)
          .setIncreaseParallelism(rocksThreads)
          .setMaxBackgroundCompactions(rocksThreads)
          .setInfoLogLevel(InfoLogLevel.INFO_LEVEL);
      dbOptions = options;

      final List<ColumnFamilyHandle> cfHandles = new ArrayList<>();
      rocksDb = RocksDB.open(options, rocksDbDir.toAbsolutePath().toString(), cfDescriptors, cfHandles);
      for(int i = 0; i < cfNames.size(); i++) {
        COLUMN_FAMILIES.put(cfNames.get(i), new ColumnFamily(cfHandles.get(i), cfOptionss.get(i)));
      }
    }
  }

  private void saveColumnFamilyNames() throws IOException {
    final Path file = rocksDbDir.resolve(COLUMN_FAMILY_NAMES_FILENAME);
    try(final PrintWriter writer = new PrintWriter(Files.newBufferedWriter(file, UTF_8))) {
      writer.println(new String(RocksDB.DEFAULT_COLUMN_FAMILY, UTF_8));
      for(final String cfName : COLUMN_FAMILIES.keySet()) {
        writer.println(cfName);
      }
    }
  }

  private List<String> loadColumnFamilyNames() throws IOException {
    final List<String> cfNames = new ArrayList<>();
    final Path file = rocksDbDir.resolve(COLUMN_FAMILY_NAMES_FILENAME);
    if(Files.exists(file)) {
      try (final LineNumberReader reader =
               new LineNumberReader(Files.newBufferedReader(file, UTF_8))) {
        String line = null;
        while ((line = reader.readLine()) != null) {
          cfNames.add(line);
        }
      }
    }
    return cfNames;
  }

  private Map<String, ByteIterator> deserializeValues(final byte[] values, final Set<String> fields,
      final Map<String, ByteIterator> result) {
    final ByteBuffer buf = ByteBuffer.allocate(4);

    int offset = 0;
    while(offset < values.length) {
      buf.put(values, offset, 4);
      buf.flip();
      final int keyLen = buf.getInt();
      buf.clear();
      offset += 4;

      final String key = new String(values, offset, keyLen);
      offset += keyLen;

      buf.put(values, offset, 4);
      buf.flip();
      final int valueLen = buf.getInt();
      buf.clear();
      offset += 4;

      if(fields == null || fields.contains(key)) {
        result.put(key, new ByteArrayByteIterator(values, offset, valueLen));
      }

      offset += valueLen;
    }

    return result;
  }

  private byte[] serializeValues(final Map<String, ByteIterator> values) throws IOException {
    try(final ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
      final ByteBuffer buf = ByteBuffer.allocate(4);

      for(final Map.Entry<String, ByteIterator> value : values.entrySet()) {
        final byte[] keyBytes = value.getKey().getBytes(UTF_8);
        final byte[] valueBytes = value.getValue().toArray();

        buf.putInt(keyBytes.length);
        baos.write(buf.array());
        baos.write(keyBytes);

        buf.clear();

        buf.putInt(valueBytes.length);
        baos.write(buf.array());
        baos.write(valueBytes);

        buf.clear();
      }
      return baos.toByteArray();
    }
  }

  private ColumnFamilyOptions getDefaultColumnFamilyOptions(final String destinationCfName) {
    final ColumnFamilyOptions cfOptions;

    if (COLUMN_FAMILIES.containsKey("default")) {
    //   LOGGER.warn("no column family options for \"" + destinationCfName + "\" " +
    //               "in options file - using options from \"default\"");
      cfOptions = COLUMN_FAMILIES.get("default").getOptions();
    } else {
    //   LOGGER.warn("no column family options for either \"" + destinationCfName + "\" or " +
    //               "\"default\" in options file - initializing with empty configuration");
      cfOptions = new ColumnFamilyOptions();
    }
    // LOGGER.warn("Add a CFOptions section for \"" + destinationCfName + "\" to the options file, " +
    //             "or subsequent runs on this DB will fail.");

    return cfOptions;
  }

  private void createColumnFamily(final String name) throws RocksDBException {
    COLUMN_FAMILY_LOCKS.putIfAbsent(name, new ReentrantLock());

    final Lock l = COLUMN_FAMILY_LOCKS.get(name);
    l.lock();
    try {
      if(!COLUMN_FAMILIES.containsKey(name)) {
        final ColumnFamilyOptions cfOptions;

        if (optionsFile != null) {
          // RocksDB requires all options files to include options for the "default" column family;
          // apply those options to this column family
          cfOptions = getDefaultColumnFamilyOptions(name);
        } else {
          cfOptions = new ColumnFamilyOptions().optimizeLevelStyleCompaction();
        }

        final ColumnFamilyHandle cfHandle = rocksDb.createColumnFamily(
            new ColumnFamilyDescriptor(name.getBytes(UTF_8), cfOptions)
        );
        COLUMN_FAMILIES.put(name, new ColumnFamily(cfHandle, cfOptions));
      }
    } finally {
      l.unlock();
    }
  }

}
