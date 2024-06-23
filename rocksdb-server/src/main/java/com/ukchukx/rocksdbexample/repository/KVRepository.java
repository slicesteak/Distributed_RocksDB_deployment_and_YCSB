package com.ukchukx.rocksdbexample.repository;

import java.util.*;

public interface KVRepository<K, V> {
    public static boolean SUCCESS = true;
    public static boolean FAIL = false;


  boolean save(K key, V value);
  Optional<V> find(K key);
  boolean delete(K key);
  
  boolean init() throws DBException ;
  void cleanup();
  
  Map<String, ByteIterator> read(String table,  String key, Set<String> fields, Map<String, ByteIterator> result);
  boolean scan(String table, String startkey, int recordcount, Set<String> fields, Vector<HashMap<String, ByteIterator>> result);
  boolean update(String table, String key, Map<String, ByteIterator> values);
  boolean insert(String table, String key, Map<String, ByteIterator> values);
  boolean delete(String table, String key);
}