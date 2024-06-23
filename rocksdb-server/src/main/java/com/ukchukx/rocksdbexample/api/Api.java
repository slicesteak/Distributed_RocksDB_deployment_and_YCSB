package com.ukchukx.rocksdbexample.api;

import com.ukchukx.rocksdbexample.repository.KVRepository;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.*; // Set
import com.ukchukx.rocksdbexample.repository.ByteIterator;
import java.util.concurrent.ConcurrentHashMap; // hashmap
import com.ukchukx.rocksdbexample.repository.ByteArrayByteIterator;
import java.nio.charset.*;
import com.ukchukx.rocksdbexample.repository.DBException;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.ByteArrayOutputStream;
import java.io.ObjectOutputStream;
import java.io.ByteArrayInputStream;
import java.io.ObjectInputStream;

@Slf4j
@RestController
@RequestMapping("/api")
public class Api {
  private final KVRepository<String, Object> repository;
  private final ObjectMapper objectMapper = new ObjectMapper();

  public Api(KVRepository<String, Object> repository) {
    this.repository = repository;
  }

  @PostMapping(value = "/init", consumes = MediaType.APPLICATION_JSON_VALUE, produces = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<Object> init() throws DBException {
        return repository.init()
                ? ResponseEntity.ok("SUCCESS")
                : ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).build();
    }

  @PostMapping(value = "/cleanup", consumes = MediaType.APPLICATION_JSON_VALUE, produces = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<Object> cleanup() {
        repository.cleanup();
        return ResponseEntity.ok("SUCCESS");
    }


    public static class ReadRequest {
        public String table;
        public String key;
        public Set<String> fields;
    }
  @PostMapping(value = "/read", consumes = MediaType.APPLICATION_JSON_VALUE, produces = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<Object> read(@RequestBody ReadRequest value) {
        // 这里解析出参数
        // Map是一个接口，不能直接实例化。你需要使用一个具体的实现类，例如HashMap、TreeMap等
        HashMap<String, ByteIterator> result = new HashMap<String, ByteIterator>();
        
        repository.read(value.table,
                        value.key,
                        value.fields,
                        result);

        // return ResponseEntity.ok(result.toString());
        System.out.println("the result size is " + result.size());
        try {
            HashMap<String, byte[]> serializedResult = new HashMap<>();
            for (Map.Entry<String, ByteIterator> entry : result.entrySet()) {
                serializedResult.put(entry.getKey(), entry.getValue().toArray());
            }

            // Convert the map to JSON
            ObjectMapper objectMapper = new ObjectMapper();
            String jsonString = objectMapper.writeValueAsString(serializedResult);

            return ResponseEntity.ok(jsonString);

        } catch (Exception e) {
            e.printStackTrace();
            return ResponseEntity.status(500).body("Error processing the response");
        }
    }

    public static class ScanRequest {
        public String table;
        public String startkey;
        public int recordcount;
        public Set<String> fields;
    }

  @PostMapping(value = "/scan", consumes = MediaType.APPLICATION_JSON_VALUE, produces = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<Object> scan(@RequestBody ScanRequest value) {
        Vector<HashMap<String, ByteIterator>> result = new Vector<>();
        
        if(repository.scan(value.table, value.startkey, value.recordcount, value.fields, result) == repository.SUCCESS){
            try {
                ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
                ObjectOutputStream objectOutputStream = new ObjectOutputStream(byteArrayOutputStream);
                objectOutputStream.writeObject(result);
                objectOutputStream.flush();
                byte[] byteArray = byteArrayOutputStream.toByteArray();
                
                return ResponseEntity.ok(byteArray);  // seems exception caused at here
            } catch (Exception e) {
                e.printStackTrace();
                return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body("Serialization error: " + e.getMessage());
            }
        } else {
            return  ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).build();
        }
    }

    public static class UpdateRequest {
        public String table;
        public String key;
        // public Map<String, ByteIterator> values;
        public Map<String, String> values;
    }
  @PostMapping(value = "/update", consumes = MediaType.APPLICATION_JSON_VALUE, produces = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<Object> update(@RequestBody UpdateRequest value) {
        // 这里解析出参数
        HashMap<String, ByteIterator> byteIteratorValues = new HashMap<>();
        for (Map.Entry<String, String> entry : value.values.entrySet()) {
            byteIteratorValues.put(entry.getKey(), new ByteArrayByteIterator(entry.getValue().getBytes(StandardCharsets.UTF_8)));
        }

        return repository.update(value.table,
                                 value.key,
                                 byteIteratorValues)
                ? ResponseEntity.ok("SUCCESS")
                : ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).build();
    }

    public static class InsertRequest {
        public String table;
        public String key;
        // public HashMap<String, ByteIterator> values;
        public HashMap<String, String> values;
    }
  @PostMapping(value = "/insert", consumes = MediaType.APPLICATION_JSON_VALUE, produces = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<Object> insert(@RequestBody InsertRequest value) {
        
        HashMap<String, ByteIterator> byteIteratorValues = new HashMap<>();
        for (Map.Entry<String, String> entry : value.values.entrySet()) {
            byteIteratorValues.put(entry.getKey(), new ByteArrayByteIterator(entry.getValue().getBytes(StandardCharsets.UTF_8)));
        }
        
        // Map需要先转换然后才能使用，不然直接解析将为NULL
        return repository.insert(value.table,
                                    value.key,
                                    byteIteratorValues)
                ? ResponseEntity.ok("SUCCESS")
                : ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).build();
    }

    public static class DeleteRequest {
        public String table;
        public String key;
    }
  @PostMapping(value = "/delete", consumes = MediaType.APPLICATION_JSON_VALUE, produces = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<Object> delete(@RequestBody DeleteRequest value) {
        // 这里解析出参数
        
        return repository.delete(value.table,
                                 value.key)
                ? ResponseEntity.ok("SUCCESS")
                : ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).build();
    }

  // curl -iv -X GET -H "Content-Type: application/json" http://localhost:8080/api/foo
//   @GetMapping(value = "/{key}")
  @GetMapping(value = "/{key}", produces = MediaType.APPLICATION_JSON_VALUE)
  public ResponseEntity<Object> find(@PathVariable("key") String key) {
    return ResponseEntity.of(repository.find(key));
  }

  // curl -iv -X DELETE -H "Content-Type: application/json" http://localhost:8080/api/foo
  @DeleteMapping(value = "/{key}", produces = MediaType.APPLICATION_JSON_VALUE)
  public ResponseEntity<Object> delete(@PathVariable("key") String key) {
    return repository.delete(key) 
      ? ResponseEntity.noContent().build() 
      : ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).build();
  }
}