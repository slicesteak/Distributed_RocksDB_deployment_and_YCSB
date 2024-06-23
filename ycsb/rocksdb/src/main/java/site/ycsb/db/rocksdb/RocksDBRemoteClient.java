package site.ycsb.db.rocksdb;

// import java.io.ByteArrayInputStream;
// import java.io.ObjectInputStream;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.net.http.HttpRequest.BodyPublishers;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;

import site.ycsb.*;
import java.util.*;
import com.fasterxml.jackson.databind.ObjectMapper;
import static java.nio.charset.StandardCharsets.UTF_8;
import org.slf4j.LoggerFactory;
import org.slf4j.Logger;

import java.util.ArrayList;

/**
 * RocksDBRemoteClient用于通过HTTP与远程的RocksDB服务交互.
 */
public class RocksDBRemoteClient {

//   private static HashMap<Integer, String> list;
  private ObjectMapper objectMapper = new ObjectMapper();
  private HttpClient httpClient = HttpClient.newHttpClient();

  private static final Logger LOGGER = LoggerFactory.getLogger(RocksDBClient.class);

  public void init(ArrayList<String> slaveNode){
    for (String node:slaveNode) {
      sendRequest(node, "/init", null, null);
    }
  }

  public void cleanup(ArrayList<String> slaveNode){
    for (String node:slaveNode) {
      sendRequest(node, "/cleanup", null, null);
    }
  }

  private Status sendRequest(String myURL, String reqType, Object requestBody, Map<String, ByteIterator> result){
    try {
      String requestBodyString = objectMapper.writeValueAsString(requestBody);
      HttpRequest httpRequest = null;

      if(requestBody == null){
        httpRequest = HttpRequest.newBuilder()
                .uri(URI.create(myURL + reqType))
                .header("Content-Type", "application/json")
                .POST(BodyPublishers.noBody())
                .build();
      } else {
        httpRequest = HttpRequest.newBuilder()
                .uri(URI.create(myURL + reqType))
                .header("Content-Type", "application/json")
                .POST(BodyPublishers.ofString(requestBodyString))
                .build();
      }
    
      HttpResponse<String> response = httpClient.send(httpRequest, HttpResponse.BodyHandlers.ofString());

      if (response.statusCode() == 200) {
        if(reqType.equals("/read")) {
          String responseBody = response.body();
          
          HashMap<String, byte[]> responseMap = objectMapper.readValue(responseBody, 
              new TypeReference<HashMap<String, byte[]>>(){});
          // 用最初的方式解决了
          for (Map.Entry<String, byte[]> entry : responseMap.entrySet()) {
            result.put(entry.getKey(), new ByteArrayByteIterator(entry.getValue()));
          }
        }
        
      } else {
        System.err.println("HTTP POST request failed with status code: " + response.statusCode());
        System.err.println("Response body: " + response.body());
      }
      return Status.OK;

    } catch (JsonProcessingException e) {
      System.err.println("Error converting request body to JSON string: " + e.getMessage());
      e.printStackTrace(); // 打印堆栈信息以供调试
      return Status.ERROR;
    } catch (IOException | InterruptedException e) {
      System.err.println("RocksDBRemoteClient.java:Error sending HTTP request: " + e.getMessage());
      System.err.println("url:" + myURL + reqType);
      e.printStackTrace(); // 打印堆栈信息以供调试
      return Status.ERROR;
    }
  }
  
  private static class ReadRequest {
    private String table;
    private String key;
    private Set<String> fields;
  
    // Getter for table
    public String getTable() {
      return table;
    }
  
    // Setter for table
    public void setTable(String table1) {
      this.table = table1;
    }
  
    // Getter for key
    public String getKey() {
      return key;
    }
  
    // Setter for key
    public void setKey(String key1) {
      this.key = key1;
    }
  
    // Getter for fields
    public Set<String> getFields() {
      return fields;
    }
  
    // Setter for fields
    public void setFields(Set<String> fields1) {
      this.fields = fields1;
    }
  }
  
  public Status read(String table,  String key, Set<String> fields, Map<String, ByteIterator> result, String nodeURL){
    ReadRequest requestBody = new ReadRequest();
    requestBody.setTable(table);
    requestBody.setKey(key);
    requestBody.setFields(fields);

    return sendRequest(nodeURL, "/read", requestBody, result);
  }

  private static class ScanRequest {
    private String table;
    private String startkey;
    private int recordcount;
    private Set<String> fields;
  
    public String getTable() {
      return table;
    }
  
    public void setTable(String table1) {
      this.table = table1;
    }
  
    public String getStartkey() {
      return startkey;
    }
  
    public void setStartkey(String startkey1) {
      this.startkey = startkey1;
    }
  
    public int getRecordcount() {
      return recordcount;
    }
  
    public void setRecordcount(int recordcount1) {
      this.recordcount = recordcount1;
    }
  
    public Set<String> getFields() {
      return fields;
    }
  
    public void setFields(Set<String> fields1) {
      this.fields = fields1;
    }
  }
  
  public Status scan(String table, String startkey, int recordcount, Set<String> fields, 
      Vector<HashMap<String, ByteIterator>> result, ArrayList<String> slaveNode){
    ScanRequest requestBody = new ScanRequest();
    requestBody.setTable(table);
    requestBody.setStartkey(startkey);
    requestBody.setRecordcount(recordcount);
    requestBody.setFields(fields);

    // Collection<String> myURLs = list.values();
    boolean isFrist = true;
    for(String node:slaveNode){
      sendRequest(node , "/scan", requestBody, new HashMap<>());
    }
    return Status.OK; // 强制OK
  }
  
  private static class UpdateRequest {
    private String table;
    private String key;
    private HashMap<String, String> values;
  
    public String getTable() {
      return table;
    }
  
    public void setTable(String table1) {
      this.table = table1;
    }
  
    public String getKey() {
      return key;
    }
  
    public void setKey(String key1) {
      this.key = key1;
    }
  
    public Map<String, String> getValues() {
      return values;
    }
  
    public void setValues(HashMap<String, String> values1) {
      this.values = values1;
    }
  }
  
  public Status update(String table, String key, Map<String, ByteIterator> values, String nodeURL){
    UpdateRequest requestBody = new UpdateRequest();
    requestBody.setTable(table);
    requestBody.setKey(key);

    HashMap<String, String> stringValues = new HashMap<>();
    for (Map.Entry<String, ByteIterator> entry : values.entrySet()) {
      stringValues.put(entry.getKey(), new String(entry.getValue().toArray(), UTF_8));
    }
    requestBody.setValues(stringValues);
    // String requestBodyStringed = objectMapper.writeValueAsString(requestBody);
    return sendRequest(nodeURL , "/update", requestBody, new HashMap<>());
  }
  
  private static class InsertRequest {
    private String table;
    private String key;
    private HashMap<String, String> values;
  
    public String getTable() {
      return table;
    }
  
    public void setTable(String table1) {
      this.table = table1;
    }
  
    public String getKey() {
      return key;
    }
  
    public void setKey(String key1) {
      this.key = key1;
    }
  
    public Map<String, String> getValues() {
      return values;
    }
  
    public void setValues(HashMap<String, String> values1) {
      this.values = values1;
    }
  }

  public Status insert(String table, String key, Map<String, ByteIterator> values, String nodeURL){
    UpdateRequest requestBody = new UpdateRequest();
    requestBody.setTable(table);
    requestBody.setKey(key);

    HashMap<String, String> stringValues = new HashMap<>();
    for (Map.Entry<String, ByteIterator> entry : values.entrySet()) {
      stringValues.put(entry.getKey(), new String(entry.getValue().toArray(), UTF_8));
    }
    requestBody.setValues(stringValues);
    // String requestBodyStringed = objectMapper.writeValueAsString(requestBody);
    return sendRequest(nodeURL , "/insert", requestBody, new HashMap<>());
  }

  private static class DeleteRequest {
    private String table;
    private String key;
  
    public String getTable() {
      return table;
    }
  
    // Setter for table
    public void setTable(String table1) {
      this.table = table1;
    }
  
    // Getter for key
    public String getKey() {
      return key;
    }
  
    // Setter for key
    public void setKey(String key1) {
      this.key = key1;
    }
  }
  
  public Status delete(String table, String key, String nodeURL){
    DeleteRequest requestBody = new DeleteRequest();
    requestBody.setTable(table);
    requestBody.setKey(key);

    // String requestBodyStringed = objectMapper.writeValueAsString(requestBody);
    return sendRequest(nodeURL , "/delete", requestBody, new HashMap<>());
  }

}
