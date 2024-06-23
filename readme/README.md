
# Experiment on distributed RocksDB via YCSB
This code is our project for big data system course, which teached by
[Shimin Chen](https://acs.ict.ac.cn/english/people_acs_en/researcher_acs_en/202302/t20230201_126786.html)

## Intro
Since RocksDB is an embedded database, we build up middle layer between YCSB and 
RocksDB for network communication. Our whole project is shown as followed:

![](./imgs/rocksdb_framework.png)

We use sharding policy to distribute key by its hash code. When YCSB need to `read`(or `insert`, `delete` and `update`) key hashed to local node, it will access the rocksdb directly. And when the key is hash to remote node, it will forward request to SpringBoot Client, which will send HTTP request to remote SpringBoot Server to get the result. SpringBoot server has a simple rocksdb driver and operates rocksdb based on the HTTP request and it will respond results via HTTP.

## how to use

0. Install RocksDB

The default version we use is v6.2.2, but latest version *maybe* still works.

1. YCSB need to know every node ip address. 

We add some class in YCSB. In `ycsb/rocksdb/src/main/java/site/ycsb/db/rocksdb/RocksDBClient.java`, modify the path of `nodeURL.txt`, which contains the node ip address for every node. The address in `nodeURL.txt` is separated by lines and the first line is the local address.

2. start the SpringBoot server before launch YCSB test

You can use `make run` to quickly launch the SringBoot server. It will choose `/tmp/rocksdb_tmp` as default storage location.

3. Start YCSB test

After compelete all requirements above, then you can start the YCSB test