# YCSB 分布式测试 RocksDB

此项目代码为陈老师的大数据系统与大数据分析课程大作业

## 简介

由于rocksdb是内嵌型数据库，没有网络交互，因此我们在YCSB与rocksDB中间实现了一个简易的网络层，用于YCSB与远程节点之间的网络交互。项目结构示意图如下所示：

![](./imgs/rocksdb_framework.png)

我们使用Sharding的方式基于key的hash值散列数据。当上层YCSB需要访问数据时，会根据key的hash值访问本地还是远程节点。当访问远程节点时，YCSB会将请求参数传给SpringBoot Client。Spring Client将请求打包成Json格式通过HTTP请求发送给远程节点。SpringBoot Server根据请求内容操作RocksDB并将结果返回。

## 如何使用

0. 安装RocksDB

默认使用v6.2.2版本，但最新版*也许*也可以用

1. 将每个节点的ip地址添加到`nodeURL.txt`里

`ycsb/rocksdb/src/main/java/site/ycsb/db/rocksdb/RocksDBClient.java`默认从`/root/nodeURL.txt` 读取每个节点的ip地址。该文件按行分隔，第一行为本地节点的ip地址

2. 使用YCSB前先启动SpringBoot Server

可以在server文件夹里直接使用 `make run`，它默认使用`/tmp/rocksdb_tmp` 存放数据库文件。

3. 开始测试

完成前面要求后，就可以测试了