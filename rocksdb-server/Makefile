target:=rocksdb-server-1.0.0.jar


run:
	rm -rf /tmp/rocksdb_tmp # 每次都需要删除一下
	mvn package
	java -jar ./target/$(target) > log.txt