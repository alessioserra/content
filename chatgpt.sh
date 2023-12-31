#!/bin/bash

/spark/bin/spark-submit \
  --master spark://spark-master-0.spark-master-service.hyperiot-test.svc.cluster.local:7077 \
  --conf spark.security.credentials.hbase.enabled=false \
  --conf spark.driver.supervise=false \
  --conf spark.executor.memory=1g \
  --conf spark.app.name=DEFAULT.3933 \
  --conf spark.driver.memory=2g \
  --conf spark.executor.pyspark.memory=1g \
  --conf spark.jars.packages=org.apache.spark:spark-avro_2.11:2.4.5,org.apache.hbase:hbase-shaded-client:2.5.3 \
  --packages org.apache.spark:spark-avro_2.12:3.4.0 \
  --py-files hdfs://hadoop-nn-0.hadoop-nn-service.hyperiot-test.svc.cluster.local:8020/spark/jobs/minimum.jar \
  /home/content/prova24.py \
  "1" "111" "hProjectAlgorithmName" \
  '{\"hbaseMasterPort\":\"16000\",\"hbaseMasterHostname\":\"hbase-hmaster-0.hbase-hmaster-service.hyperiot-test.svc.cluster.local\",\"hbaseZookeeperQuorum\":\"zookeeper-stateful-1-0.zookeeper-service-1.hyperiot-test,zookeeper-stateful-2-0.zookeeper-service-2.hyperiot-test,zookeeper-stateful-3-0.zookeeper-service-3.hyperiot-test:2181\",\"fsDefaultFs\":\"hdfs://hadoop-nn-0.hadoop-nn-service.hyperiot-test.svc.cluster.local:8020\",\"hbaseMaster\":\"hbase-hmaster-0.hbase-hmaster-service.hyperiot-test.svc.cluster.local:16000\",\"hbaseRootdir\":\"hdfs://hadoop-nn-0.hadoop-nn-service.hyperiot-test.svc.cluster.local:8020/hbase\",\"hdfsWriteDir\":\"/data/HPacket/\",\"hbaseRegionserverInfoPort\":\"16030\",\"hbaseRegionserverPort\":\"16020\",\"hbaseClusterDistributed\":\"true\",\"hbaseMasterInfoPort\":\"16010\"}' '{\"input\":[{\"packetId\":665,\"mappedInputList\":[{\"packetFieldId\":666,\"algorithmInput\":{\"id\":1,\"name\":\"input\",\"description\":null,\"fieldType\":\"DOUBLE\",\"multiplicity\":\"SINGLE\",\"type\":\"INPUT\"}}]}],\"output\":[{\"id\":2,\"name\":\"output\",\"description\":null,\"fieldType\":\"DOUBLE\",\"multiplicity\":\"SINGLE\",\"type\":\"OUTPUT\"}]}'
