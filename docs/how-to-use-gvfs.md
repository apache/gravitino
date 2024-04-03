---
title: How to use Gravitino Virtual File System
slug: /how-to-use-gvfs
license: "Copyright 2024 Datastrato Pvt Ltd.
This software is licensed under the Apache License version 2."
---

## Introduction

Gravitino supports operating the storage mapping to `fileset` through the Hadoop Compatible File System (HCFS) interface,
which names `Gravitino Virtual File System` (It is called `gvfs` for short).  
You can access the storage mapping to fileset through the following path:
```text
gvfs://fileset/${catalog_name}/${schema_name}/${fileset_name}/sub_dir/
```
The following mainly introduces how to use `Gravitino Virtual File System` to operate fileset which mapping to HDFS.


## Prerequisites

+ Hadoop environment (Hadoop 3.1.0 has been tested)
+ HDFS (Best matched to Hadoop environment)
+ Already created a fileset mapping to HDFS through the Gravitino server


## Mainly Configuration

| Configuration item                                    | Description                                                                                                                                                                                       | Default value | Required | Since version |
|-------------------------------------------------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|---------------|----------|---------------|
| `fs.AbstractFileSystem.gvfs.impl`                     | The Gravitino Virtual File System abstract class. Please configure it as `com.datastrato.gravitino.filesystem.hadoop3.Gvfs`.                                                                      | (none)        | Yes      | 0.5.0         |
| `fs.gvfs.impl`                                        | The Gravitino Virtual File System implementation class. Please configure it as `com.datastrato.gravitino.filesystem.hadoop3.GravitinoVirtualFileSystem`.                                          | (none)        | Yes      | 0.5.0         |
| `fs.gvfs.impl.disable.cache`                          | Close the Gravitino Virtual File System cache in Hadoop environment. If you need to proxy multi-user operations, please set this value to `true` and create a separate File System for each user. | `false`       | No       | 0.5.0         |
| `fs.gravitino.server.uri`                             | The Gravitino server uri which gvfs needs to load the fileset meta.                                                                                                                               | (none)        | Yes      | 0.5.0         |
| `fs.gravitino.client.metalake`                        | The metalake which fileset belongs.                                                                                                                                                               | (none)        | Yes      | 0.5.0         |
| `fs.gravitino.fileset.cache.maxCapacity`              | The cache capacity in the Gravitino Virtual File System.                                                                                                                                          | `20`          | No       | 0.5.0         |
| `fs.gravitino.fileset.cache.evictionMillsAfterAccess` | The value of time that the cache evicts the element after access in the Gravitino Virtual File System. The value is in `milliseconds`.                                                            | `300000`      | No       | 0.5.0         |

You can configure these properties in two ways:  
1. Before getting the `FileSystem` in the code, construct the `Configuration` and set the properties:  
```text
Configuration conf = new Configuration();
conf.set("fs.AbstractFileSystem.gvfs.impl","com.datastrato.gravitino.filesystem.hadoop3.Gvfs");
conf.set("fs.gvfs.impl","com.datastrato.gravitino.filesystem.hadoop3.GravitinoVirtualFileSystem");
conf.set("fs.gravitino.server.uri","http://localhost:8090");
conf.set("fs.gravitino.client.metalake","test_metalake");
Path filesetPath = new Path("gvfs://fileset/test_catalog/test_schema/test_fileset_1");
FileSystem fs = filesetPath.getFileSystem(conf);
```
2. Configure these properties in the `core-site.xml` file of the Hadoop environment:
```text
  <property>
    <name>fs.AbstractFileSystem.gvfs.impl</name>
    <value>com.datastrato.gravitino.filesystem.hadoop3.Gvfs</value>
  </property>

  <property>
    <name>fs.gvfs.impl</name>
    <value>com.datastrato.gravitino.filesystem.hadoop3.GravitinoVirtualFileSystem</value>
  </property>

  <property>
    <name>fs.gravitino.server.uri</name>
    <value>http://localhost:8090</value>
  </property>

  <property>
    <name>fs.gravitino.client.metalake</name>
    <value>test_metalake</value>
  </property>
```

## Usage

Please get the `Gravitino Virtual File System runtime jar` firstly, you can get it in two ways:
1. [Maven repository](https://mvnrepository.com/):  
You can find the runtime jar in the maven central repository, which names like `gravitino-filesystem-hadoop3-runtime-{version}.jar`.
2. Compiles the source code:  
You can also download the [Gravitino source code](https://github.com/datastrato/gravitino), and compile it locally using the following command:
```shell
./${GRAVITINO_SOURCE_CODE_HOME}/gradlew :clients:filesystem-hadoop3-runtime:build -x test
```

### 1. Hadoop shell
You can use the Hadoop shell command to operate the fileset storage. For example:
```shell
# 1. Configure the hadoop `core-site.xml` configuration
# you should put the properties above into this file
vi ${HADOOP_HOME}/etc/hadoop/core-site.xml

# 2. Place the gvfs runtime jar into your Hadoop environment
cp gravitino-filesystem-hadoop3-runtime-{version}.jar ${HADOOP_HOME}/share/hadoop/common/lib/

# 3. Complete the Kerberos authentication of the Hadoop environment.
# You need to ensure that the Kerberos has permission to operate the HDFS directory.
kinit -kt your_kerbers.keytab your_kerberos@xxx.com

# 4. Try to list the fileset
./${HADOOP_HOME}/bin/hadoop dfs -ls gvfs://fileset/test_catalog/test_schema/test_fileset_1
```

### 2. Java code
You can also operate the fileset storage through the java code.  
You need to ensure that your code is running in the correct Hadoop environment, and the environment has the `gravitino-filesystem-hadoop3-runtime-{version}.jar`.  
For example:
```text
Configuration conf = new Configuration();
conf.set("fs.AbstractFileSystem.gvfs.impl","com.datastrato.gravitino.filesystem.hadoop3.Gvfs");
conf.set("fs.gvfs.impl","com.datastrato.gravitino.filesystem.hadoop3.GravitinoVirtualFileSystem");
conf.set("fs.gravitino.server.uri","http://localhost:8090");
conf.set("fs.gravitino.client.metalake","test_metalake");
Path filesetPath = new Path("gvfs://fileset/test_catalog/test_schema/test_fileset_1");
FileSystem fs = filesetPath.getFileSystem(conf);
fs.getFileStatus(filesetPath);
```

### 3. Spark
#### 3.1 Include the runtime jar
You can use `--packages` in the Spark submit shell to include the Gravitino Virtual File System runtime jar like this:
```shell
./${SPARK_HOME}/bin/spark-submit --packages com.datastrato.gravitino:filesystem-hadoop3-runtime:${version}
```
If you want to include Gravitino Virtual File System runtime jar in your Spark installation, add it to `${SPARK_HOME}/jars/` folder.

#### 3.2 Configure the Hadoop configuration when submit the job
Then, you can configure the Hadoop configuration in the submit shell command like this:
```shell
--conf spark.hadoop.fs.AbstractFileSystem.gvfs.impl=com.datastrato.gravitino.filesystem.hadoop3.Gvfs
--conf spark.hadoop.fs.gvfs.impl=com.datastrato.gravitino.filesystem.hadoop3.GravitinoVirtualFileSystem
--conf spark.hadoop.fs.gravitino.server.uri=${your_gravitino_server_uri}
--conf spark.hadoop.fs.gravitino.client.metalake=${your_gravitino_metalake}
```

#### 3.3 Operate the fileset storage in your code
Finally, you can operate the fileset storage in your Spark program:
```scala
// Scala
val spark = SparkSession.builder()
      .appName("Gvfs Example")
      .getOrCreate()

val rdd = spark.sparkContext.textFile("gvfs://fileset/test_catalog/test_schema/test_fileset_1")

rdd.foreach(println)
```

