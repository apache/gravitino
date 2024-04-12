---
title: How to use Gravitino Virtual File System with Filesets
slug: /how-to-use-gvfs
license: "Copyright 2024 Datastrato Pvt Ltd.
This software is licensed under the Apache License version 2."
---

## Introduction

`Fileset` is a concept brought in by Gravitino, which is a logical collection of files and
directories, with `fileset` you can manage the non-tabular data through Gravitino. For the
details you can see [How to manage fileset metadata using Gravitino](./manage-fileset-metadata-using-gravitino.md).

To use `Fileset` managed by Gravitino, Gravitino provides a virtual file system layer called
Gravitino Virtual File System (GVFS) that is built on top of the Hadoop Compatible File System
(HCFS) interface.

GVFS is a virtual layer that manages the files and directories in the fileset through a virtual
path, without needing to understand the specific storage details of the fileset. You can access
the files or folders like below:

```text
gvfs://fileset/${catalog_name}/${schema_name}/${fileset_name}/sub_dir/
```

Here `gvfs` is the scheme of the GVFS, `fileset` is the root directory of the GVFS and cannot be
modified, the `${catalog_name}/${schema_name}/${fileset_name}` is the virtual path of the fileset.
You can access the files and folders under this virtual path by concatenating the file or folder
name to the virtual path.

The usage pattern for GVFS is exactly the same as HDFS, S3 and others, GVFS internally will manage
the path mapping and converting automatically.

## Prerequisites

+ A Hadoop environment (Hadoop 3.1.0 has been tested) with HDFS running. GVFS is built against
  Hadoop 3.1.0, it is recommended to use Hadoop 3.1.0 or later, but it should work with Hadoop 2.
  x. Please create an [issue](https://www.github.com/datastrato/gravitino/issues) if you find any
  compatibility issues.
+ You already manage the filesets with Gravitino.

## Configuration

| Configuration item                                    | Description                                                                                                                                                                                       | Default value | Required                          | Since version |
|-------------------------------------------------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|---------------|-----------------------------------|---------------|
| `fs.AbstractFileSystem.gvfs.impl`                     | The Gravitino Virtual File System abstract class. Please configure it as `com.datastrato.gravitino.filesystem.hadoop.Gvfs`.                                                                       | (none)        | Yes                               | 0.5.0         |
| `fs.gvfs.impl`                                        | The Gravitino Virtual File System implementation class. Please configure it as `com.datastrato.gravitino.filesystem.hadoop.GravitinoVirtualFileSystem`.                                           | (none)        | Yes                               | 0.5.0         |
| `fs.gvfs.impl.disable.cache`                          | Close the Gravitino Virtual File System cache in Hadoop environment. If you need to proxy multi-user operations, please set this value to `true` and create a separate File System for each user. | `false`       | No                                | 0.5.0         |
| `fs.gravitino.server.uri`                             | The Gravitino server uri which gvfs needs to load the fileset meta.                                                                                                                               | (none)        | Yes                               | 0.5.0         |
| `fs.gravitino.client.metalake`                        | The metalake which fileset belongs.                                                                                                                                                               | (none)        | Yes                               | 0.5.0         |
| `fs.gravitino.client.authType`                        | The auth type to initialize the Gravitino client in Gravitino Virtual File System. Now only supports `simple` and `oauth2` auth type.                                                             | `simple`      | No                                | 0.5.0         |
| `fs.gravitino.client.oauth2.serverUri`                | The auth server uri for the Gravitino client which using `oauth2` auth type in Gravitino Virtual File System.                                                                                     | (none)        | Yes if you use `oauth2` auth type | 0.5.0         |
| `fs.gravitino.client.oauth2.credential`               | The auth credential for the Gravitino client which using `oauth2` auth type in Gravitino Virtual File System.                                                                                     | (none)        | Yes if you use `oauth2` auth type | 0.5.0         |
| `fs.gravitino.client.oauth2.path`                     | The auth server path for the Gravitino client which using `oauth2` auth type in Gravitino Virtual File System. Please remove first slash (`/`) for path, which may like `oauth/token`.            | (none)        | Yes if you use `oauth2` auth type | 0.5.0         |
| `fs.gravitino.client.oauth2.scope`                    | The auth scope for the Gravitino client which using `oauth2` auth type in Gravitino Virtual File System.                                                                                          | (none)        | Yes if you use `oauth2` auth type | 0.5.0         |
| `fs.gravitino.fileset.cache.maxCapacity`              | The cache capacity in the Gravitino Virtual File System.                                                                                                                                          | `20`          | No                                | 0.5.0         |
| `fs.gravitino.fileset.cache.evictionMillsAfterAccess` | The value of time that the cache evicts the element after access in the Gravitino Virtual File System. The value is in `milliseconds`.                                                            | `300000`      | No                                | 0.5.0         |

You can configure these properties in two ways:

1. Before getting the `FileSystem` in the code, construct the `Configuration` and set the properties:

    ```java
    Configuration conf = new Configuration();
    conf.set("fs.AbstractFileSystem.gvfs.impl","com.datastrato.gravitino.filesystem.hadoop.Gvfs");
    conf.set("fs.gvfs.impl","com.datastrato.gravitino.filesystem.hadoop.GravitinoVirtualFileSystem");
    conf.set("fs.gravitino.server.uri","http://localhost:8090");
    conf.set("fs.gravitino.client.metalake","test_metalake");
    Path filesetPath = new Path("gvfs://fileset/test_catalog/test_schema/test_fileset_1");
    FileSystem fs = filesetPath.getFileSystem(conf);
    ```

2. Configure these properties in the `core-site.xml` file of the Hadoop environment:

    ```xml
      <property>
        <name>fs.AbstractFileSystem.gvfs.impl</name>
        <value>com.datastrato.gravitino.filesystem.hadoop.Gvfs</value>
      </property>

      <property>
        <name>fs.gvfs.impl</name>
        <value>com.datastrato.gravitino.filesystem.hadoop.GravitinoVirtualFileSystem</value>
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

## How to use

Please make sure to have the Gravitino Virtual File System runtime jar firstly, you can get it in
two ways:

1. Download from the maven central repository: You can download the runtime jar which name is like
   `gravitino-filesystem-hadoop3-runtime-{version}.jar` from [Maven repository](https://mvnrepository.com/).
2. Compile from the source code:

   Download the [Gravitino source code](https://github.com/datastrato/gravitino), and compile it
   locally using the following command in the Gravitino source code directory:

    ```shell
       ./gradlew :clients:filesystem-hadoop3-runtime:build -x test
    ```

### Use GVFS via Hadoop shell command

You can use the Hadoop shell command to operate the fileset storage. For example:

```shell
# 1. Configure the hadoop `core-site.xml` configuration
# you should put the properties above into this file
vi ${HADOOP_HOME}/etc/hadoop/core-site.xml

# 2. Place the gvfs runtime jar into your Hadoop environment
cp gravitino-filesystem-hadoop3-runtime-{version}.jar ${HADOOP_HOME}/share/hadoop/common/lib/

# 3. Complete the Kerberos authentication of the Hadoop environment (if necessary).
# You need to ensure that the Kerberos has permission to operate the HDFS directory.
kinit -kt your_kerberos.keytab your_kerberos@xxx.com

# 4. Try to list the fileset
./${HADOOP_HOME}/bin/hadoop dfs -ls gvfs://fileset/test_catalog/test_schema/test_fileset_1
```

### Use GVFS via Java code

You can also operate the files or directories managed by fileset through Java code.
Please make sure that your code is running in the correct Hadoop environment, and the environment
has the `gravitino-filesystem-hadoop3-runtime-{version}.jar` dependency.

For example:

```java
Configuration conf = new Configuration();
conf.set("fs.AbstractFileSystem.gvfs.impl","com.datastrato.gravitino.filesystem.hadoop.Gvfs");
conf.set("fs.gvfs.impl","com.datastrato.gravitino.filesystem.hadoop.GravitinoVirtualFileSystem");
conf.set("fs.gravitino.server.uri","http://localhost:8090");
conf.set("fs.gravitino.client.metalake","test_metalake");
Path filesetPath = new Path("gvfs://fileset/test_catalog/test_schema/test_fileset_1");
FileSystem fs = filesetPath.getFileSystem(conf);
fs.getFileStatus(filesetPath);
```

### Use GVFS with Apache Spark

1. Add the gvfs runtime jar to the Spark environment.

    You can use `--packages` or `--jars` in the Spark submit shell to include the Gravitino Virtual
    File System runtime jar, like this:

    ```shell
    ./${SPARK_HOME}/bin/spark-submit --packages com.datastrato.gravitino:filesystem-hadoop3-runtime:${version}
    ```

    If you want to include Gravitino Virtual File System runtime jar in your Spark installation,
    make sure to add it to the `${SPARK_HOME}/jars/` folder.

2. Configure the Hadoop configuration when submitting the job.

    Then, you can configure the Hadoop configuration in the shell command like this:

    ```shell
    --conf spark.hadoop.fs.AbstractFileSystem.gvfs.impl=com.datastrato.gravitino.filesystem.hadoop.Gvfs
    --conf spark.hadoop.fs.gvfs.impl=com.datastrato.gravitino.filesystem.hadoop.GravitinoVirtualFileSystem
    --conf spark.hadoop.fs.gravitino.server.uri=${your_gravitino_server_uri}
    --conf spark.hadoop.fs.gravitino.client.metalake=${your_gravitino_metalake}
    ```

3. Operate the fileset storage in your code.

    Finally, you can operate the fileset storage in your Spark program:

    ```scala
    // Scala
    val spark = SparkSession.builder()
          .appName("Gvfs Example")
          .getOrCreate()

    val rdd = spark.sparkContext.textFile("gvfs://fileset/test_catalog/test_schema/test_fileset_1")

    rdd.foreach(println)
    ```


### Use GVFS with Tensorflow

In order for tensorflow to support GVFS, you need to recompile the [tensorflow-io](https://github.com/tensorflow/io) module.

1. Add a patch and recompile tensorflow-io.

    You need to add a [patch](https://github.com/tensorflow/io/pull/1970) to support GVFS on
    tensorflow-io. Then you can follow the [tutorial](https://github.com/tensorflow/io/blob/master/docs/development.md)
    to recompile your code and release the tensorflow-io module.

2. Configure the Hadoop configuration.

   You need to configure the hadoop configuration and `gravitino-filesystem-hadoop3-runtime-{version}.jar`,
   also kerberos environment according to the [Use GVFS via Hadoop shell command](#use-gvfs-via-hadoop-shell-command) sections.

   Then you need to set your environment as follows:

   ```shell
   export HADOOP_HOME=${your_hadoop_home}
   export HADOOP_CONF_DIR=${your_hadoop_conf_home}
   export PATH=$PATH:$HADOOP_HOME/libexec/hadoop-config.sh
   export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:$JAVA_HOME/jre/lib/amd64/server
   export PATH=$PATH:$HADOOP_HOME/bin:$HADOOP_HOME/sbin
   export CLASSPATH="$(hadoop classpath --glob)"
   ```

3. Import tensorflow-io and test.

   ```python
   import tensorflow as tf
   import tensorflow_io as tfio

   ## read a file
   print(tf.io.read_file('gvfs://fileset/test_catalog/test_schema/test_fileset_1/test.txt'))

   ## list directory
   print(tf.io.gfile.listdir('gvfs://fileset/test_catalog/test_schema/test_fileset_1/'))
   ```

## Authentication

Currently, Gravitino Virtual File System supports two kinds of authentication types to access Gravitino server: `simple` and `oauth2`.

The type of `simple` is the default authentication type in Gravitino Virtual File System.

### How to use

#### Use `simple` authentication type

Please make sure that your Gravitino server also configure the `simple` authentication mode firstly.

Then, you can configure the Hadoop configuration like this:

```java
// Simple type allows the client to use the environment variable `GRAVITINO_USER` as the user.
// If the environment variable `GRAVITINO_USER` isn't set,
// the client uses the user of the machine that sends requests.
System.setProperty("GRAVITINO_USER", "test");

Configuration conf = new Configuration();
conf.set("fs.AbstractFileSystem.gvfs.impl","com.datastrato.gravitino.filesystem.hadoop.Gvfs");
conf.set("fs.gvfs.impl","com.datastrato.gravitino.filesystem.hadoop.GravitinoVirtualFileSystem");
conf.set("fs.gravitino.server.uri","http://localhost:8090");
conf.set("fs.gravitino.client.metalake","test_metalake");
// Configure the auth type to simple,
// or do not configure this configuration, gvfs will use simple type as default.
conf.set("fs.gravitino.client.authType", "simple");
Path filesetPath = new Path("gvfs://fileset/test_catalog/test_schema/test_fileset_1");
FileSystem fs = filesetPath.getFileSystem(conf);
```

#### Use `oauth2` authentication type

If you want to use `oauth2` authentication type for the Gravitino client in Gravitino Virtual File System,
please refer to this document to complete the configuration of the Gravitino server and the OAuth server: [Security](./security.md).

Then, you can configure the Hadoop configuration like this:

```java
Configuration conf = new Configuration();
conf.set("fs.AbstractFileSystem.gvfs.impl","com.datastrato.gravitino.filesystem.hadoop.Gvfs");
conf.set("fs.gvfs.impl","com.datastrato.gravitino.filesystem.hadoop.GravitinoVirtualFileSystem");
conf.set("fs.gravitino.server.uri","http://localhost:8090");
conf.set("fs.gravitino.client.metalake","test_metalake");
// Configure the auth type to oatuh2.
conf.set("fs.gravitino.client.authType", "oauth2");
// Configure the oauth conifiguration.
conf.set("fs.gravitino.client.oauth2.serverUri", "${your_oauth_server_uri}");
conf.set("fs.gravitino.client.oauth2.credential", "${your_client_credential}");
conf.set("fs.gravitino.client.oauth2.path", "${your_oauth_server_path}");
conf.set("fs.gravitino.client.oauth2.scope", "${your_client_scope}");
Path filesetPath = new Path("gvfs://fileset/test_catalog/test_schema/test_fileset_1");
FileSystem fs = filesetPath.getFileSystem(conf);
```

