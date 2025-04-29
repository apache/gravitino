---
title: Using Apache Gravitino Virtual File System for Filesets
slug: /how-to-use-gvfs
license: "This software is licensed under the Apache License version 2."
---

## Introduction

*Fileset* in Apache Gravitinois a conceptual, logical collection of files and directories,
In Gravitino, you can manage non-tabular data with filesets.
For more details, refer to [managing fileset using Gravitino](../../../metadata/fileset.md).

Gravitino provides a virtual file system layer called the Gravitino Virtual File System (GVFS),
for managing filesets.

* In Java, it's built on top of the Hadoop Compatible File System(HCFS) interface.
* In Python, it's built on top of the [fsspec](https://filesystem-spec.readthedocs.io/en/stable/index.html) interface.

GVFS is a virtual layer that manages the files and directories in the fileset through a virtual path,
without needing to understand the specific storage details of the fileset.
You can access the files or folders as shown below:

```text
gvfs://fileset/{catalog}/{schema}/{fileset}/sub_dir/
```

In python, you can also access the files or folders as shown below:

```text
fileset/{catalog}/{schema}/{fileset}/sub_dir/
```

where

- `gvfs`: the scheme of the GVFS.
- `fileset`: the root directory of the GVFS; it is immutable.
- `{catalog}/{schema}/{fileset}`: the virtual path of the fileset.

You can access the files and folders under this virtual path
by concatenating a file or folder name to the virtual path.

The usage pattern for GVFS is the same as HDFS or S3.
GVFS internally manages the path mapping and convert automatically.

## Managing Fileset with Java GVFS

- GVFS has been tested against Hadoop 3.3.1.
  It is recommended to use Hadoop 3.3.1 or later, but it should work with Hadoop 2.x.
  Please create an [issue](https://www.github.com/apache/gravitino/issues)
  if you find any compatibility issues.

### Java GVFS Configuration

<table>
<thead>
<tr>
  <th>Configuration item</th>
  <th>Description</th>
  <th>Default value</th>
  <th>Required</th>
  <th>Since version</th>
</tr>
</thead>
<tbody>
<tr>
  <td><tt>fs.AbstractFileSystem.gvfs.impl</tt></td>
  <td>
    The Gravitino Virtual File System (GVFS) abstract class.
    Set it to `org.apache.gravitino.filesystem.hadoop.Gvfs`.
  </td>
  <td>(none)</td>
  <td>Yes</td>
  <td>0.5.0</td>
</tr>
<tr>
  <td><tt>fs.gvfs.impl</tt></td>
  <td>
    The Gravitino Virtual File System implementation class.
    Set it to `org.apache.gravitino.filesystem.hadoop.GravitinoVirtualFileSystem`.
  </td>
  <td>(none)</td>
  <td>Yes</td>
  <td>0.5.0</td>
</tr>
<tr>
  <td><tt>fs.gvfs.impl.disable.cache</tt></td>
  <td>
    Disable the Gravitino Virtual File System cache in the Hadoop environment.
    If you need to proxy multi-user operations, you can set this value to `true`
    and create a separate File System for each user.
  </td>
  <td>`false`</td>
  <td>No</td>
  <td>0.5.0</td>
</tr>
<tr>
  <td><tt>fs.gravitino.server.uri</tt></td>
  <td>
    The Gravitino server URI from which GVFS loads the fileset metadata.
  </td>
  <td>(none)</td>
  <td>Yes</td>
  <td>0.5.0</td>
</tr>
<tr>
  <td><tt>fs.gravitino.client.metalake</tt></td>
  <td>The metalake to which the fileset belongs.</td>
  <td>(none)</td>
  <td>Yes</td>
  <td>0.5.0</td>
</tr>
<tr>
  <td><tt>fs.gravitino.client.authType</tt></td>
  <td>
    The authentication type for initializing the Gravitino client
    to use the Gravitino Virtual File System.
    Currently only supports `simple`, `oauth2` and `kerberos` authentication types.
  </td>
  <td>`simple`</td>
  <td>No</td>
  <td>0.5.0</td>
</tr>
<tr>
  <td><tt>fs.gravitino.client.oauth2.serverUri</tt></td>
  <td>
    The authentication server URI for the Gravitino client
    when using the `oauth2` for the Gravitino Virtual File System.

    This field is required if `oauth2` is used, or else it is optional.
  </td>
  <td>(none)</td>
  <td>Yes|No</td>
  <td>0.5.0</td>
</tr>
<tr>
  <td><tt>fs.gravitino.client.oauth2.credential</tt></td>
  <td>
    The authentication credential for the Gravitino client
    when using `oauth2` for the Gravitino Virtual File System.

    This field is required if `oauth2` is used.
  </td>
  <td>(none)</td>
  <td>Yes|No</td>
  <td>0.5.0</td>
</tr>
<tr>
  <td><tt>fs.gravitino.client.oauth2.path</tt></td>
  <td>
    The authentication server path for the Gravitino client
    when using `oauth2` for the Gravitino Virtual File System.
    Please remove the first slash `/` from the path, for example `oauth/token`.

    This field is required if `oauth2` is used.
  </td>
  <td>(none)</td>
  <td>Yes|No</td>
  <td>0.5.0</td>
</tr>
<tr>
  <td><tt>fs.gravitino.client.oauth2.scope</tt></td>
  <td>
    The authentication scope for the Gravitino client
    when using `oauth2` with the Gravitino Virtual File System.
  </td>
  <td>(none)</td>
  <td>Yes|No</td>
  <td>0.5.0</td>
</tr>
<tr>
  <td><tt>fs.gravitino.client.kerberos.principal</tt></td>
  <td>
    The authentication principal for the Gravitino client
    when using `kerberos` for authentication against the Gravitino Virtual File System.

    If the `kerberos` authentication type is used, then this field is required.
  </td>
  <td>(none)</td>
  <td>Yes|No</td>
  <td>0.5.1</td>
</tr>
<tr>
  <td><tt>fs.gravitino.client.kerberos.keytabFilePath</tt></td>
  <td>
    The authentication keytab file path for the Gravitino client
    when using `kerberos` for authentication against the Gravitino Virtual File System.
  </td>
  <td>(none)</td>
  <td>No</td>
  <td>0.5.1</td>
</tr>
<tr>
  <td><tt>fs.gravitino.fileset.cache.maxCapacity</tt></td>
  <td>The cache capacity of the Gravitino Virtual File System.</td>
  <td>`20`</td>
  <td>No</td>
  <td>0.5.0</td>
</tr>
<tr>
  <td><tt>fs.gravitino.fileset.cache.evictionMillsAfterAccess</tt></td>
  <td>
    The value of time that the cache expires after accessing in the Gravitino Virtual File System.

    The value is in milliseconds.
  </td>
  <td>`3600000`</td>
  <td>No</td>
  <td>0.5.0</td>
</tr>
<tr>
  <td><tt>fs.gravitino.current.location.name</tt></td>
  <td>
    The configuration used to select the location of the fileset.
    If this configuration is not set, the value of environment variable configured by
    `fs.gravitino.current.location.env.var` will be checked.
    If neither is set, the value of fileset property `default-location-name` will be used as the location name.
  </td>
  <td>the value of fileset property `default-location-name`</td>
  <td>No</td>
  <td>0.9.0-incubating</td>
</tr>
<tr>
  <td><tt>fs.gravitino.current.location.name.env.var</tt></td>
  <td>The environment variable name to get the current location name.</td>
  <td>`CURRENT_LOCATION_NAME`</td>
  <td>No</td>
  <td>0.9.0-incubating</td>
</tr>
<tr>
  <td><tt>fs.gravitino.operations.class</tt></td>
  <td>
    The operations class that provides the FS operations for the Gravitino Virtual File System.
    Users can extend the `BaseGVFSOperations` interface to implement their own operations,
    and then set the class name using this configuration item to use their custom FS operations.
  </td>
  <td>`org.apache.gravitino.filesystem.hadoop.DefaultGVFSOperations`</td>
  <td>No</td>
  <td>`0.9.0-incubating`</td>
</tr>
</tbody>
</table>

In addition to the above properties, to access fileset like S3, GCS, OSS and custom fileset,
some extra properties are needed. For more information, please see 

- [S3 GVFS Java client configurations](../hadoop/s3.md#using-the-gvfs-java-client-to-access-the-fileset)
- [GCS GVFS Java client configurations](../hadoop/gcs.md#using-the-gvfs-java-client-to-access-the-fileset)
- [OSS GVFS Java client configurations](../hadoop/oss.md#using-the-gvfs-java-client-to-access-the-fileset)
- [Azure Blob Storage GVFS Java client configurations](../hadoop/adls.md#using-the-gvfs-java-client-to-access-the-fileset)

#### Custom fileset 

Since *0.7.0-incubating*, users can define their own fileset type and configure the corresponding properties.
For more details, please refer to [Custom Fileset](../hadoop/hadoop-catalog.md#how-to-custom-your-own-hcfs-file-system-fileset).
If you want to access the custom fileset through GVFS, you need to configure the corresponding properties.

<table>
<thead>
<tr>
  <td>Configuration item</td>
  <td>Description</td>
  <td>Default value</td>
  <td>Required</td>
  <td>Since version</td>
</tr>
</thead>
<tbody>
<tr>
  <td><tt>your-custom-properties</tt></td>
  <td>
    The properties to be used to create a FileSystem instance
    in `CustomFileSystemProvider#getFileSystem`
  </td>
  <td>(none)</td>
  <td>No</td>
  <td></td>
</tr>
</tbody>
</table>
 
You can configure these properties in two ways:

1. Before obtaining the `FileSystem` in the code, construct a `Configuration` object and set its properties:

   ```java
   Configuration conf = new Configuration();
   conf.set("fs.AbstractFileSystem.gvfs.impl", "org.apache.gravitino.filesystem.hadoop.Gvfs");
   conf.set("fs.gvfs.impl", "org.apache.gravitino.filesystem.hadoop.GravitinoVirtualFileSystem");
   conf.set("fs.gravitino.server.uri", "http://localhost:8090");
   conf.set("fs.gravitino.client.metalake", "mymetalake");
   Path filesetPath = new Path("gvfs://fileset/mycatalog/myschema/my_fileset_1");
   FileSystem fs = filesetPath.getFileSystem(conf);
   ```

1. Configure the properties in the `core-site.xml` file for the Hadoop environment:

   ```xml
   <property>
     <name>fs.AbstractFileSystem.gvfs.impl</name>
     <value>org.apache.gravitino.filesystem.hadoop.Gvfs</value>
   </property>

   <property>
     <name>fs.gvfs.impl</name>
     <value>org.apache.gravitino.filesystem.hadoop.GravitinoVirtualFileSystem</value>
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

### Usage examples

The firs step is to get the Gravitino Virtual File System runtime JAR file.
You can get the file by:

1. Download from the maven central repository. You can download the runtime JAR
   named `gravitino-filesystem-hadoop3-runtime-{version}.jar`
   from the [Maven repository](https://mvnrepository.com/).

1. Compile from the source code:

   Download or clone the [Gravitino repository](https://github.com/apache/gravitino),
   and compile it locally using the following command in the Gravitino directory:

   ```shell
   ./gradlew :clients:filesystem-hadoop3-runtime:build -x test
   ```

#### Access using Hadoop shell command

You can use the Hadoop shell command to perform operations on the fileset storage.
For example:

1. Configure the hadoop `core-site.xml` configuration
   You should put the required properties into this file.

   ```shell
   vi ${HADOOP_HOME}/etc/hadoop/core-site.xml
   ```

1. Place the GVFS runtime JAR into your Hadoop environment. For example:

   ```shell
   cp gravitino-filesystem-hadoop3-runtime-{version}.jar ${HADOOP_HOME}/share/hadoop/common/lib/
   ```

1. Complete the Kerberos authentication setup of the Hadoop environment (if needed).
   You need to ensure that the Kerberos has permission on the HDFS directory.

   ```shell
   kinit -kt your_kerberos.keytab your_kerberos@xxx.com
   ```

1. Verify the setup by trying to  list the filesets:

   ```shel
   ./${HADOOP_HOME}/bin/hadoop dfs -ls gvfs://fileset/test_catalog/test_schema/test_fileset_1
   ```

#### Access in Java code

You can also perform operations on the files or directories managed by fileset through Java code.
Make sure that your code is using the correct Hadoop environment,
and that your environment has the `gravitino-filesystem-hadoop3-runtime-{version}.jar` dependency.

For example:

```java
Configuration conf = new Configuration();
conf.set("fs.AbstractFileSystem.gvfs.impl", "org.apache.gravitino.filesystem.hadoop.Gvfs");
conf.set("fs.gvfs.impl", "org.apache.gravitino.filesystem.hadoop.GravitinoVirtualFileSystem");
conf.set("fs.gravitino.server.uri", "http://localhost:8090");
conf.set("fs.gravitino.client.metalake", "mymetalake");
Path filesetPath = new Path("gvfs://fileset/mycatalog/myschema/my_fileset_1");
FileSystem fs = filesetPath.getFileSystem(conf);
fs.getFileStatus(filesetPath);
```

#### Access using Apache Spark

1. Add the GVFS runtime JAR to the Spark environment.

   You can use `--packages` or `--jars` in the Spark submit shell
   to include the Gravitino Virtual File System runtime JAR.
   For example:

   ```shell
   ./${SPARK_HOME}/bin/spark-submit --packages org.apache.gravitino:filesystem-hadoop3-runtime:${version}
   ```

   If you want to include the Gravitino Virtual File System runtime JAR in your Spark installation,
   you can add it to the `${SPARK_HOME}/jars/` folder.

1. Configure the Hadoop configuration when submitting the job.
   You can configure in the shell command in this way:

   ```shell
   --conf spark.hadoop.fs.AbstractFileSystem.gvfs.impl=org.apache.gravitino.filesystem.hadoop.Gvfs
   --conf spark.hadoop.fs.gvfs.impl=org.apache.gravitino.filesystem.hadoop.GravitinoVirtualFileSystem
   --conf spark.hadoop.fs.gravitino.server.uri=${your_gravitino_server_uri}
   --conf spark.hadoop.fs.gravitino.client.metalake=${your_gravitino_metalake}
   ```

1. Access the fileset storage in your Spark program:

   ```scala
   // Scala code
   val spark = SparkSession.builder()
         .appName("Gvfs Example")
         .getOrCreate()

   val rdd = spark.sparkContext.textFile("gvfs://fileset/mycatalog/myschema/my_fileset_1")

   rdd.foreach(println)
   ```

#### Access from Tensorflow

For Tensorflow to support GVFS, you need to recompile the [tensorflow-io](https://github.com/tensorflow/io) module.

1. Apply a patch and recompile "tensorflow-io".

   You need to add a [patch](https://github.com/tensorflow/io/pull/1970) to support GVFS on "tensorflow-io".
   You can follow the [tutorial](https://github.com/tensorflow/io/blob/master/docs/development.md)
   to recompile your code and release the tensorflow-io module.

1. Set up the Hadoop configuration.

   You need to set up the Hadoop configuration and add `gravitino-filesystem-hadoop3-runtime-{version}.jar`.
   You can then set up the Kerberos environment. The process is like those from the 
   [Access using Hadoop shell command](#access-using-hadoop-shell-command) section.

   ```shell
   export HADOOP_HOME=${your_hadoop_home}
   export HADOOP_CONF_DIR=${your_hadoop_conf_home}
   export PATH=$PATH:$HADOOP_HOME/libexec/hadoop-config.sh
   export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:$JAVA_HOME/jre/lib/amd64/server
   export PATH=$PATH:$HADOOP_HOME/bin:$HADOOP_HOME/sbin
   export CLASSPATH="$(hadoop classpath --glob)"
   ```

1. Import "tensorflow-io" and test.

   ```python
   import tensorflow as tf
   import tensorflow_io as tfio

   ## read a file
   print(tf.io.read_file('gvfs://fileset/mycatalog/myschema/my_fileset_1/test.txt'))

   ## list directory
   print(tf.io.gfile.listdir('gvfs://fileset/mycatalog/myschema/my_fileset_1/'))
   ```

### Authentication

Currently, Gravitino Virtual File System supports two kinds of authentication types
when accessing the Gravitino server: `simple` and `oauth2`.
The type of `simple` is the default authentication type in Gravitino Virtual File System.
See [authentication documentation](../../../security/authentication.md) for more details.

#### Using `simple` authentication

If  your Gravitino server is configured to use the `simple` authentication mode,
you can configure the Hadoop configuration like this:

```java
// Simple type uses the environment variable `GRAVITINO_USER` as the client user.
// If the environment variable `GRAVITINO_USER` isn't set,
// the client uses the user of the machine that sends requests.
System.setProperty("GRAVITINO_USER", "test");

Configuration conf = new Configuration();
conf.set("fs.AbstractFileSystem.gvfs.impl", "org.apache.gravitino.filesystem.hadoop.Gvfs");
conf.set("fs.gvfs.impl", "org.apache.gravitino.filesystem.hadoop.GravitinoVirtualFileSystem");
conf.set("fs.gravitino.server.uri", "http://localhost:8090");
conf.set("fs.gravitino.client.metalake", "mymetalake");

// Configure the auth type to simple.
// If this configuration omitted, GVFS will use 'simple' by default.
conf.set("fs.gravitino.client.authType", "simple");
Path filesetPath = new Path("gvfs://fileset/mycatalog/myschema/my_fileset_1");
FileSystem fs = filesetPath.getFileSystem(conf);
```

#### Using OAuth authentication

If you want to use `oauth2` authentication for the Gravitino client
to authenticate against the the Gravitino Virtual File System,
refer to the [security guide](../../../security/index.md) for configuring the Gravitino server
and the OAuth server.
After having configured the server, you can set up the Hadoop configuration like this:

```java
Configuration conf = new Configuration();
conf.set("fs.AbstractFileSystem.gvfs.impl","org.apache.gravitino.filesystem.hadoop.Gvfs");
conf.set("fs.gvfs.impl","org.apache.gravitino.filesystem.hadoop.GravitinoVirtualFileSystem");
conf.set("fs.gravitino.server.uri","http://localhost:8090");
conf.set("fs.gravitino.client.metalake","mymetalake");

// Set the auth type to oauth2.
conf.set("fs.gravitino.client.authType", "oauth2");

// Configure the OAuth configuration.
conf.set("fs.gravitino.client.oauth2.serverUri", "${your_oauth_server_uri}");
conf.set("fs.gravitino.client.oauth2.credential", "${your_client_credential}");
conf.set("fs.gravitino.client.oauth2.path", "${your_oauth_server_path}");
conf.set("fs.gravitino.client.oauth2.scope", "${your_client_scope}");

Path filesetPath = new Path("gvfs://fileset/mycatalog/myschema/my_fileset_1");
FileSystem fs = filesetPath.getFileSystem(conf);
```

#### Using Kerberos authentication

If you want to use `kerberos` authentication for the Gravitino client
to authenticate against the Gravitino Virtual File System,
refer to the [security guide](../../../security/index.md) for details
on configuring the Gravitino server.
After having prepared the server environment, you can setup the Hadoop configuration like this:

```java
Configuration conf = new Configuration();
conf.set("fs.AbstractFileSystem.gvfs.impl","org.apache.gravitino.filesystem.hadoop.Gvfs");
conf.set("fs.gvfs.impl","org.apache.gravitino.filesystem.hadoop.GravitinoVirtualFileSystem");
conf.set("fs.gravitino.server.uri","http://localhost:8090");
conf.set("fs.gravitino.client.metalake","mymetalake");

// Set the auth type to kerberos.
conf.set("fs.gravitino.client.authType", "kerberos");
// Configure the Kerberos configuration.
conf.set("fs.gravitino.client.kerberos.principal", "${your_kerberos_principal}");
// Optional. You don't need to set the keytab if you use kerberos ticket cache.
conf.set("fs.gravitino.client.kerberos.keytabFilePath", "${your_kerberos_keytab}");

Path filesetPath = new Path("gvfs://fileset/mycatalog/myschema/my_fileset_1");
FileSystem fs = filesetPath.getFileSystem(conf);
```

## Managing files of Fileset with Python GVFS

### Prerequisites

- A Hadoop environment with HDFS or other Hadoop Compatible File System (HCFS) implementations like S3, GCS, etc.
  GVFS has been tested against Hadoop 3.3.1.
  It is recommended to use Hadoop 3.3.1 or later, but it should work with Hadoop 2.x.
  Please create an [issue](https://www.github.com/apache/gravitino/issues) if you find any compatibility issues.

- Python version >= 3.8. It has been tested GVFS works well with Python 3.8 and Python 3.9.
  Your Python version should be at least higher than Python 3.8.

:::warning
If you are using macOS or Windows operating system, you need to follow
[the Hadoop official building documentation](https://github.com/apache/hadoop/blob/trunk/BUILDING.txt)
to recompile the native libraries like `libhdfs` and others.
Then you need to completely replace the files in `${HADOOP_HOME}/lib/native`.

### Python GVFS Configuration

<table>
<thead>
<tr>
  <th>Configuration item</th>
  <th>Description</th>
  <th>Default value</th>
  <th>Required</th>
  <th>Since version</th>
</tr>
</thead>
<tbody>
<tr>
  <td><tt>server_uri</tt></td>
  <td>The Gravitino server URI, e.g. `http://localhost:8090`.</td>
  <td>(none)</td>
  <td>Yes</td>
  <td>`0.6.0-incubating`</td>
</tr>
<tr>
  <td><tt>metalake_name</tt></td>
  <td>The metalake name which the fileset belongs to.</td>
  <td>(none)</td>
  <td>Yes</td>
  <td>`0.6.0-incubating`</td>
</tr>
<tr>
  <td><tt>cache_size</tt></td>
  <td>The cache capacity of the Gravitino Virtual File System.</td>
  <td>`20`</td>
  <td>No</td>
  <td>`0.6.0-incubating`</td>
</tr>
<tr>
  <td><tt>cache_expired_time</tt></td>
  <td>
    The value of time that the cache expires after accessing in the Gravitino Virtual File System.
    The value is in *seconds*.
  </td>
  <td>`3600`</td>
  <td>No</td>
  <td>`0.6.0-incubating`</td>
</tr>
<tr>
  <td><tt>auth_type</tt></td>
  <td>
    The authentication type to initialize the Gravitino client
    to authenticate against the Gravitino Virtual File System.
    Currently supports `simple` and `oauth2` authentication types.
  </td>
  <td>`simple`</td>
  <td>No</td>
  <td>`0.6.0-incubating`</td>
</tr>
<tr>
  <td><tt>oauth2_server_uri</tt></td>
  <td>
    The authentication server URI for the Gravitino client
    when using the `oauth2` auth type.

    This field is required if `oauth2` authentication is used.
  </td>
  <td>(none)</td>
  <td>Yes|No</td>
  <td>`0.7.0-incubating`</td>
</tr>
<tr>
  <td><tt>oauth2_credential</tt></td>
  <td>
    The authentication credential for the Gravitino client
    when using `oauth2` auth type.

    This field is required if `oauth2` authentication is used.
  </td>
  <td>(none)</td>
  <td>Yes|No</td>
  <td>`0.7.0-incubating`</td>
</tr>
<tr>
  <td><tt>oauth2_path</tt></td>
  <td>
    The authentication server path for the Gravitino client
    when using `oauth2` auth type.
    Please remove the leading slash `/` from the path.

    This field is required if `oauth2` authentication is used.
  </td>
  <td>(none)</td>
  <td>Yes|No</td>
  <td>`0.7.0-incubating`</td>
</tr>
<tr>
  <td><tt>oauth2_scope</tt></td>
  <td>
    The authentication scope for the Gravitino client
    when using `oauth2` for authentication against the Gravitino Virtual File System.

    This field is required if `oauth2` authentication is used.
  </td>
  <td>(none)</td>
  <td>Yes|No</td>
  <td>`0.7.0-incubating`</td>
</tr>
<tr>
  <td><tt>credential_expiration_ratio</tt></td>
  <td>
    The ratio of expiration time for credential from Gravitino.
    This is used in the cases where Gravitino Hadoop catalogs have enabled credential vending.
    If the expiration time of credential fetched from Gravitino is 1 hour,
    GVFS client will try to refresh the credential in 1 * 0.9 = 0.5 hour.
  </td>
  <td>`0.5`</td>
  <td>No</td>
  <td>`0.8.0-incubating`</td>
</tr>
<tr>
  <td><tt>current_location_name</tt></td>
  <td>
    The configuration used to select the location of the fileset.
    If this configuration is not set, the value of environment variable configured by
    `current_location_name_env_var` will be checked.
    If neither is set, the value of fileset property `default-location-name` will be used as the location name.
  </td>
  <td>the value of fileset property `default-location-name`</td>
  <td>No</td>
  <td>`0.9.0-incubating`</td>
</tr>
<tr>
  <td><tt>current_location_name_env_var</tt></td>
  <td>The environment variable name to get the current location name.</td>
  <td>`CURRENT_LOCATION_NAME`</td>
  <td>No</td>
  <td>`0.9.0-incubating`</td>
</tr>
<tr>
  <td><tt>operations_class</tt></td>
  <td>
    The operations class that provides the FS operations for the Gravitino Virtual File System.
    Users can extend the `BaseGVFSOperations` interface to implement their own operations,
    and then set the class name using this configuration item to use their custom FS operations.
  </td>
  <td>`gravitino.filesystem.gvfs_default_operations.DefaultGVFSOperations`</td>
  <td>No</td>
  <td>`0.9.0-incubating`</td>
</tr>
</tbody>
</table>

### Configurations for S3, GCS, OSS and Azure Blob storage fileset

For more details, please see the cloud-storage-specific configurations
- [GCS GVFS Java client configurations](../hadoop/gcs.md#using-the-gvfs-python-client-to-access-a-fileset)
- [S3 GVFS Java client configurations](../hadoop/s3.md#using-the-gvfs-python-client-to-access-a-fileset)
- [OSS GVFS Java client configurations](../hadoop/oss.md#using-the-gvfs-python-client-to-access-a-fileset)
- [Azure Blob Storage GVFS Java client configurations](../hadoop/adls.md#using-the-gvfs-python-client-to-access-a-fileset) 

:::note
Gravitino python client does not support user defined
[customized file systems](../hadoop/hadoop-catalog.md#how-to-custom-your-own-hcfs-file-system-fileset)
due to the limit of `fsspec` library. 
:::

### Usage examples

1. Install the Gravitino library using [pip](https://pip.pypa.io/en/stable/installation/):

   ```shell
   pip install apache-gravitino
   ```

1. Configuring the Hadoop environment.

   You should ensure that the Python client has Kerberos authentication information and
   configure Hadoop environments in the system environment:

   ```shell
   # kinit kerberos
   kinit -kt /tmp/xxx.keytab xxx@HADOOP.COM
   ```

   Or you can configure kerberos information in the Hadoop `core-site.xml` file:

   ```xml
   <property>
     <name>hadoop.security.authentication</name>
     <value>kerberos</value>
   </property>

   <property>
     <name>hadoop.client.kerberos.principal</name>
     <value>xxx@HADOOP.COM</value>
   </property>

   <property>
     <name>hadoop.client.keytab.file</name>
     <value>/tmp/xxx.keytab</value>
   </property>
   ```
   
   You can also Configure Hadoop environment variables in Linux:

   ```shell
   export HADOOP_HOME=${YOUR_HADOOP_PATH}
   export HADOOP_CONF_DIR=${YOUR_HADOOP_PATH}/etc/hadoop
   export CLASSPATH=`$HADOOP_HOME/bin/hdfs classpath --glob`
   ```

### Access using the fsspec-style interface

You can use the fsspec-style interface to perform operations on the fileset files.

For example:

```python
from gravitino import gvfs

# init the gvfs
fs = gvfs.GravitinoVirtualFileSystem(
        server_uri="http://localhost:8090",
         metalake_name="mymetalake")

fileset = "gvfs://fileset/mycatalog/myschema/my_fileset"

# list file infos under the fileset
fs.ls(path=fileset + "/sub_dir")

# get file info under the fileset
fs.info(path=fileset + "/sub_dir/test.parquet")

# check a file or a directory whether exists
fs.exists(path=fileset + "/sub_dir")

# write something into a file
with fs.open(path=fileset + /sub_dir/test.txt", mode="wb") as output_stream:
    output_stream.write(b"hello world")

# append something into a file
with fs.open(path=fileset + "/sub_dir/test.txt", mode="ab") as append_stream:
    append_stream.write(b"hello world")

# read something from a file
with fs.open(path=fileset + "/sub_dir/test.txt", mode="rb") as input_stream:
    input_stream.read()

# copy a file
fs.cp_file(path1=fileset + "/sub_dir/test.txt",
           path2=fileset + "/sub_dir/test-1.txt")

# delete a file
fs.rm_file(path=fileset + "/sub_dir/test-1.txt")

# two methods to create a directory
fs.makedirs(path=fileset + "/sub_dir_2")
fs.mkdir(path=fileset + "/sub_dir_3")

# delete a file or a directory recursively
fs.rm(path=fileset + "/sub_dir_2", recursive=True)

# delete a directory
fs.rmdir(path=fileset + "/sub_dir_2")

# move a file or a directory
fs.mv(path1=fileset + "/test-1.txt",
      path2=fileset + "/sub_dir/test-2.txt")

# get the content of a file
fs.cat_file(path=fileset + "/test-1.txt")

# copy a remote file to local
fs.get_file(rpath=fileset + "/test-1.txt",
            lpath="/tmp/local-file-1.txt")
```

#### Integrating with Third-party Python libraries

You can also perform operations on the files or directories
managed by fileset integrating with some Third-party Python libraries
which support "fsspec" compatible filesystems.

For example:

1. Integrating with [Pandas](https://pandas.pydata.org/docs/reference/io.html)(2.0.3).

   ```python
   from gravitino import gvfs
   import pandas as pd

   fileset = "gvfs://fileset/mycatalog/myschema/myfileset 
   data = pd.DataFrame({'Name': ['A', 'B', 'C', 'D'], 'ID': [20, 21, 19, 18]})
   storage_options = {
      'server_uri': 'http://localhost:8090',
      'metalake_name': 'mymetalake'
   }

   # save data to a parquet file under the fileset
   data.to_parquet(fileset + '/test.parquet',
                   storage_options=storage_options)

   # read data from a parquet file under the fileset
   ds = pd.read_parquet(path=fileset + "/test.parquet",
                        storage_options=storage_options)
   print(ds)

   # save data to a csv file under the fileset
   data.to_csv(fileset + "/test.csv', storage_options=storage_options)

   # save data from a csv file under the fileset
   df = pd.read_csv(fileset + "/test.csv', storage_options=storage_options)
   print(df)
   ```

1. Integrating with [PyArrow](https://arrow.apache.org/docs/python/filesystems.html)(15.0.2).

   ```python
   from gravitino import gvfs
   import pyarrow.dataset as dt
   import pyarrow.parquet as pq

   fs = gvfs.GravitinoVirtualFileSystem(
       server_uri="http://localhost:8090", metalake_name="test_metalake"
   )
   fileset = "gvfs://fileset/mycatalog/myschema/myfileset"

   # read a parquet file as arrow dataset
   arrow_dataset = dt.dataset(fileset + "/test.parquet", filesystem=fs)

   # read a parquet file as arrow parquet table
   arrow_table = pq.read_table(fileset + "/test.parquet", filesystem=fs)
   ```

1. Integrating with [Ray](https://docs.ray.io/en/latest/data/loading-data.html#loading-data)(2.10.0).

   ```python
   from gravitino import gvfs
   import ray

   fs = gvfs.GravitinoVirtualFileSystem(
       server_uri="http://localhost:8090", metalake_name="test_metalake"
   )
   fileset = "gvfs://fileset/mycatalog/myschema/myfileset"

   # read a parquet file as ray dataset
   ds = ray.data.read_parquet(fileset + "/test.parquet",fs)
   ```

1. Integrating with [LlamaIndex](https://docs.llamaindex.ai/en/stable/module_guides/loading/simpledirectoryreader/#support-for-external-filesystems)(0.10.40).

   ```python
   from gravitino import gvfs
   from llama_index.core import SimpleDirectoryReader

   fs = gvfs.GravitinoVirtualFileSystem(server_uri=server_uri, metalake_name=metalake_name)
   fileset = "gvfs://fileset/mycatalog/myschema/myfileset"

   # read all document files like csv files under the fileset sub dir
   reader = SimpleDirectoryReader(
       input_dir=fileset + "/sub_dir',
       fs=fs,
       recursive=True,  # recursively searches all subdirectories
   )
   documents = reader.load_data()
   print(documents)
   ```

### Authentication

Currently, Gravitino Virtual File System in Python supports two kinds of authentication types
for accessing Gravitino server: `simple` and `oauth2`.

The type of `simple` is the default authentication type in Gravitino Virtual File System in Python.

#### Using `simple` authentication

If your Gravitino server is configured to use the `simple` authentication mode.
You can configure the authentication like this:

```python
from gravitino import gvfs

options = {"auth_type": "simple"}
fs = gvfs.GravitinoVirtualFileSystem(
    server_uri="http://localhost:8090",
    metalake_name="mymetalake",
    options=options)
print(fs.ls("gvfs://fileset/mycatalog/myschema/myfileset"))
```

##### Using `OAuth` authentication

If your Gravitino server is configured to use the `oauth2` authentication mode,
and you have an OAuth server to fetch the token,
you can configure the authentication like this:

```python
from gravitino import gvfs

options = {
    GVFSConfig.AUTH_TYPE: GVFSConfig.OAUTH2_AUTH_TYPE,
    GVFSConfig.OAUTH2_SERVER_URI: "http://127.0.0.1:1082",
    GVFSConfig.OAUTH2_CREDENTIAL: "xx:xx",
    GVFSConfig.OAUTH2_SCOPE: "test",
    GVFSConfig.OAUTH2_PATH: "token/test",
}
fs = gvfs.GravitinoVirtualFileSystem(
    server_uri="http://localhost:8090",
    metalake_name="mymetalake",
    options=options)
print(fs.ls("gvfs://fileset/mycatalog/myschema/myfileset"))
```

