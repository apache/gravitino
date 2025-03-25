---
title: How to use Apache Gravitino Virtual File System with Filesets
slug: /how-to-use-gvfs
license: "This software is licensed under the Apache License version 2."
---

## Introduction

`Fileset` is a concept brought in by Apache Gravitino, which is a logical collection of files and
directories, with `fileset` you can manage non-tabular data through Gravitino. For
details, you can read [How to manage fileset metadata using Gravitino](./manage-fileset-metadata-using-gravitino.md).

To use `fileset` managed by Gravitino, Gravitino provides a virtual file system layer called
the Gravitino Virtual File System (GVFS):
* In Java, it's built on top of the Hadoop Compatible File System(HCFS) interface.
* In Python, it's built on top of the [fsspec](https://filesystem-spec.readthedocs.io/en/stable/index.html)
  interface.

GVFS is a virtual layer that manages the files and directories in the fileset through a virtual
path, without needing to understand the specific storage details of the fileset. You can access
the files or folders as shown below:

```text
gvfs://fileset/${catalog_name}/${schema_name}/${fileset_name}/sub_dir/
```

In python GVFS, you can also access the files or folders as shown below:

```text
fileset/${catalog_name}/${schema_name}/${fileset_name}/sub_dir/
```

Here `gvfs` is the scheme of the GVFS, `fileset` is the root directory of the GVFS which can't
modified, and `${catalog_name}/${schema_name}/${fileset_name}` is the virtual path of the fileset.
You can access the files and folders under this virtual path by concatenating a file or folder
name to the virtual path.

The usage pattern for GVFS is the same as HDFS or S3. GVFS internally manages
the path mapping and convert automatically.

## 1. Managing files of Fileset with Java GVFS

### Prerequisites

 - GVFS has been tested against Hadoop 3.3.1. It is recommended to use Hadoop 3.3.1 or later, but it should work with Hadoop 2.
  x. Please create an [issue](https://www.github.com/apache/gravitino/issues) if you find any
  compatibility issues.

### Configuration

| Configuration item                                    | Description                                                                                                                                                                                             | Default value | Required                            | Since version   |
|-------------------------------------------------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|---------------|-------------------------------------|-----------------|
| `fs.AbstractFileSystem.gvfs.impl`                     | The Gravitino Virtual File System abstract class, set it to `org.apache.gravitino.filesystem.hadoop.Gvfs`.                                                                                              | (none)        | Yes                                 | 0.5.0           |
| `fs.gvfs.impl`                                        | The Gravitino Virtual File System implementation class, set it to `org.apache.gravitino.filesystem.hadoop.GravitinoVirtualFileSystem`.                                                                  | (none)        | Yes                                 | 0.5.0           |
| `fs.gvfs.impl.disable.cache`                          | Disable the Gravitino Virtual File System cache in the Hadoop environment. If you need to proxy multi-user operations, please set this value to `true` and create a separate File System for each user. | `false`       | No                                  | 0.5.0           |
| `fs.gravitino.server.uri`                             | The Gravitino server URI which GVFS needs to load the fileset metadata.                                                                                                                                 | (none)        | Yes                                 | 0.5.0           |
| `fs.gravitino.client.metalake`                        | The metalake to which the fileset belongs.                                                                                                                                                              | (none)        | Yes                                 | 0.5.0           |
| `fs.gravitino.client.authType`                        | The auth type to initialize the Gravitino client to use with the Gravitino Virtual File System. Currently only supports `simple`, `oauth2` and `kerberos` auth types.                                   | `simple`      | No                                  | 0.5.0           |
| `fs.gravitino.client.oauth2.serverUri`                | The auth server URI for the Gravitino client when using `oauth2` auth type with the Gravitino Virtual File System.                                                                                      | (none)        | Yes if you use `oauth2` auth type   | 0.5.0           |
| `fs.gravitino.client.oauth2.credential`               | The auth credential for the Gravitino client when using `oauth2` auth type in the Gravitino Virtual File System.                                                                                        | (none)        | Yes if you use `oauth2` auth type   | 0.5.0           |
| `fs.gravitino.client.oauth2.path`                     | The auth server path for the Gravitino client when using `oauth2` auth type with the Gravitino Virtual File System. Please remove the first slash `/` from the path, for example `oauth/token`.         | (none)        | Yes if you use `oauth2` auth type   | 0.5.0           |
| `fs.gravitino.client.oauth2.scope`                    | The auth scope for the Gravitino client when using `oauth2` auth type with the Gravitino Virtual File System.                                                                                           | (none)        | Yes if you use `oauth2` auth type   | 0.5.0           |
| `fs.gravitino.client.kerberos.principal`              | The auth principal for the Gravitino client when using `kerberos` auth type with the Gravitino Virtual File System.                                                                                     | (none)        | Yes if you use `kerberos` auth type | 0.5.1           |
| `fs.gravitino.client.kerberos.keytabFilePath`         | The auth keytab file path for the Gravitino client when using `kerberos` auth type in the Gravitino Virtual File System.                                                                                | (none)        | No                                  | 0.5.1           |
| `fs.gravitino.fileset.cache.maxCapacity`              | The cache capacity of the Gravitino Virtual File System.                                                                                                                                                | `20`          | No                                  | 0.5.0           |
| `fs.gravitino.fileset.cache.evictionMillsAfterAccess` | The value of time that the cache expires after accessing in the Gravitino Virtual File System. The value is in `milliseconds`.                                                                          | `3600000`     | No                                  | 0.5.0           |
| `fs.gravitino.fileset.cache.evictionMillsAfterAccess` | The value of time that the cache expires after accessing in the Gravitino Virtual File System. The value is in `milliseconds`.                                                                          | `3600000`     | No                                  | 0.5.0           |

Apart from the above properties, to access fileset like S3, GCS, OSS and custom fileset, extra properties are needed, please see 
[S3 GVFS Java client configurations](./hadoop-catalog-with-s3.md#using-the-gvfs-java-client-to-access-the-fileset), [GCS GVFS Java client configurations](./hadoop-catalog-with-gcs.md#using-the-gvfs-java-client-to-access-the-fileset), [OSS GVFS Java client configurations](./hadoop-catalog-with-oss.md#using-the-gvfs-java-client-to-access-the-fileset) and [Azure Blob Storage GVFS Java client configurations](./hadoop-catalog-with-adls.md#using-the-gvfs-java-client-to-access-the-fileset) for more details.

#### Custom fileset 
Since 0.7.0-incubating, users can define their own fileset type and configure the corresponding properties, for more, please refer to [Custom Fileset](./hadoop-catalog.md#how-to-custom-your-own-hcfs-file-system-fileset).
So, if you want to access the custom fileset through GVFS, you need to configure the corresponding properties.

| Configuration item             | Description                                                                                             | Default value | Required | Since version    |
|--------------------------------|---------------------------------------------------------------------------------------------------------|---------------|----------|------------------|
| `your-custom-properties`       | The properties will be used to create a FileSystem instance in `CustomFileSystemProvider#getFileSystem` | (none)        | No       | -                |

You can configure these properties in two ways:

1. Before obtaining the `FileSystem` in the code, construct a `Configuration` object and set its properties:

    ```java
    Configuration conf = new Configuration();
    conf.set("fs.AbstractFileSystem.gvfs.impl","org.apache.gravitino.filesystem.hadoop.Gvfs");
    conf.set("fs.gvfs.impl","org.apache.gravitino.filesystem.hadoop.GravitinoVirtualFileSystem");
    conf.set("fs.gravitino.server.uri","http://localhost:8090");
    conf.set("fs.gravitino.client.metalake","test_metalake");
    Path filesetPath = new Path("gvfs://fileset/test_catalog/test_schema/test_fileset_1");
    FileSystem fs = filesetPath.getFileSystem(conf);
    ```
   
2. Configure the properties in the `core-site.xml` file of the Hadoop environment:

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

First make sure to obtain the Gravitino Virtual File System runtime jar, which you can get in
two ways:

1. Download from the maven central repository. You can download the runtime jar named
   `gravitino-filesystem-hadoop3-runtime-{version}.jar` from [Maven repository](https://mvnrepository.com/).

2. Compile from the source code:

   Download or clone the [Gravitino source code](https://github.com/apache/gravitino), and compile it
   locally using the following command in the Gravitino source code directory:

    ```shell
       ./gradlew :clients:filesystem-hadoop3-runtime:build -x test
    ```

#### Via Hadoop shell command

You can use the Hadoop shell command to perform operations on the fileset storage. For example:

```shell
# 1. Configure the hadoop `core-site.xml` configuration
# You should put the required properties into this file
vi ${HADOOP_HOME}/etc/hadoop/core-site.xml

# 2. Place the GVFS runtime jar into your Hadoop environment
cp gravitino-filesystem-hadoop3-runtime-{version}.jar ${HADOOP_HOME}/share/hadoop/common/lib/

# 3. Complete the Kerberos authentication setup of the Hadoop environment (if necessary).
# You need to ensure that the Kerberos has permission on the HDFS directory.
kinit -kt your_kerberos.keytab your_kerberos@xxx.com

# 4. Try to list the fileset
./${HADOOP_HOME}/bin/hadoop dfs -ls gvfs://fileset/test_catalog/test_schema/test_fileset_1
```

#### Via Java code

You can also perform operations on the files or directories managed by fileset through Java code.
Make sure that your code is using the correct Hadoop environment, and that your environment
has the `gravitino-filesystem-hadoop3-runtime-{version}.jar` dependency.

For example:

```java
Configuration conf = new Configuration();
conf.set("fs.AbstractFileSystem.gvfs.impl","org.apache.gravitino.filesystem.hadoop.Gvfs");
conf.set("fs.gvfs.impl","org.apache.gravitino.filesystem.hadoop.GravitinoVirtualFileSystem");
conf.set("fs.gravitino.server.uri","http://localhost:8090");
conf.set("fs.gravitino.client.metalake","test_metalake");
Path filesetPath = new Path("gvfs://fileset/test_catalog/test_schema/test_fileset_1");
FileSystem fs = filesetPath.getFileSystem(conf);
fs.getFileStatus(filesetPath);
```

#### Via Apache Spark

1. Add the GVFS runtime jar to the Spark environment.

   You can use `--packages` or `--jars` in the Spark submit shell to include the Gravitino Virtual
   File System runtime jar, like so:

    ```shell
    ./${SPARK_HOME}/bin/spark-submit --packages org.apache.gravitino:filesystem-hadoop3-runtime:${version}
    ```

   If you want to include the Gravitino Virtual File System runtime jar in your Spark installation, add it to the `${SPARK_HOME}/jars/` folder.

2. Configure the Hadoop configuration when submitting the job.

   You can configure in the shell command in this way:

    ```shell
    --conf spark.hadoop.fs.AbstractFileSystem.gvfs.impl=org.apache.gravitino.filesystem.hadoop.Gvfs
    --conf spark.hadoop.fs.gvfs.impl=org.apache.gravitino.filesystem.hadoop.GravitinoVirtualFileSystem
    --conf spark.hadoop.fs.gravitino.server.uri=${your_gravitino_server_uri}
    --conf spark.hadoop.fs.gravitino.client.metalake=${your_gravitino_metalake}
    ```

3. Perform operations on the fileset storage in your code.

   Finally, you can access the fileset storage in your Spark program:

    ```scala
    // Scala code
    val spark = SparkSession.builder()
          .appName("Gvfs Example")
          .getOrCreate()

    val rdd = spark.sparkContext.textFile("gvfs://fileset/test_catalog/test_schema/test_fileset_1")

    rdd.foreach(println)
    ```

#### Via Tensorflow

For Tensorflow to support GVFS, you need to recompile the [tensorflow-io](https://github.com/tensorflow/io) module.

1. First, add a patch and recompile tensorflow-io.

   You need to add a [patch](https://github.com/tensorflow/io/pull/1970) to support GVFS on
   tensorflow-io. Then you can follow the [tutorial](https://github.com/tensorflow/io/blob/master/docs/development.md)
   to recompile your code and release the tensorflow-io module.

2. Then you need to configure the Hadoop configuration.

   You need to configure the Hadoop configuration and add `gravitino-filesystem-hadoop3-runtime-{version}.jar`,
   and set up the Kerberos environment according to the [Use GVFS via Hadoop shell command](#use-gvfs-via-hadoop-shell-command) sections.

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

### Authentication

Currently, Gravitino Virtual File System supports two kinds of authentication types to access Gravitino server: `simple` and `oauth2`.

The type of `simple` is the default authentication type in Gravitino Virtual File System.

#### How to use authentication

##### Using `simple` authentication

First, make sure that your Gravitino server is also configured to use the `simple` authentication mode.

Then, you can configure the Hadoop configuration like this:

```java
// Simple type uses the environment variable `GRAVITINO_USER` as the client user.
// If the environment variable `GRAVITINO_USER` isn't set,
// the client uses the user of the machine that sends requests.
System.setProperty("GRAVITINO_USER", "test");

Configuration conf = new Configuration();
conf.set("fs.AbstractFileSystem.gvfs.impl","org.apache.gravitino.filesystem.hadoop.Gvfs");
conf.set("fs.gvfs.impl","org.apache.gravitino.filesystem.hadoop.GravitinoVirtualFileSystem");
conf.set("fs.gravitino.server.uri","http://localhost:8090");
conf.set("fs.gravitino.client.metalake","test_metalake");
// Configure the auth type to simple,
// or do not configure this configuration, gvfs will use simple type as default.
conf.set("fs.gravitino.client.authType", "simple");
Path filesetPath = new Path("gvfs://fileset/test_catalog/test_schema/test_fileset_1");
FileSystem fs = filesetPath.getFileSystem(conf);
```

##### Using `OAuth` authentication

If you want to use `oauth2` authentication for the Gravitino client in the Gravitino Virtual File System,
please refer to this document to complete the configuration of the Gravitino server and the OAuth server: [Security](security/security.md).

Then, you can configure the Hadoop configuration like this:

```java
Configuration conf = new Configuration();
conf.set("fs.AbstractFileSystem.gvfs.impl","org.apache.gravitino.filesystem.hadoop.Gvfs");
conf.set("fs.gvfs.impl","org.apache.gravitino.filesystem.hadoop.GravitinoVirtualFileSystem");
conf.set("fs.gravitino.server.uri","http://localhost:8090");
conf.set("fs.gravitino.client.metalake","test_metalake");
// Configure the auth type to oauth2.
conf.set("fs.gravitino.client.authType", "oauth2");
// Configure the OAuth configuration.
conf.set("fs.gravitino.client.oauth2.serverUri", "${your_oauth_server_uri}");
conf.set("fs.gravitino.client.oauth2.credential", "${your_client_credential}");
conf.set("fs.gravitino.client.oauth2.path", "${your_oauth_server_path}");
conf.set("fs.gravitino.client.oauth2.scope", "${your_client_scope}");
Path filesetPath = new Path("gvfs://fileset/test_catalog/test_schema/test_fileset_1");
FileSystem fs = filesetPath.getFileSystem(conf);
```

##### Using `Kerberos` authentication

If you want to use `kerberos` authentication for the Gravitino client in the Gravitino Virtual File System,
please refer to this document to complete the configuration of the Gravitino server: [Security](security/security.md).

Then, you can configure the Hadoop configuration like this:

```java
Configuration conf = new Configuration();
conf.set("fs.AbstractFileSystem.gvfs.impl","org.apache.gravitino.filesystem.hadoop.Gvfs");
conf.set("fs.gvfs.impl","org.apache.gravitino.filesystem.hadoop.GravitinoVirtualFileSystem");
conf.set("fs.gravitino.server.uri","http://localhost:8090");
conf.set("fs.gravitino.client.metalake","test_metalake");
// Configure the auth type to kerberos.
conf.set("fs.gravitino.client.authType", "kerberos");
// Configure the Kerberos configuration.
conf.set("fs.gravitino.client.kerberos.principal", "${your_kerberos_principal}");
// Optional. You don't need to set the keytab if you use kerberos ticket cache.
conf.set("fs.gravitino.client.kerberos.keytabFilePath", "${your_kerberos_keytab}");
Path filesetPath = new Path("gvfs://fileset/test_catalog/test_schema/test_fileset_1");
FileSystem fs = filesetPath.getFileSystem(conf);
```

## 2. Managing files of Fileset with Python GVFS

### Prerequisites

+ A Hadoop environment with HDFS or other Hadoop Compatible File System (HCFS) implementations like S3, GCS, etc. GVFS has been tested against Hadoop 3.3.1. It is recommended to use Hadoop 3.3.1 or later, but it should work with Hadoop 2.x. Please create an [issue](https://www.github.com/apache/gravitino/issues)
  if you find any compatibility issues.
+ Python version >= 3.8. It has been tested GVFS works well with Python 3.8 and Python 3.9.
  Your Python version should be at least higher than Python 3.8.

Attention: If you are using macOS or Windows operating system, you need to follow the steps in the
[Hadoop official building documentation](https://github.com/apache/hadoop/blob/trunk/BUILDING.txt)(Need match your Hadoop version)
to recompile the native libraries like `libhdfs` and others, and completely replace the files in `${HADOOP_HOME}/lib/native`.

### Configuration

| Configuration item            | Description                                                                                                                                                                                                                                                                                        | Default value | Required                          | Since version    |
|-------------------------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|---------------|-----------------------------------|------------------|
| `server_uri`                  | The Gravitino server uri, e.g. `http://localhost:8090`.                                                                                                                                                                                                                                            | (none)        | Yes                               | 0.6.0-incubating |
| `metalake_name`               | The metalake name which the fileset belongs to.                                                                                                                                                                                                                                                    | (none)        | Yes                               | 0.6.0-incubating |
| `cache_size`                  | The cache capacity of the Gravitino Virtual File System.                                                                                                                                                                                                                                           | `20`          | No                                | 0.6.0-incubating |                                                                                                                      
| `cache_expired_time`          | The value of time that the cache expires after accessing in the Gravitino Virtual File System. The value is in `seconds`.                                                                                                                                                                          | `3600`        | No                                | 0.6.0-incubating |
| `auth_type`                   | The auth type to initialize the Gravitino client to use with the Gravitino Virtual File System. Currently supports `simple` and `oauth2` auth types.                                                                                                                                               | `simple`      | No                                | 0.6.0-incubating |
| `oauth2_server_uri`           | The auth server URI for the Gravitino client when using `oauth2` auth type.                                                                                                                                                                                                                        | (none)        | Yes if you use `oauth2` auth type | 0.7.0-incubating |
| `oauth2_credential`           | The auth credential for the Gravitino client when using `oauth2` auth type.                                                                                                                                                                                                                        | (none)        | Yes if you use `oauth2` auth type | 0.7.0-incubating |
| `oauth2_path`                 | The auth server path for the Gravitino client when using `oauth2` auth type. Please remove the first slash `/` from the path, for example `oauth/token`.                                                                                                                                           | (none)        | Yes if you use `oauth2` auth type | 0.7.0-incubating |
| `oauth2_scope`                | The auth scope for the Gravitino client when using `oauth2` auth type with the Gravitino Virtual File System.                                                                                                                                                                                      | (none)        | Yes if you use `oauth2` auth type | 0.7.0-incubating |
| `credential_expiration_ratio` | The ratio of expiration time for credential from Gravitino. This is used in the cases where Gravitino Hadoop catalogs have enable credential vending. if the expiration time of credential fetched from Gravitino is 1 hour, GVFS client will try to refresh the credential in 1 * 0.9 = 0.5 hour. | 0.5           | No                                | 0.8.0-incubating |

#### Configurations for S3, GCS, OSS and Azure Blob storage fileset

Please see the cloud-storage-specific configurations [GCS GVFS Java client configurations](./hadoop-catalog-with-gcs.md#using-the-gvfs-python-client-to-access-a-fileset), [S3 GVFS Java client configurations](./hadoop-catalog-with-s3.md#using-the-gvfs-python-client-to-access-a-fileset), [OSS GVFS Java client configurations](./hadoop-catalog-with-oss.md#using-the-gvfs-python-client-to-access-a-fileset) and [Azure Blob Storage GVFS Java client configurations](./hadoop-catalog-with-adls.md#using-the-gvfs-python-client-to-access-a-fileset) for more details.

:::note
Gravitino python client does not support [customized file systems](hadoop-catalog.md#how-to-custom-your-own-hcfs-file-system-fileset) defined by users due to the limit of `fsspec` library. 
:::

### Usage examples

1. Make sure to obtain the Gravitino library.
   You can get it by [pip](https://pip.pypa.io/en/stable/installation/):

    ```shell
    pip install apache-gravitino
    ```

2. Configuring the Hadoop environment.
   You should ensure that the Python client has Kerberos authentication information and
   configure Hadoop environments in the system environment:

    ```shell
    # kinit kerberos
    kinit -kt /tmp/xxx.keytab xxx@HADOOP.COM
    # Or you can configure kerberos information in the Hadoop `core-site.xml` file
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
    # Configure Hadoop env in Linux
    export HADOOP_HOME=${YOUR_HADOOP_PATH}
    export HADOOP_CONF_DIR=${YOUR_HADOOP_PATH}/etc/hadoop
    export CLASSPATH=`$HADOOP_HOME/bin/hdfs classpath --glob`
    ```

#### Via fsspec-style interface

You can use the fsspec-style interface to perform operations on the fileset files.

For example:

```python
from gravitino import gvfs

# init the gvfs
fs = gvfs.GravitinoVirtualFileSystem(server_uri="http://localhost:8090", metalake_name="test_metalake")

# list file infos under the fileset
fs.ls(path="gvfs://fileset/fileset_catalog/tmp/tmp_fileset/sub_dir")

# get file info under the fileset
fs.info(path="gvfs://fileset/fileset_catalog/tmp/tmp_fileset/sub_dir/test.parquet")

# check a file or a diretory whether exists
fs.exists(path="gvfs://fileset/fileset_catalog/tmp/tmp_fileset/sub_dir")

# write something into a file
with fs.open(path="gvfs://fileset/fileset_catalog/tmp/tmp_fileset/sub_dir/test.txt", mode="wb") as output_stream:
    output_stream.write(b"hello world")

# append something into a file
with fs.open(path="gvfs://fileset/fileset_catalog/tmp/tmp_fileset/sub_dir/test.txt", mode="ab") as append_stream:
    append_stream.write(b"hello world")

# read something from a file
with fs.open(path="gvfs://fileset/fileset_catalog/tmp/tmp_fileset/sub_dir/test.txt", mode="rb") as input_stream:
    input_stream.read()

# copy a file
fs.cp_file(path1="gvfs://fileset/fileset_catalog/tmp/tmp_fileset/sub_dir/test.txt",
           path2="gvfs://fileset/fileset_catalog/tmp/tmp_fileset/sub_dir/test-1.txt")

# delete a file
fs.rm_file(path="gvfs://fileset/fileset_catalog/tmp/tmp_fileset/ttt/test-1.txt")

# two methods to create a directory
fs.makedirs(path="gvfs://fileset/fileset_catalog/tmp/tmp_fileset/sub_dir_2")

fs.mkdir(path="gvfs://fileset/fileset_catalog/tmp/tmp_fileset/sub_dir_3")

# delete a file or a directory recursively
fs.rm(path="gvfs://fileset/fileset_catalog/tmp/tmp_fileset/sub_dir_2", recursive=True)

# delete a directory
fs.rmdir(path="gvfs://fileset/fileset_catalog/tmp/tmp_fileset/sub_dir_2")

# move a file or a directory
fs.mv(path1="gvfs://fileset/fileset_catalog/tmp/tmp_fileset/test-1.txt",
      path2="gvfs://fileset/fileset_catalog/tmp/tmp_fileset/sub_dir/test-2.txt")

# get the content of a file
fs.cat_file(path="gvfs://fileset/fileset_catalog/tmp/tmp_fileset/test-1.txt")

# copy a remote file to local
fs.get_file(rpath="gvfs://fileset/fileset_catalog/tmp/tmp_fileset/test-1.txt",
            lpath="/tmp/local-file-1.txt")
```

#### Integrating with Third-party Python libraries

You can also perform operations on the files or directories managed by fileset
integrating with some Third-party Python libraries which support fsspec compatible filesystems.

For example:
1. Integrating with [Pandas](https://pandas.pydata.org/docs/reference/io.html)(2.0.3).

```python
from gravitino import gvfs
import pandas as pd

data = pd.DataFrame({'Name': ['A', 'B', 'C', 'D'], 'ID': [20, 21, 19, 18]})
storage_options = {'server_uri': 'http://localhost:8090', 'metalake_name': 'test_metalake'}
# save data to a parquet file under the fileset
data.to_parquet('gvfs://fileset/fileset_catalog/tmp/tmp_fileset/test.parquet', storage_options=storage_options)

# read data from a parquet file under the fileset
ds = pd.read_parquet(path="gvfs://fileset/fileset_catalog/tmp/tmp_fileset/test.parquet",
                     storage_options=storage_options)
print(ds)

# save data to a csv file under the fileset
data.to_csv('gvfs://fileset/fileset_catalog/tmp/tmp_fileset/test.csv', storage_options=storage_options)

# save data from a csv file under the fileset
df = pd.read_csv('gvfs://fileset/fileset_catalog/tmp/tmp_fileset/test.csv', storage_options=storage_options)
print(df)
```

2. Integrating with [PyArrow](https://arrow.apache.org/docs/python/filesystems.html)(15.0.2).

```python
from gravitino import gvfs
import pyarrow.dataset as dt
import pyarrow.parquet as pq

fs = gvfs.GravitinoVirtualFileSystem(
    server_uri="http://localhost:8090", metalake_name="test_metalake"
)

# read a parquet file as arrow dataset
arrow_dataset = dt.dataset("gvfs://fileset/fileset_catalog/tmp/tmp_fileset/test.parquet", filesystem=fs)

# read a parquet file as arrow parquet table
arrow_table = pq.read_table("gvfs://fileset/fileset_catalog/tmp/tmp_fileset/test.parquet", filesystem=fs)
```

3. Integrating with [Ray](https://docs.ray.io/en/latest/data/loading-data.html#loading-data)(2.10.0).

```python
from gravitino import gvfs
import ray

fs = gvfs.GravitinoVirtualFileSystem(
    server_uri="http://localhost:8090", metalake_name="test_metalake"
)

# read a parquet file as ray dataset
ds = ray.data.read_parquet("gvfs://fileset/fileset_catalog/tmp/tmp_fileset/test.parquet",fs)
```

4. Integrating with [LlamaIndex](https://docs.llamaindex.ai/en/stable/module_guides/loading/simpledirectoryreader/#support-for-external-filesystems)(0.10.40).

```python
from gravitino import gvfs
from llama_index.core import SimpleDirectoryReader

fs = gvfs.GravitinoVirtualFileSystem(server_uri=server_uri, metalake_name=metalake_name)

# read all document files like csv files under the fileset sub dir
reader = SimpleDirectoryReader(
    input_dir='fileset/fileset_catalog/tmp/tmp_fileset/sub_dir',
    fs=fs,
    recursive=True,  # recursively searches all subdirectories
)
documents = reader.load_data()
print(documents)
```

### Authentication

Currently, Gravitino Virtual File System in Python supports two kinds of authentication types to access Gravitino server: `simple` and `oauth2`.

The type of `simple` is the default authentication type in Gravitino Virtual File System in Python.

#### How to use authentication

##### Using `simple` authentication

First, make sure that your Gravitino server is also configured to use the `simple` authentication mode.

Then, you can configure the authentication like this:

```python
from gravitino import gvfs

options = {"auth_type": "simple"}
fs = gvfs.GravitinoVirtualFileSystem(server_uri="http://localhost:8090", metalake_name="test_metalake", options=options)
print(fs.ls("gvfs://fileset/fileset_catlaog/tmp/test_fileset"))
```

##### Using `OAuth` authentication

First, make sure that your Gravitino server is also configured to use the `oauth2` authentication mode,
and you have an OAuth server to fetch the token: [Security](security/security.md).

Then, you can configure the authentication like this:

```python
from gravitino import gvfs

options = {
    GVFSConfig.AUTH_TYPE: GVFSConfig.OAUTH2_AUTH_TYPE,
    GVFSConfig.OAUTH2_SERVER_URI: "http://127.0.0.1:1082",
    GVFSConfig.OAUTH2_CREDENTIAL: "xx:xx",
    GVFSConfig.OAUTH2_SCOPE: "test",
    GVFSConfig.OAUTH2_PATH: "token/test",
}
fs = gvfs.GravitinoVirtualFileSystem(server_uri="http://localhost:8090", metalake_name="test_metalake", options=options)
print(fs.ls("gvfs://fileset/fileset_catlaog/tmp/test_fileset"))
```
