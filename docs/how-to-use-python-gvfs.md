---
title: How to use Gravitino Virtual File System with Filesets in Python
slug: /how-to-use-python-gvfs
license: "Copyright 2024 Datastrato Pvt Ltd.
This software is licensed under the Apache License version 2."
---

## Introduction

`Fileset` is a concept brought in by Gravitino, which is a logical collection of files and
directories, with `fileset` you can manage non-tabular data through Gravitino. For
details, you can read [How to manage fileset metadata using Gravitino](./manage-fileset-metadata-using-gravitino.md).

To use `Fileset` managed by Gravitino in Python, Gravitino provides a virtual file system layer called
the Gravitino Virtual File System (GVFS) that's built on top of the [fsspec](https://filesystem-spec.readthedocs.io/en/latest/)
interface.

GVFS is a virtual layer that manages the files and directories in the fileset through a virtual
path, without needing to understand the specific storage details of the fileset. You can access
the files or folders as shown below:

```text
gvfs://fileset/${catalog_name}/${schema_name}/${fileset_name}/sub_dir/
or like:
fileset/${catalog_name}/${schema_name}/${fileset_name}/sub_dir/
```

Here `gvfs` is the scheme of the GVFS, `fileset` is the root directory of the GVFS which can't
modified, and `${catalog_name}/${schema_name}/${fileset_name}` is the virtual path of the fileset.
You can access the files and folders under this virtual path by concatenating a file or folder
name to the virtual path.

The usage pattern for GVFS is the same as HDFS or S3. GVFS internally manages
the path mapping and convert automatically.

## Prerequisites

+ A Hadoop environment with HDFS running. Now we only supports Fileset on HDFS.
  GVFS in Python has been tested against Hadoop 2.7.3. It is recommended to use Hadoop 2.7.3 or later,
  it should work with Hadoop 3.x. Please create an [issue](https://www.github.com/datastrato/gravitino/issues)
  if you find any compatibility issues.
+ Python version >= 3.8. It has been tested GVFS works well with Python 3.8 and Python 3.9.
  Your Python version should be at least higher than Python 3.8.

Attention: If you are using macOS or Windows operating system, you need to follow the steps in the
[Hadoop official building documentation](https://github.com/apache/hadoop/blob/trunk/BUILDING.txt)(Need match your Hadoop version)
to recompile the native libraries like `libhdfs` and others, and completely replace the files in `${HADOOP_HOME}/lib/native`.

## Configuration

| Configuration item   | Description                                                                                                               | Default value | Required | Since version |
|----------------------|---------------------------------------------------------------------------------------------------------------------------|---------------|----------|---------------|
| `server_uri`         | The Gravitino server uri, e.g. `http://localhost:8090`.                                                                   | (none)        | Yes      | 0.6.0         |.                                                                                                                | (none)        | Yes                                 | 0.6.0         |
| `metalake_name`      | The metalake name which the fileset belongs to.                                                                           | (none)        | Yes      | 0.6.0         |.                                                                                                                |  (none)        | Yes                                 | 0.6.0         | .                               | (none)        | Yes      | 0.6.0         |
| `cache_size`         | The cache capacity of the Gravitino Virtual File System.                                                                  | `20`          | No       | 0.6.0         |.                                                                                                                |  (none)        | Yes                                 | 0.6.0         | .                               | (none)        | Yes      | 0.6.0         |
| `cache_expired_time` | The value of time that the cache expires after accessing in the Gravitino Virtual File System. The value is in `seconds`. | `3600`        | No       | 0.6.0         |.                                                                                                                |  (none)        | Yes                                 | 0.6.0         | .                               | (none)        | Yes      | 0.6.0         |


You can configure these properties when obtaining the `Gravitino Virtual FileSystem` in Python like this:

```python
from gravitino import gvfs

fs = gvfs.GravitinoVirtualFileSystem(server_uri="http://localhost:8090", metalake_name="test_metalake")
```

## How to use the Gravitino Virtual File System in Python

1. Make sure to obtain the Gravitino library.
   You can get it by [pip](https://pip.pypa.io/en/stable/installation/):

```shell
pip install gravitino
```

2. Configuring the Hadoop environment.
   You should configure Hadoop environments in the system environment:
```shell
# Configure them in Linux
export HADOOP_HOME=${YOUR_HADOOP_PATH}
export HADOOP_CONF_DIR=${YOUR_HADOOP_PATH}/etc/hadoop
export CLASSPATH=`$HADOOP_HOME/bin/hdfs classpath --glob`
```

### Using GVFS's fsspec-style interface

You can use the GVFS fsspec-style interface to perform operations on the fileset storage.

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

### Integrating GVFS with Third-party Python libraries

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
import pandas as pd

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

4. Integrating with [Llama Index](https://docs.llamaindex.ai/en/stable/module_guides/loading/simpledirectoryreader/#support-for-external-filesystems)(0.10.40).
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

