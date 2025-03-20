<!--
  Licensed to the Apache Software Foundation (ASF) under one
  or more contributor license agreements.  See the NOTICE file
  distributed with this work for additional information
  regarding copyright ownership.  The ASF licenses this file
  to you under the Apache License, Version 2.0 (the
  "License"); you may not use this file except in compliance
  with the License.  You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing,
  software distributed under the License is distributed on an
  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  KIND, either express or implied.  See the License for the
  specific language governing permissions and limitations
  under the License.
-->

# Gvfs-fuse

Gvfs-fuse is the Fuse client for Gravitino fileset. It allows users to mount Gravitino filesets to their local file system via Fuse, enabling access to Gravitino fileset files as if they were part of the local file system.

## Features

- Supports mounting S3 filesets
- Supports basic file read and write operations
- Supports directory operations
- Allows file attribute manipulation


## Environment setup and build instructions

### Prerequisites

You need to install the following software before you can build gvfs-fuse:

- [fuse3 and libfuse3-dev](https://www.kernel.org/doc/html/next/filesystems/fuse.html) for Linux systems.
- [macFUSE](https://macfuse.github.io) for macOS systems.
- [Rust](https://www.rust-lang.org) environment for compiling.

### Build process

Navigate to the `client/filesystem-fuse` directory of the Gravitino project and execute the following commands:

```shell
# Code verification
make check

# Code formatting
make fmt

# Building Gvfs-fuse
make
```

After building, the `gvfs-fuse` executable will be located in the `target/debug` directory.

### Testing

Run the following commands to execute tests:

```
# Run tests that do not depend on the S3 or Gravitino environment
make test

# Run tests that depend on the S3 environment
make test-s3

# Run integration tests that depend on the Gravitino fileset environment
make test-fuse-it
```

## Usage Guide

Navigate to the `clients/filesystem-fuse` directory, run the following commands to view
help information or to perform mounting operations:

```shell
# Display help information for gvfs-fuse
target/debug/gvfs-fuse --help

# Display help for the mount command
target/debug/gvfs-fuse mount --help

# Display help for the umount command
target/debug/gvfs-fuse umount --help

#create mount directory
mkdir -p target/gvfs

# Execute the mount command in the foreground
# mount the fileset uri to the local directory target/gvfs. You need to start the gravitino server and create the fileset first
target/debug/gvfs-fuse mount target/gvfs gvfs://fileset/test/c1/s1/fileset1 -c conf/gvfs_fuse.toml -f

# Execute the mount command in the background
# mount the fileset uri to the local directory target/gvfs
target/debug/gvfs-fuse mount target/gvfs gvfs://fileset/test/c1/s1/fileset1 -c conf/gvfs_fuse.toml

# Execute the mount command in the background with debug logging of gvfs_fuse package (this is equivalent to setting environment variable: `RUST_LOG=gvfs_fuse=debug`)
target/debug/gvfs-fuse mount target/gvfs gvfs://fileset/test/c1/s1/fileset1 -c conf/gvfs_fuse.toml -d 1

# You can also specify custom log filter by providing `RUST_LOG` environment variable,
# Note that no matter which `RUST_LOG` is provided, if `-d` is `1`, logging of `gvfs_fuse` will always be `DEBUG` level.
# See [tracing-subscriber](https://docs.rs/tracing-subscriber/latest/tracing_subscriber/filter/struct.EnvFilter.html#directives) documentation for more details
RUST_LOG=info,gvfs_fuse::memory_filesystem=trace target/debug/gvfs-fuse mount target/gvfs gvfs://fileset/test/c1/s1/fileset1 -c conf/gvfs_fuse.toml -d 1

# Execute the umount command
# unmount the fileset from the local directory target/gvfs
target/debug/gvfs-fuse umount target/gvfs
```

The `conf/gvfs_fuse.toml` file is a configuration file that contains the following information:

```toml
# fuse settings
[fuse]
file_mask = 0o600
dir_mask = 0o700
fs_type = "memory"
data_path = "target/gvfs-fuse"

[fuse.properties]

# filesystem settings
[filesystem]
block_size = 8192
```

The `gvfs mount` command starts a FUSE program with the given configuration, using a `MemoryFilesystem` for testing.
You can access the filesystem in the `target/gvfs` directory.

More configration file examples can be found in the `tests/conf` directory.
