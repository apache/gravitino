/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */
package org.apache.gravitino.catalog.hadoop.fs;

import static org.apache.gravitino.catalog.hadoop.fs.Constants.BUILTIN_HDFS_FS_PROVIDER;
import static org.apache.gravitino.catalog.hadoop.fs.Constants.BUILTIN_LOCAL_FS_PROVIDER;

import java.io.IOException;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class LocalFileSystemProvider implements FileSystemProvider {

  @Override
  public FileSystem getFileSystem(Path path, Map<String, String> config) throws IOException {
    Configuration configuration = new Configuration();
    config.forEach(
        (k, v) -> {
          configuration.set(k.replace(BUILTIN_HDFS_FS_PROVIDER, ""), v);
        });

    return FileSystem.newInstance(path.toUri(), configuration);
  }

  @Override
  public String scheme() {
    return "file";
  }

  @Override
  public String name() {
    return BUILTIN_LOCAL_FS_PROVIDER;
  }
}
