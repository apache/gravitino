/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.gravitino.catalog.hadoop.fs;

import java.io.IOException;
import java.util.Map;
import javax.annotation.Nonnull;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 * FileSystemProvider is an interface for providing FileSystem instances. It is used by the
 * HadoopCatalog to create FileSystem instances for accessing Hadoop compatible file systems.
 */
public interface FileSystemProvider {

  /**
   * The prefix of the configuration key that should be bypassed when setting the configuration to
   * the FileSystem instance.
   *
   * <p>For example, if the configuration key passed to {@link
   * FileSystemProvider#getFileSystem(Path, Map)} 'gravitino.bypass.fs.s3a.endpoint', the prefix
   * 'gravitino.bypass.' should be removed when setting the configuration to the FileSystem
   * instance.
   *
   * <p>User can use this prefix to pass the configuration item that has not been defined in
   * Gravitino.
   */
  String GRAVITINO_BYPASS = "gravitino.bypass.";

  /**
   * Get the FileSystem instance according to the configuration map and file path.
   *
   * <p>Compared to the {@link FileSystem#get(Configuration)} method, this method allows the
   * provider to create a FileSystem instance with a specific configuration and do further
   * initialization if needed.
   *
   * <p>For example: 1. We can check the endpoint value validity for S3AFileSystem then do further
   * actions. 2. We can also change some default behavior of the FileSystem initialization process
   * 3. More...
   *
   * @param config The configuration for the FileSystem instance.
   * @param path The path to the file system.
   * @return The FileSystem instance.
   * @throws IOException If the FileSystem instance cannot be created.
   */
  FileSystem getFileSystem(@Nonnull Path path, @Nonnull Map<String, String> config)
      throws IOException;

  /**
   * Scheme of this FileSystem provider. The value is 'file' for LocalFileSystem, 'hdfs' for HDFS,
   * etc.
   *
   * @return The scheme of this FileSystem provider used.
   */
  String scheme();

  /**
   * Name of this FileSystem provider. The value is 'builtin-local' for LocalFileSystem,
   * 'builtin-hdfs' for HDFS, etc.
   *
   * @return The name of this FileSystem provider.
   */
  String name();
}
