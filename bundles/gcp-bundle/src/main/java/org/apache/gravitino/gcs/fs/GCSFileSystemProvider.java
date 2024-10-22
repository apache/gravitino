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
package org.apache.gravitino.gcs.fs;

import com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem;
import java.io.IOException;
import java.util.Map;
import org.apache.gravitino.catalog.hadoop.fs.FileSystemProvider;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GCSFileSystemProvider implements FileSystemProvider {
  private static final Logger LOGGER = LoggerFactory.getLogger(GCSFileSystemProvider.class);

  @Override
  public FileSystem getFileSystem(Path path, Map<String, String> config) throws IOException {
    Configuration configuration = new Configuration();
    config.forEach(
        (k, v) -> {
          configuration.set(k.replace("gravitino.bypass.", ""), v);
        });

    LOGGER.info("Creating GCS file system with config: {}", config);
    return GoogleHadoopFileSystem.newInstance(path.toUri(), configuration);
  }

  @Override
  public String scheme() {
    return "gs";
  }

  @Override
  public String name() {
    return "gcs";
  }
}
