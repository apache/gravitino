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

package org.apache.gravitino.s3.fs;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import java.io.IOException;
import java.util.Map;
import org.apache.gravitino.catalog.hadoop.fs.FileSystemProvider;
import org.apache.gravitino.catalog.hadoop.fs.FileSystemUtils;
import org.apache.gravitino.storage.S3Properties;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.s3a.Constants;
import org.apache.hadoop.fs.s3a.S3AFileSystem;

public class S3FileSystemProvider implements FileSystemProvider {

  @VisibleForTesting
  public static final Map<String, String> GRAVITINO_KEY_TO_S3_HADOOP_KEY =
      ImmutableMap.of(
          S3Properties.GRAVITINO_S3_ENDPOINT, Constants.ENDPOINT,
          S3Properties.GRAVITINO_S3_ACCESS_KEY_ID, Constants.ACCESS_KEY,
          S3Properties.GRAVITINO_S3_SECRET_ACCESS_KEY, Constants.SECRET_KEY);

  @Override
  public FileSystem getFileSystem(Path path, Map<String, String> config) throws IOException {
    Configuration configuration = new Configuration();
    Map<String, String> hadoopConfMap =
        FileSystemUtils.toHadoopConfigMap(config, GRAVITINO_KEY_TO_S3_HADOOP_KEY);

    if (!hadoopConfMap.containsKey(Constants.AWS_CREDENTIALS_PROVIDER)) {
      configuration.set(
          Constants.AWS_CREDENTIALS_PROVIDER, Constants.ASSUMED_ROLE_CREDENTIALS_DEFAULT);
    }
    hadoopConfMap.forEach(configuration::set);
    return S3AFileSystem.newInstance(path.toUri(), configuration);
  }

  /**
   * Get the scheme of the FileSystem. Attention, for S3 the schema is "s3a", not "s3". Users should
   * use "s3a://..." to access S3.
   */
  @Override
  public String scheme() {
    return "s3a";
  }

  @Override
  public String name() {
    return "s3";
  }
}
