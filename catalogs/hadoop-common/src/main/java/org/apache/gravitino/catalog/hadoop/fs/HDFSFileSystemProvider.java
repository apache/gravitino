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
import static org.apache.gravitino.catalog.hadoop.fs.Constants.DEFAULT_CONNECTION_TIMEOUT;
import static org.apache.gravitino.catalog.hadoop.fs.Constants.DEFAULT_HDFS_IPC_PING;
import static org.apache.gravitino.catalog.hadoop.fs.Constants.HDFS_IPC_CLIENT_CONNECT_TIMEOUT_KEY;
import static org.apache.gravitino.catalog.hadoop.fs.Constants.HDFS_IPC_PING_KEY;
import static org.apache.gravitino.catalog.hadoop.fs.kerberos.AuthenticationConfig.IMPERSONATION_ENABLE_KEY;
import static org.apache.gravitino.catalog.hadoop.fs.kerberos.KerberosConfig.PRINCIPAL_KEY;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import java.io.IOException;
import java.util.Map;
import javax.annotation.Nonnull;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class HDFSFileSystemProvider implements FileSystemProvider {
  public static final String IPC_FALLBACK_TO_SIMPLE_AUTH_ALLOWED =
      "hadoop.rpc.protection.fallback-to-simple-auth-allowed";
  public static final String SCHEME_HDFS = "hdfs";

  @Override
  public FileSystem getFileSystem(@Nonnull Path path, @Nonnull Map<String, String> config)
      throws IOException {
    Map<String, String> hadoopConfMap = additionalHDFSConfig(config);
    Configuration configuration =
        FileSystemUtils.createConfiguration(GRAVITINO_BYPASS, hadoopConfMap);

    HDFSFileSystemProxy proxy = new HDFSFileSystemProxy(path, configuration, config);
    return proxy.getProxy();
  }

  @Override
  public String scheme() {
    return SCHEME_HDFS;
  }

  @Override
  public String name() {
    return BUILTIN_HDFS_FS_PROVIDER;
  }

  @Override
  public String getFullAuthority(Path path, Map<String, String> conf) {
    String authority = path.toUri().getAuthority();
    String principal = conf.get(PRINCIPAL_KEY);
    if (StringUtils.isNotBlank(principal)) {
      principal = principal.replaceAll("@.*$", ""); // Remove realm if exists
      authority = String.format("%s@%s", principal, authority);
    }
    String impersonationEnabled = conf.get(IMPERSONATION_ENABLE_KEY);
    if (conf.containsKey(IMPERSONATION_ENABLE_KEY)) {
      authority = String.format("%s?impersonation_enabled=%s", authority, impersonationEnabled);
    }
    return authority;
  }

  /**
   * Add additional HDFS specific configurations.
   *
   * @param configs Original configurations.
   * @return Configurations with additional HDFS specific configurations.
   */
  private Map<String, String> additionalHDFSConfig(Map<String, String> configs) {
    Map<String, String> additionalConfigs = Maps.newHashMap(configs);

    // Avoid multiple retries to speed up failure in test cases.
    // Use hard code instead of CommonConfigurationKeysPublic.IPC_CLIENT_CONNECT_TIMEOUT_KEY to
    // avoid dependency on a specific Hadoop version.
    if (!configs.containsKey(HDFS_IPC_CLIENT_CONNECT_TIMEOUT_KEY)) {
      additionalConfigs.put(HDFS_IPC_CLIENT_CONNECT_TIMEOUT_KEY, DEFAULT_CONNECTION_TIMEOUT);
    }

    if (!configs.containsKey(HDFS_IPC_PING_KEY)) {
      additionalConfigs.put(HDFS_IPC_PING_KEY, DEFAULT_HDFS_IPC_PING);
    }

    // More tuning can be added here.

    return ImmutableMap.copyOf(additionalConfigs);
  }
}
