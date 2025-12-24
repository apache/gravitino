/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.gravitino.filesystem.hadoop.integration.test;

import com.google.common.collect.Maps;
import java.util.Map;
import org.apache.gravitino.catalog.hadoop.fs.FileSystemUtils;
import org.apache.gravitino.credential.GCSTokenCredential;
import org.apache.gravitino.gcs.fs.GCSFileSystemProvider;
import org.apache.gravitino.storage.GCSProperties;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.junit.jupiter.api.condition.EnabledIf;
import org.junit.platform.commons.util.StringUtils;

@EnabledIf(value = "isGCPConfigured", disabledReason = "GCP is not configured")
public class FileSystemGCSCredentialAuthorizationIT
    extends AbstractFileSystemCredentialAuthorizationIT {
  public static final String BUCKET_NAME = System.getenv("GCS_BUCKET_NAME_FOR_CREDENTIAL");
  public static final String SERVICE_ACCOUNT_FILE =
      System.getenv("GCS_SERVICE_ACCOUNT_JSON_PATH_FOR_CREDENTIAL");

  @Override
  protected String providerName() {
    return "gcs";
  }

  @Override
  protected String providerBundleName() {
    return "gcp-bundle";
  }

  @Override
  protected String credentialProviderType() {
    return GCSTokenCredential.GCS_TOKEN_CREDENTIAL_TYPE;
  }

  @Override
  protected Map<String, String> catalogBaseProperties() {
    Map<String, String> properties = Maps.newHashMap();
    properties.put(GCSProperties.GRAVITINO_GCS_SERVICE_ACCOUNT_FILE, SERVICE_ACCOUNT_FILE);
    return properties;
  }

  @Override
  protected String genStorageLocation(String fileset) {
    return String.format("gs://%s/dir1/dir2/%s", BUCKET_NAME, fileset);
  }

  @Override
  protected Path genGvfsPath(String fileset) {
    return new Path(String.format("gvfs://fileset/%s/%s/%s", catalogName, schemaName, fileset));
  }

  @Override
  protected Configuration convertGvfsConfigToRealFileSystemConfig(Configuration gvfsConf) {
    Configuration gcsConf = new Configuration();
    Map<String, String> map = Maps.newHashMap();
    map.put(GCSProperties.GRAVITINO_GCS_SERVICE_ACCOUNT_FILE, SERVICE_ACCOUNT_FILE);
    gvfsConf.forEach(entry -> map.put(entry.getKey(), entry.getValue()));
    Map<String, String> hadoopConfMap =
        FileSystemUtils.toHadoopConfigMap(
            map, GCSFileSystemProvider.GRAVITINO_KEY_TO_GCS_HADOOP_KEY);
    hadoopConfMap.forEach(gcsConf::set);
    return gcsConf;
  }

  @Override
  protected String providerPrefix() {
    return "gvfs_gcs";
  }

  @Override
  protected String providerRoleName() {
    return "gvfs_gcs_credential_auth_role";
  }

  protected static boolean isGCPConfigured() {
    return StringUtils.isNotBlank(System.getenv("GCS_SERVICE_ACCOUNT_JSON_PATH_FOR_CREDENTIAL"))
        && StringUtils.isNotBlank(System.getenv("GCS_BUCKET_NAME_FOR_CREDENTIAL"));
  }
}
