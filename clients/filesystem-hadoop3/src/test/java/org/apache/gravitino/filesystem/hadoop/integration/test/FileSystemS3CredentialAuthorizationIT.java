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

package org.apache.gravitino.filesystem.hadoop.integration.test;

import com.google.common.collect.Maps;
import java.util.Map;
import org.apache.gravitino.catalog.hadoop.fs.FileSystemUtils;
import org.apache.gravitino.credential.S3TokenCredential;
import org.apache.gravitino.s3.fs.S3FileSystemProvider;
import org.apache.gravitino.storage.S3Properties;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.junit.jupiter.api.condition.EnabledIf;
import org.junit.platform.commons.util.StringUtils;

/**
 * GVFS integration test that verifies credential vending with authorization enabled on S3. <br>
 * - READ_FILESET: can read via GVFS but cannot write. <br>
 * - WRITE_FILESET: can read and write via GVFS.
 */
@EnabledIf(value = "s3IsConfigured", disabledReason = "s3 with credential is not prepared")
public class FileSystemS3CredentialAuthorizationIT
    extends AbstractFileSystemCredentialAuthorizationIT {

  public static final String BUCKET_NAME = System.getenv("S3_BUCKET_NAME_FOR_CREDENTIAL");
  public static final String S3_ACCESS_KEY = System.getenv("S3_ACCESS_KEY_ID_FOR_CREDENTIAL");
  public static final String S3_SECRET_KEY = System.getenv("S3_SECRET_ACCESS_KEY_FOR_CREDENTIAL");
  public static final String S3_ENDPOINT = System.getenv("S3_ENDPOINT_FOR_CREDENTIAL");
  public static final String S3_REGION = System.getenv("S3_REGION_FOR_CREDENTIAL");
  public static final String S3_ROLE_ARN = System.getenv("S3_ROLE_ARN_FOR_CREDENTIAL");

  @Override
  protected String providerName() {
    return "s3";
  }

  @Override
  protected String providerBundleName() {
    return "aws-bundle";
  }

  @Override
  protected String credentialProviderType() {
    return S3TokenCredential.S3_TOKEN_CREDENTIAL_TYPE;
  }

  @Override
  protected Map<String, String> catalogBaseProperties() {
    Map<String, String> properties = Maps.newHashMap();
    properties.put(S3Properties.GRAVITINO_S3_ACCESS_KEY_ID, S3_ACCESS_KEY);
    properties.put(S3Properties.GRAVITINO_S3_SECRET_ACCESS_KEY, S3_SECRET_KEY);
    properties.put(S3Properties.GRAVITINO_S3_ENDPOINT, S3_ENDPOINT);
    properties.put(
        "gravitino.bypass.fs.s3a.aws.credentials.provider",
        "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider");
    properties.put(S3Properties.GRAVITINO_S3_REGION, S3_REGION);
    properties.put(S3Properties.GRAVITINO_S3_ROLE_ARN, S3_ROLE_ARN);
    return properties;
  }

  @Override
  protected String genStorageLocation(String fileset) {
    return String.format("s3a://%s/%s", BUCKET_NAME, fileset);
  }

  @Override
  protected Path genGvfsPath(String fileset) {
    return new Path(String.format("gvfs://fileset/%s/%s/%s", catalogName, schemaName, fileset));
  }

  @Override
  protected Configuration convertGvfsConfigToRealFileSystemConfig(Configuration gvfsConf) {
    Configuration s3Conf = new Configuration();
    Map<String, String> map = Maps.newHashMap();
    gvfsConf.forEach(entry -> map.put(entry.getKey(), entry.getValue()));
    map.put(S3Properties.GRAVITINO_S3_ACCESS_KEY_ID, S3_ACCESS_KEY);
    map.put(S3Properties.GRAVITINO_S3_SECRET_ACCESS_KEY, S3_SECRET_KEY);
    map.put(S3Properties.GRAVITINO_S3_REGION, S3_REGION);
    map.put(S3Properties.GRAVITINO_S3_ENDPOINT, S3_ENDPOINT);
    Map<String, String> hadoopConfMap =
        FileSystemUtils.toHadoopConfigMap(map, S3FileSystemProvider.GRAVITINO_KEY_TO_S3_HADOOP_KEY);
    hadoopConfMap.forEach(s3Conf::set);
    return s3Conf;
  }

  @Override
  protected String providerPrefix() {
    return "gvfs_s3";
  }

  @Override
  protected String providerRoleName() {
    return "gvfs_s3_credential_auth_role";
  }

  protected static boolean s3IsConfigured() {
    return StringUtils.isNotBlank(System.getenv("S3_ACCESS_KEY_ID_FOR_CREDENTIAL"))
        && StringUtils.isNotBlank(System.getenv("S3_SECRET_ACCESS_KEY_FOR_CREDENTIAL"))
        && StringUtils.isNotBlank(System.getenv("S3_ENDPOINT_FOR_CREDENTIAL"))
        && StringUtils.isNotBlank(System.getenv("S3_BUCKET_NAME_FOR_CREDENTIAL"))
        && StringUtils.isNotBlank(System.getenv("S3_REGION_FOR_CREDENTIAL"))
        && StringUtils.isNotBlank(System.getenv("S3_ROLE_ARN_FOR_CREDENTIAL"));
  }
}
