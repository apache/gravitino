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
package org.apache.gravitino.catalog.hadoop.config;

import com.google.common.collect.ImmutableMap;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.Config;
import org.apache.gravitino.config.ConfigBuilder;
import org.apache.gravitino.config.ConfigConstants;
import org.apache.gravitino.config.ConfigEntry;
import org.apache.gravitino.connector.PropertyEntry;
import org.apache.gravitino.storage.S3Properties;

public class HadoopS3FileSystemConfig extends Config {

  public static final Map<String, String> GRAVITINO_KEY_TO_S3_HADOOP_KEY =
      ImmutableMap.of(
          S3Properties.GRAVITINO_S3_ENDPOINT, "fs.s3a.endpoint",
          S3Properties.GRAVITINO_S3_ACCESS_KEY_ID, "fs.s3a.access.key",
          S3Properties.GRAVITINO_S3_SECRET_ACCESS_KEY, "fs.s3a.secret.key",
          S3Properties.GRAVITINO_S3_CREDS_PROVIDER, "fs.s3a.aws.credentials.provider");

  public HadoopS3FileSystemConfig(Map<String, String> properties) {
    super(false);
    loadFromMap(properties, k -> true);
  }

  public static final ConfigEntry<String> HADOOP_S3_ENDPOINT_ENTRY =
      new ConfigBuilder(S3Properties.GRAVITINO_S3_ENDPOINT)
          .doc("The endpoint of the AWS s3")
          .version(ConfigConstants.VERSION_0_7_0)
          .stringConf()
          .checkValue(StringUtils::isNotBlank, ConfigConstants.NOT_BLANK_ERROR_MSG)
          .create();

  public static final ConfigEntry<String> HADOOP_S3_ACCESS_KEY_ENTRY =
      new ConfigBuilder(S3Properties.GRAVITINO_S3_ACCESS_KEY_ID)
          .doc("The access key of the AWS s3")
          .version(ConfigConstants.VERSION_0_7_0)
          .stringConf()
          .checkValue(StringUtils::isNotBlank, ConfigConstants.NOT_BLANK_ERROR_MSG)
          .create();

  public static final ConfigEntry<String> HADOOP_S3_SECRET_KEY_ENTRY =
      new ConfigBuilder(S3Properties.GRAVITINO_S3_SECRET_ACCESS_KEY)
          .doc("The secret key of the AWS s3")
          .version(ConfigConstants.VERSION_0_7_0)
          .stringConf()
          .checkValue(StringUtils::isNotBlank, ConfigConstants.NOT_BLANK_ERROR_MSG)
          .create();

  public static final ConfigEntry<String> HADOOP_S3_CREDENTIAL_PROVIDER_KEY_ENTRY =
      new ConfigBuilder(S3Properties.GRAVITINO_S3_CREDS_PROVIDER)
          .doc("The S3 credentials provider class name")
          .version(ConfigConstants.VERSION_0_7_0)
          .stringConf()
          .checkValue(StringUtils::isNotBlank, ConfigConstants.NOT_BLANK_ERROR_MSG)
          .create();

  public static final Map<String, PropertyEntry<?>> S3_FILESYSTEM_PROPERTY_ENTRIES =
      new ImmutableMap.Builder<String, PropertyEntry<?>>()
          .put(
              S3Properties.GRAVITINO_S3_ENDPOINT,
              PropertyEntry.stringOptionalPropertyEntry(
                  S3Properties.GRAVITINO_S3_ENDPOINT,
                  "The endpoint of the AWS s3",
                  false /* immutable */,
                  null /* defaultValue */,
                  false /* hidden */))
          .put(
              S3Properties.GRAVITINO_S3_ACCESS_KEY_ID,
              PropertyEntry.stringOptionalPropertyEntry(
                  S3Properties.GRAVITINO_S3_ACCESS_KEY_ID,
                  "The access key of the AWS s3",
                  false /* immutable */,
                  null /* defaultValue */,
                  false /* hidden */))
          .put(
              S3Properties.GRAVITINO_S3_SECRET_ACCESS_KEY,
              PropertyEntry.stringOptionalPropertyEntry(
                  S3Properties.GRAVITINO_S3_SECRET_ACCESS_KEY,
                  "The secret key of the AWS s3",
                  false /* immutable */,
                  null /* defaultValue */,
                  false /* hidden */))
          .put(
              S3Properties.GRAVITINO_S3_CREDS_PROVIDER,
              PropertyEntry.stringOptionalPropertyEntry(
                  S3Properties.GRAVITINO_S3_CREDS_PROVIDER,
                  "The S3 credentials provider class name",
                  false /* immutable */,
                  null /* defaultValue */,
                  false /* hidden */))
          .build();
}
