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
import org.apache.gravitino.storage.OSSProperties;

public class HadoopOSSFileSystemConfig extends Config {
  public static final Map<String, String> GRAVITINO_KEY_TO_OSS_HADOOP_KEY =
      ImmutableMap.of(
          OSSProperties.GRAVITINO_OSS_ENDPOINT, "fs.oss.endpoint",
          OSSProperties.GRAVITINO_OSS_ACCESS_KEY_ID, "fs.oss.accessKeyId",
          OSSProperties.GRAVITINO_OSS_ACCESS_KEY_SECRET, "fs.oss.accessKeySecret");

  public HadoopOSSFileSystemConfig(Map<String, String> properties) {
    super(false);
    loadFromMap(properties, k -> true);
  }

  public static final ConfigEntry<String> HADOOP_OSS_ENDPOINT_ENTRY =
      new ConfigBuilder(OSSProperties.GRAVITINO_OSS_ENDPOINT)
          .doc("The endpoint of the Aliyun OSS")
          .version(ConfigConstants.VERSION_0_7_0)
          .stringConf()
          .checkValue(StringUtils::isNotBlank, ConfigConstants.NOT_BLANK_ERROR_MSG)
          .create();

  public static final ConfigEntry<String> HADOOP_OSS_ACCESS_KEY_ENTRY =
      new ConfigBuilder(OSSProperties.GRAVITINO_OSS_ACCESS_KEY_ID)
          .doc("The access key of the Aliyun OSS")
          .version(ConfigConstants.VERSION_0_7_0)
          .stringConf()
          .checkValue(StringUtils::isNotBlank, ConfigConstants.NOT_BLANK_ERROR_MSG)
          .create();

  public static final ConfigEntry<String> HADOOP_OSS_SECRET_KEY_ENTRY =
      new ConfigBuilder(OSSProperties.GRAVITINO_OSS_ACCESS_KEY_SECRET)
          .doc("The secret key of the Aliyun OSS")
          .version(ConfigConstants.VERSION_0_7_0)
          .stringConf()
          .checkValue(StringUtils::isNotBlank, ConfigConstants.NOT_BLANK_ERROR_MSG)
          .create();

  public String getOSSEndpoint() {
    return get(HADOOP_OSS_ENDPOINT_ENTRY);
  }

  public String getOSSAccessKey() {
    return get(HADOOP_OSS_ACCESS_KEY_ENTRY);
  }

  public String getOSSSecretKey() {
    return get(HADOOP_OSS_SECRET_KEY_ENTRY);
  }

  public static final Map<String, PropertyEntry<?>> OSS_FILESYSTEM_PROPERTY_ENTRIES =
      new ImmutableMap.Builder<String, PropertyEntry<?>>()
          .put(
              OSSProperties.GRAVITINO_OSS_ENDPOINT,
              PropertyEntry.stringOptionalPropertyEntry(
                  OSSProperties.GRAVITINO_OSS_ENDPOINT,
                  "The endpoint of the Aliyun OSS",
                  false /* immutable */,
                  null /* defaultValue */,
                  false /* hidden */))
          .put(
              OSSProperties.GRAVITINO_OSS_ACCESS_KEY_ID,
              PropertyEntry.stringOptionalPropertyEntry(
                  OSSProperties.GRAVITINO_OSS_ACCESS_KEY_ID,
                  "The access key of the Aliyun OSS",
                  false /* immutable */,
                  null /* defaultValue */,
                  false /* hidden */))
          .put(
              OSSProperties.GRAVITINO_OSS_ACCESS_KEY_SECRET,
              PropertyEntry.stringOptionalPropertyEntry(
                  OSSProperties.GRAVITINO_OSS_ACCESS_KEY_SECRET,
                  "The secret key of the Aliyun OSS",
                  false /* immutable */,
                  null /* defaultValue */,
                  false /* hidden */))
          .build();
}
