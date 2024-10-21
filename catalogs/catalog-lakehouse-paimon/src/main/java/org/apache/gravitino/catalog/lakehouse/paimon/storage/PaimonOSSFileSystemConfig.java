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
package org.apache.gravitino.catalog.lakehouse.paimon.storage;

import com.google.common.collect.ImmutableMap;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.Config;
import org.apache.gravitino.config.ConfigBuilder;
import org.apache.gravitino.config.ConfigConstants;
import org.apache.gravitino.config.ConfigEntry;
import org.apache.gravitino.connector.PropertyEntry;

public class PaimonOSSFileSystemConfig extends Config {
  // OSS related properties
  public static final String OSS_ENDPOINT = "fs.oss.endpoint";
  public static final String OSS_ACCESS_KEY = "fs.oss.accessKeyId";
  public static final String OSS_SECRET_KEY = "fs.oss.accessKeySecret";

  public PaimonOSSFileSystemConfig(Map<String, String> properties) {
    super(false);
    loadFromMap(properties, k -> true);
  }

  public static final ConfigEntry<String> PAIMON_OSS_ENDPOINT_ENTRY =
      new ConfigBuilder(OSS_ENDPOINT)
          .doc("The endpoint of the Aliyun oss")
          .version(ConfigConstants.VERSION_0_7_0)
          .stringConf()
          .checkValue(StringUtils::isNotBlank, ConfigConstants.NOT_BLANK_ERROR_MSG)
          .create();

  public static final ConfigEntry<String> PAIMON_OSS_ACCESS_KEY_ENTRY =
      new ConfigBuilder(OSS_ACCESS_KEY)
          .doc("The access key of the Aliyun oss")
          .version(ConfigConstants.VERSION_0_7_0)
          .stringConf()
          .checkValue(StringUtils::isNotBlank, ConfigConstants.NOT_BLANK_ERROR_MSG)
          .create();

  public static final ConfigEntry<String> PAIMON_OSS_SECRET_KEY_ENTRY =
      new ConfigBuilder(OSS_SECRET_KEY)
          .doc("The secret key of the Aliyun oss")
          .version(ConfigConstants.VERSION_0_7_0)
          .stringConf()
          .checkValue(StringUtils::isNotBlank, ConfigConstants.NOT_BLANK_ERROR_MSG)
          .create();

  public String getOSSEndpoint() {
    return get(PAIMON_OSS_ENDPOINT_ENTRY);
  }

  public String getOSSAccessKey() {
    return get(PAIMON_OSS_ACCESS_KEY_ENTRY);
  }

  public String getOSSSecretKey() {
    return get(PAIMON_OSS_SECRET_KEY_ENTRY);
  }

  public static final Map<String, PropertyEntry<?>> OSS_FILESYSTEM_PROPERTY_ENTRIES =
      new ImmutableMap.Builder<String, PropertyEntry<?>>()
          .put(
              OSS_ENDPOINT,
              PropertyEntry.stringOptionalPropertyEntry(
                  OSS_ENDPOINT,
                  "The endpoint of the Aliyun oss",
                  false /* immutable */,
                  null /* defaultValue */,
                  false /* hidden */))
          .put(
              OSS_ACCESS_KEY,
              PropertyEntry.stringOptionalPropertyEntry(
                  OSS_ACCESS_KEY,
                  "The access key of the Aliyun oss",
                  false /* immutable */,
                  null /* defaultValue */,
                  false /* hidden */))
          .put(
              OSS_SECRET_KEY,
              PropertyEntry.stringOptionalPropertyEntry(
                  OSS_SECRET_KEY,
                  "The secret key of the Aliyun oss",
                  false /* immutable */,
                  null /* defaultValue */,
                  false /* hidden */))
          .build();
}
