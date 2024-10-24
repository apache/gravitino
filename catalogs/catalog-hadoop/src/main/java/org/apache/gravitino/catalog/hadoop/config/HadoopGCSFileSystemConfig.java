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
import org.apache.gravitino.storage.GCSProperties;

public class HadoopGCSFileSystemConfig extends Config {

  public static final Map<String, String> GRAVITINO_KEY_TO_GCS_HADOOP_KEY =
      ImmutableMap.of(
          GCSProperties.GCS_SERVICE_ACCOUNT_JSON_PATH, "fs.gs.auth.service.account.json.keyfile");

  public HadoopGCSFileSystemConfig(Map<String, String> properties) {
    super(false);
    loadFromMap(properties, k -> true);
  }

  public static final ConfigEntry<String> HADOOP_OSS_ENDPOINT_ENTRY =
      new ConfigBuilder(GCSProperties.GCS_SERVICE_ACCOUNT_JSON_PATH)
          .doc("The path of service account json file of Google Cloud Storage")
          .version(ConfigConstants.VERSION_0_7_0)
          .stringConf()
          .checkValue(StringUtils::isNotBlank, ConfigConstants.NOT_BLANK_ERROR_MSG)
          .create();

  public String getServiceAccountJsonPath() {
    return get(HADOOP_OSS_ENDPOINT_ENTRY);
  }

  public static final Map<String, PropertyEntry<?>> GCS_FILESYSTEM_PROPERTY_ENTRIES =
      new ImmutableMap.Builder<String, PropertyEntry<?>>()
          .put(
              GCSProperties.GCS_SERVICE_ACCOUNT_JSON_PATH,
              PropertyEntry.stringOptionalPropertyEntry(
                  GCSProperties.GCS_SERVICE_ACCOUNT_JSON_PATH,
                  "The path of service account json file of Google Cloud Storage",
                  false /* immutable */,
                  null /* defaultValue */,
                  false /* hidden */))
          .build();
}
