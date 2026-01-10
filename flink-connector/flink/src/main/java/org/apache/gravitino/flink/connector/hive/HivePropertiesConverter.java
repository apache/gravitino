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

package org.apache.gravitino.flink.connector.hive;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.hive.util.Constants;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.gravitino.catalog.hive.HiveConstants;
import org.apache.gravitino.flink.connector.PropertiesConverter;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.ql.io.RCFileStorageFormatDescriptor;
import org.apache.hadoop.hive.ql.io.StorageFormatDescriptor;
import org.apache.hadoop.hive.ql.io.StorageFormatFactory;

public class HivePropertiesConverter implements PropertiesConverter {

  private static final StorageFormatFactory STORAGE_FORMAT_FACTORY = new StorageFormatFactory();
  private static final Map<String, String> HIVE_CATALOG_CONFIG_TO_GRAVITINO =
      ImmutableMap.of(HiveConf.ConfVars.METASTOREURIS.varname, HiveConstants.METASTORE_URIS);
  private static final Map<String, String> GRAVITINO_CONFIG_TO_HIVE =
      ImmutableMap.of(HiveConstants.METASTORE_URIS, HiveConf.ConfVars.METASTOREURIS.varname);
  private final HiveConf hiveConf;

  HivePropertiesConverter() {
    this(new HiveConf());
  }

  HivePropertiesConverter(HiveConf hiveConf) {
    this.hiveConf = hiveConf;
  }

  @Override
  public String transformPropertyToGravitinoCatalog(String configKey) {
    return HIVE_CATALOG_CONFIG_TO_GRAVITINO.get(configKey);
  }

  @Override
  public String transformPropertyToFlinkCatalog(String configKey) {
    return GRAVITINO_CONFIG_TO_HIVE.get(configKey);
  }

  @Override
  public Map<String, String> toFlinkTableProperties(
      Map<String, String> flinkCatalogProperties,
      Map<String, String> gravitinoTableProperties,
      ObjectPath tablePath) {
    Map<String, String> normalizedProperties = Maps.newHashMap(gravitinoTableProperties);
    applyDefaultStorageFormat(normalizedProperties);
    Map<String, String> properties =
        normalizedProperties.entrySet().stream()
            .collect(
                Collectors.toMap(
                    entry -> toFlinkTablePropertyKey(entry.getKey()),
                    Map.Entry::getValue,
                    (existingValue, newValue) -> newValue));
    properties.put(FactoryUtil.CONNECTOR.key(), Constants.IDENTIFIER);
    return properties;
  }

  @Override
  public Map<String, String> toGravitinoTableProperties(Map<String, String> flinkProperties) {
    Map<String, String> properties =
        flinkProperties.entrySet().stream()
            .collect(
                Collectors.toMap(
                    entry -> toGravitinoTablePropertyKey(entry.getKey()),
                    Map.Entry::getValue,
                    (existingValue, newValue) -> newValue,
                    Maps::newHashMap));
    applyDefaultStorageFormat(properties);
    return properties;
  }

  @Override
  public String getFlinkCatalogType() {
    return GravitinoHiveCatalogFactoryOptions.IDENTIFIER;
  }

  private String toGravitinoTablePropertyKey(String key) {
    if (key.startsWith(Constants.SERDE_INFO_PROP_PREFIX)) {
      return HiveConstants.SERDE_PARAMETER_PREFIX
          + key.substring(Constants.SERDE_INFO_PROP_PREFIX.length());
    }
    if (Constants.SERDE_LIB_CLASS_NAME.equals(key)) {
      return HiveConstants.SERDE_LIB;
    }
    if (Constants.STORED_AS_FILE_FORMAT.equals(key)) {
      return HiveConstants.FORMAT;
    }
    if (Constants.STORED_AS_INPUT_FORMAT.equals(key)) {
      return HiveConstants.INPUT_FORMAT;
    }
    if (Constants.STORED_AS_OUTPUT_FORMAT.equals(key)) {
      return HiveConstants.OUTPUT_FORMAT;
    }
    if (Constants.TABLE_LOCATION_URI.equals(key)) {
      return HiveConstants.LOCATION;
    }
    if (Constants.TABLE_IS_EXTERNAL.equals(key)) {
      return HiveConstants.EXTERNAL;
    }
    return key;
  }

  private String toFlinkTablePropertyKey(String key) {
    if (key.startsWith(HiveConstants.SERDE_PARAMETER_PREFIX)) {
      return Constants.SERDE_INFO_PROP_PREFIX
          + key.substring(HiveConstants.SERDE_PARAMETER_PREFIX.length());
    }
    if (HiveConstants.SERDE_LIB.equals(key)) {
      return Constants.SERDE_LIB_CLASS_NAME;
    }
    if (HiveConstants.FORMAT.equals(key)) {
      return Constants.STORED_AS_FILE_FORMAT;
    }
    if (HiveConstants.INPUT_FORMAT.equals(key)) {
      return Constants.STORED_AS_INPUT_FORMAT;
    }
    if (HiveConstants.OUTPUT_FORMAT.equals(key)) {
      return Constants.STORED_AS_OUTPUT_FORMAT;
    }
    if (HiveConstants.LOCATION.equals(key)) {
      return Constants.TABLE_LOCATION_URI;
    }
    if (HiveConstants.EXTERNAL.equals(key)) {
      return Constants.TABLE_IS_EXTERNAL;
    }
    return key;
  }

  private void applyDefaultStorageFormat(Map<String, String> properties) {
    String format = properties.get(HiveConstants.FORMAT);
    String inputFormat = properties.get(HiveConstants.INPUT_FORMAT);
    String outputFormat = properties.get(HiveConstants.OUTPUT_FORMAT);
    String serdeLib = properties.get(HiveConstants.SERDE_LIB);

    StorageFormatDescriptor defaultDescriptor =
        getStorageFormatDescriptor(hiveConf.getVar(ConfVars.HIVEDEFAULTFILEFORMAT));
    String defaultSerde =
        resolveSerde(defaultDescriptor, hiveConf.getVar(ConfVars.HIVEDEFAULTSERDE));

    if (format != null) {
      StorageFormatDescriptor descriptor = getStorageFormatDescriptor(format);
      properties.put(HiveConstants.INPUT_FORMAT, descriptor.getInputFormat());
      properties.put(HiveConstants.OUTPUT_FORMAT, descriptor.getOutputFormat());
      String resolvedSerde = resolveSerde(descriptor, null);
      if (resolvedSerde != null) {
        properties.put(HiveConstants.SERDE_LIB, resolvedSerde);
      }
      properties.put(HiveConstants.FORMAT, format);
      return;
    }

    if (inputFormat != null || outputFormat != null) {
      if (inputFormat != null) {
        properties.put(HiveConstants.INPUT_FORMAT, inputFormat);
      }
      if (outputFormat != null) {
        properties.put(HiveConstants.OUTPUT_FORMAT, outputFormat);
      }
      if (serdeLib == null) {
        properties.put(HiveConstants.SERDE_LIB, defaultSerde);
      }
      return;
    }

    properties.put(HiveConstants.FORMAT, hiveConf.getVar(ConfVars.HIVEDEFAULTFILEFORMAT));
    properties.put(HiveConstants.INPUT_FORMAT, defaultDescriptor.getInputFormat());
    properties.put(HiveConstants.OUTPUT_FORMAT, defaultDescriptor.getOutputFormat());
    properties.put(HiveConstants.SERDE_LIB, defaultSerde);
  }

  private StorageFormatDescriptor getStorageFormatDescriptor(String format) {
    StorageFormatDescriptor descriptor = STORAGE_FORMAT_FACTORY.get(format);
    if (descriptor == null) {
      throw new IllegalArgumentException("Unknown storage format " + format);
    }
    return descriptor;
  }

  private String resolveSerde(StorageFormatDescriptor descriptor, String fallback) {
    String serdeLib = descriptor.getSerde();
    if (serdeLib == null && descriptor instanceof RCFileStorageFormatDescriptor) {
      serdeLib = hiveConf.getVar(ConfVars.HIVEDEFAULTRCFILESERDE);
    }
    return serdeLib != null ? serdeLib : fallback;
  }
}
