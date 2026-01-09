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
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.hive.util.Constants;
import org.apache.gravitino.catalog.hive.HiveConstants;
import org.apache.gravitino.catalog.hive.StorageFormat;
import org.apache.gravitino.flink.connector.CatalogPropertiesConverter;
import org.apache.gravitino.flink.connector.SchemaAndTablePropertiesConverter;
import org.apache.hadoop.hive.conf.HiveConf;

public class HivePropertiesConverter
    implements CatalogPropertiesConverter, SchemaAndTablePropertiesConverter {

  public static final HivePropertiesConverter INSTANCE = new HivePropertiesConverter(null);
  private static final Map<String, String> HIVE_CATALOG_CONFIG_TO_GRAVITINO =
      ImmutableMap.of(HiveConf.ConfVars.METASTOREURIS.varname, HiveConstants.METASTORE_URIS);
  private static final Map<String, String> GRAVITINO_CONFIG_TO_HIVE =
      ImmutableMap.of(HiveConstants.METASTORE_URIS, HiveConf.ConfVars.METASTOREURIS.varname);
  private final HiveConf hiveConf;

  HivePropertiesConverter(@Nullable HiveConf hiveConf) {
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
    Map<String, String> properties =
        gravitinoTableProperties.entrySet().stream()
            .collect(
                Collectors.toMap(
                    entry -> {
                      String key = entry.getKey();
                      if (key.startsWith(HiveConstants.SERDE_PARAMETER_PREFIX)) {
                        return key.substring(HiveConstants.SERDE_PARAMETER_PREFIX.length());
                      } else {
                        return key;
                      }
                    },
                    Map.Entry::getValue,
                    (existingValue, newValue) -> newValue));
    properties.put("connector", "hive");
    return properties;
  }

  @Override
  public Map<String, String> toGravitinoTableProperties(Map<String, String> flinkProperties) {
    Map<String, String> properties = new HashMap<>(flinkProperties);
    String rowFormatSerde = properties.remove(Constants.SERDE_LIB_CLASS_NAME);
    String storedAsFileFormat = properties.remove(Constants.STORED_AS_FILE_FORMAT);
    String storedAsInputFormat = properties.remove(Constants.STORED_AS_INPUT_FORMAT);
    String storedAsOutputFormat = properties.remove(Constants.STORED_AS_OUTPUT_FORMAT);

    Map<String, String> serdeParameters = new HashMap<>();
    for (Map.Entry<String, String> entry : properties.entrySet()) {
      String key = entry.getKey();
      if (key.startsWith(Constants.SERDE_INFO_PROP_PREFIX)) {
        String parameterKey = key.substring(Constants.SERDE_INFO_PROP_PREFIX.length());
        serdeParameters.put(HiveConstants.SERDE_PARAMETER_PREFIX + parameterKey, entry.getValue());
      }
    }
    properties.keySet().removeIf(k -> k.startsWith(Constants.SERDE_INFO_PROP_PREFIX));
    properties.putAll(serdeParameters);

    HiveConf effectiveHiveConf = hiveConf == null ? new HiveConf() : hiveConf;
    StorageFormat resolvedFormat = null;
    if (storedAsFileFormat != null) {
      resolvedFormat = resolveStorageFormat(storedAsFileFormat, true, effectiveHiveConf);
      properties.put(HiveConstants.FORMAT, resolvedFormat.name());
    } else if (properties.containsKey(HiveConstants.FORMAT)) {
      resolvedFormat =
          resolveStorageFormat(
              properties.get(HiveConstants.FORMAT), false, effectiveHiveConf);
    } else {
      resolvedFormat =
          resolveStorageFormat(
              effectiveHiveConf.getVar(HiveConf.ConfVars.HIVEDEFAULTFILEFORMAT),
              false,
              effectiveHiveConf);
      if (resolvedFormat != null) {
        properties.put(HiveConstants.FORMAT, resolvedFormat.name());
      }
    }

    if (storedAsInputFormat != null) {
      properties.put(HiveConstants.INPUT_FORMAT, storedAsInputFormat);
    }
    if (storedAsOutputFormat != null) {
      properties.put(HiveConstants.OUTPUT_FORMAT, storedAsOutputFormat);
    }

    String serdeToUse = null;
    boolean storageFormatSpecified = storedAsFileFormat != null;
    if (storageFormatSpecified) {
      if (formatProvidesSerde(resolvedFormat, effectiveHiveConf)) {
        serdeToUse = resolveSerdeForFormat(resolvedFormat, effectiveHiveConf);
      } else if (rowFormatSerde != null) {
        serdeToUse = rowFormatSerde;
      } else {
        serdeToUse = effectiveHiveConf.getVar(HiveConf.ConfVars.HIVEDEFAULTSERDE);
      }
    } else if (rowFormatSerde != null) {
      serdeToUse = rowFormatSerde;
    } else if (resolvedFormat != null && !formatProvidesSerde(resolvedFormat, effectiveHiveConf)) {
      serdeToUse = effectiveHiveConf.getVar(HiveConf.ConfVars.HIVEDEFAULTSERDE);
    } else if (StorageFormat.RCFILE.equals(resolvedFormat)) {
      serdeToUse = resolveSerdeForFormat(resolvedFormat, effectiveHiveConf);
    }

    if (serdeToUse != null) {
      properties.put(HiveConstants.SERDE_LIB, serdeToUse);
    }

    return properties;
  }

  @Override
  public String getFlinkCatalogType() {
    return GravitinoHiveCatalogFactoryOptions.IDENTIFIER;
  }

  private static StorageFormat resolveStorageFormat(
      String format, boolean failOnUnknown, HiveConf hiveConf) {
    if (format == null) {
      return null;
    }
    String normalized = format.trim();
    if (normalized.isEmpty()) {
      return null;
    }
    normalized = normalized.toLowerCase(Locale.ROOT);
    switch (normalized) {
      case "textfile":
      case "text":
        return StorageFormat.TEXTFILE;
      case "sequencefile":
      case "sequence":
        return StorageFormat.SEQUENCEFILE;
      case "rcfile":
        return StorageFormat.RCFILE;
      case "orc":
        return StorageFormat.ORC;
      case "parquet":
        return StorageFormat.PARQUET;
      case "avro":
        return StorageFormat.AVRO;
      case "json":
        return StorageFormat.JSON;
      case "csv":
        return StorageFormat.CSV;
      case "regex":
        return StorageFormat.REGEX;
      default:
        break;
    }
    try {
      return StorageFormat.valueOf(normalized.toUpperCase(Locale.ROOT));
    } catch (IllegalArgumentException ignored) {
      if (failOnUnknown) {
        throw new IllegalArgumentException("Unknown storage format: " + format);
      }
      return null;
    }
  }

  private static boolean formatProvidesSerde(StorageFormat format, HiveConf hiveConf) {
    if (format == null) {
      return false;
    }
    if (StorageFormat.RCFILE.equals(format)) {
      String serde = hiveConf.getVar(HiveConf.ConfVars.HIVEDEFAULTRCFILESERDE);
      return serde != null && !serde.isEmpty();
    }
    return format.getSerde() != null && !format.getSerde().isEmpty();
  }

  private static String resolveSerdeForFormat(StorageFormat format, HiveConf hiveConf) {
    if (format == null) {
      return null;
    }
    if (StorageFormat.RCFILE.equals(format)) {
      return hiveConf.getVar(HiveConf.ConfVars.HIVEDEFAULTRCFILESERDE);
    }
    return format.getSerde();
  }
}
