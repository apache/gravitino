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
import java.util.Map;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.hive.util.Constants;
import org.apache.gravitino.catalog.hive.HiveConstants;
import org.apache.gravitino.flink.connector.CatalogPropertiesConverter;
import org.apache.gravitino.flink.connector.SchemaAndTablePropertiesConverter;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.io.RCFileStorageFormatDescriptor;
import org.apache.hadoop.hive.ql.io.StorageFormatDescriptor;
import org.apache.hadoop.hive.ql.io.StorageFormatFactory;

public class HivePropertiesConverter
    implements CatalogPropertiesConverter, SchemaAndTablePropertiesConverter {

  public static final HivePropertiesConverter INSTANCE = new HivePropertiesConverter(null);
  private static final Map<String, String> HIVE_CATALOG_CONFIG_TO_GRAVITINO =
      ImmutableMap.of(HiveConf.ConfVars.METASTOREURIS.varname, HiveConstants.METASTORE_URIS);
  private static final Map<String, String> GRAVITINO_CONFIG_TO_HIVE =
      ImmutableMap.of(HiveConstants.METASTORE_URIS, HiveConf.ConfVars.METASTOREURIS.varname);
  private static final StorageFormatFactory STORAGE_FORMAT_FACTORY = new StorageFormatFactory();
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
    String specifiedSerdeLib = properties.remove(Constants.SERDE_LIB_CLASS_NAME);
    String specifiedStorageFormat = properties.remove(Constants.STORED_AS_FILE_FORMAT);
    String specifiedInputFormat = properties.remove(Constants.STORED_AS_INPUT_FORMAT);
    String specifiedOutputFormat = properties.remove(Constants.STORED_AS_OUTPUT_FORMAT);

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
    if (specifiedStorageFormat == null && specifiedInputFormat != null) {
      properties.put(HiveConstants.INPUT_FORMAT, specifiedInputFormat);
    }
    if (specifiedStorageFormat == null && specifiedOutputFormat != null) {
      properties.put(HiveConstants.OUTPUT_FORMAT, specifiedOutputFormat);
    }

    String serdeToUse =
        resolveSerdeLib(
            specifiedStorageFormat,
            specifiedSerdeLib,
            effectiveHiveConf);
    if (serdeToUse != null) {
      properties.put(HiveConstants.SERDE_LIB, serdeToUse);
    }

    String formatRaw = resolveStorageFormat(specifiedStorageFormat, effectiveHiveConf);
    if (formatRaw != null) {
      properties.put(HiveConstants.FORMAT, formatRaw);
    }
    return properties;
  }

  @Override
  public String getFlinkCatalogType() {
    return GravitinoHiveCatalogFactoryOptions.IDENTIFIER;
  }

  private static String resolveStorageFormat(String storedAsFileFormat, HiveConf hiveConf) {
    if (storedAsFileFormat != null) {
      return storedAsFileFormat;
    }
    return getDefaultStorageFormat(hiveConf);
  }

  private static String getDefaultStorageFormat(HiveConf hiveConf) {
    return hiveConf.getVar(HiveConf.ConfVars.HIVEDEFAULTFILEFORMAT);
  }

  // 1. use the serde lib in the format
  // 2. for rc format, use `hive.default.rcfile.serde` in hive conf
  // 3. use the serde lib specified in the properties
  // 4. use the default serde in hive conf
  // please refer to  org.apache.flink.table.catalog.hive.util.HiveTableUtils for more details
  private static String resolveSerdeLib(
      String specifiedStorageFormat,
      @Nullable String specifiedSerde,
      HiveConf hiveConf) {
    String formatSerde = getSerdeForFormat(specifiedStorageFormat, hiveConf);
    if (formatSerde != null) {
      return formatSerde;
    }

    if (specifiedSerde != null) {
      return specifiedSerde;
    }

    if (specifiedStorageFormat == null) {
      formatSerde = getSerdeForFormat(getDefaultStorageFormat(hiveConf), hiveConf);
      if (formatSerde != null) {
        return formatSerde;
      }
    }

    return hiveConf.getVar(HiveConf.ConfVars.HIVEDEFAULTSERDE);
  }

  private static String getSerdeForFormat(String format, HiveConf hiveConf) {
    if (format == null) {
      return null;
    }
    StorageFormatDescriptor descriptor = STORAGE_FORMAT_FACTORY.get(format);
    if (descriptor == null) {
      return null;
    }
    String serdeLib = descriptor.getSerde();
    if (serdeLib == null && descriptor instanceof RCFileStorageFormatDescriptor) {
      serdeLib = hiveConf.getVar(HiveConf.ConfVars.HIVEDEFAULTRCFILESERDE);
    }
    return serdeLib;
  }
}
