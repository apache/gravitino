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

package org.apache.gravitino.flink.connector.iceberg;

import com.google.common.collect.ImmutableMap;
import java.util.HashMap;
import java.util.Map;
import org.apache.gravitino.catalog.lakehouse.iceberg.IcebergConstants;
import org.apache.gravitino.catalog.lakehouse.iceberg.IcebergPropertiesUtils;
import org.apache.gravitino.flink.connector.PropertiesConverter;

public class IcebergPropertiesConverter implements PropertiesConverter {
  public static IcebergPropertiesConverter INSTANCE = new IcebergPropertiesConverter();

  private IcebergPropertiesConverter() {}

  private static final Map<String, String> GRAVITINO_CONFIG_TO_FLINK_ICEBERG =
      ImmutableMap.of(
          IcebergConstants.CATALOG_BACKEND, IcebergPropertiesConstants.ICEBERG_CATALOG_TYPE);

  @Override
  public String transformPropertyToGravitinoCatalog(String configKey) {
    return IcebergPropertiesUtils.ICEBERG_CATALOG_CONFIG_TO_GRAVITINO.get(configKey);
  }

  @Override
  public String transformPropertyToFlinkCatalog(String configKey) {

    String icebergConfigKey = null;
    if (IcebergPropertiesUtils.GRAVITINO_CONFIG_TO_ICEBERG.containsKey(configKey)) {
      icebergConfigKey = IcebergPropertiesUtils.GRAVITINO_CONFIG_TO_ICEBERG.get(configKey);
    }
    if (GRAVITINO_CONFIG_TO_FLINK_ICEBERG.containsKey(configKey)) {
      icebergConfigKey = GRAVITINO_CONFIG_TO_FLINK_ICEBERG.get(configKey);
    }
    return icebergConfigKey;
  }

  @Override
  public Map<String, String> toGravitinoTableProperties(Map<String, String> properties) {
    return new HashMap<>(properties);
  }

  @Override
  public String getFlinkCatalogType() {
    return GravitinoIcebergCatalogFactoryOptions.IDENTIFIER;
  }
}
