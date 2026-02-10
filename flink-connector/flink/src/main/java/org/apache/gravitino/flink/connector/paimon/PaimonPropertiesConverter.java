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

package org.apache.gravitino.flink.connector.paimon;

import org.apache.gravitino.catalog.lakehouse.paimon.PaimonConstants;
import org.apache.gravitino.catalog.lakehouse.paimon.PaimonPropertiesUtils;
import org.apache.gravitino.flink.connector.PropertiesConverter;

public class PaimonPropertiesConverter implements PropertiesConverter {

  public static final PaimonPropertiesConverter INSTANCE = new PaimonPropertiesConverter();

  private PaimonPropertiesConverter() {}

  @Override
  public String transformPropertyToGravitinoCatalog(String configKey) {
    if (configKey.equalsIgnoreCase(PaimonConstants.METASTORE)) {
      return PaimonConstants.CATALOG_BACKEND;
    }
    return PaimonPropertiesUtils.PAIMON_CATALOG_CONFIG_TO_GRAVITINO.get(configKey);
  }

  @Override
  public String transformPropertyToFlinkCatalog(String configKey) {
    if (configKey.equals(PaimonConstants.CATALOG_BACKEND)) {
      return PaimonConstants.METASTORE;
    }
    return PaimonPropertiesUtils.GRAVITINO_CONFIG_TO_PAIMON.get(configKey);
  }

  @Override
  public String getFlinkCatalogType() {
    return GravitinoPaimonCatalogFactoryOptions.IDENTIFIER;
  }
}
