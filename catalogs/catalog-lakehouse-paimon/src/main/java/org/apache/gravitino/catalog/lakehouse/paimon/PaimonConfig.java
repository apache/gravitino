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
package org.apache.gravitino.catalog.lakehouse.paimon;

import java.util.Map;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.Config;
import org.apache.gravitino.config.ConfigBuilder;
import org.apache.gravitino.config.ConfigConstants;
import org.apache.gravitino.config.ConfigEntry;
import org.apache.paimon.options.CatalogOptions;

public class PaimonConfig extends Config {

  public static final ConfigEntry<String> CATALOG_BACKEND =
      new ConfigBuilder(PaimonCatalogPropertiesMetadata.PAIMON_METASTORE)
          .doc(CatalogOptions.METASTORE.description().toString())
          .version(ConfigConstants.VERSION_0_6_0)
          .stringConf()
          .createWithDefault(CatalogOptions.METASTORE.defaultValue());

  public static final ConfigEntry<String> CATALOG_WAREHOUSE =
      new ConfigBuilder(PaimonCatalogPropertiesMetadata.WAREHOUSE)
          .doc(CatalogOptions.WAREHOUSE.description().toString())
          .version(ConfigConstants.VERSION_0_6_0)
          .stringConf()
          .checkValue(StringUtils::isNotBlank, ConfigConstants.NOT_BLANK_ERROR_MSG)
          .create();

  public static final ConfigEntry<String> CATALOG_URI =
      new ConfigBuilder(PaimonCatalogPropertiesMetadata.URI)
          .doc(CatalogOptions.URI.description().toString())
          .version(ConfigConstants.VERSION_0_6_0)
          .stringConf()
          .create();

  public static final ConfigEntry<String> CATALOG_JDBC_USER =
      new ConfigBuilder(PaimonCatalogPropertiesMetadata.GRAVITINO_JDBC_USER)
          .doc("Paimon catalog jdbc user")
          .version(ConfigConstants.VERSION_0_7_0)
          .stringConf()
          .checkValue(StringUtils::isNotBlank, ConfigConstants.NOT_BLANK_ERROR_MSG)
          .create();

  public static final ConfigEntry<String> CATALOG_JDBC_PASSWORD =
      new ConfigBuilder(PaimonCatalogPropertiesMetadata.GRAVITINO_JDBC_PASSWORD)
          .doc("Paimon catalog jdbc password")
          .version(ConfigConstants.VERSION_0_7_0)
          .stringConf()
          .checkValue(StringUtils::isNotBlank, ConfigConstants.NOT_BLANK_ERROR_MSG)
          .create();

  public static final ConfigEntry<String> CATALOG_JDBC_DRIVER =
      new ConfigBuilder(PaimonCatalogPropertiesMetadata.GRAVITINO_JDBC_DRIVER)
          .doc("The driver of the Jdbc connection")
          .version(ConfigConstants.VERSION_0_7_0)
          .stringConf()
          .checkValue(StringUtils::isNotBlank, ConfigConstants.NOT_BLANK_ERROR_MSG)
          .create();

  public PaimonConfig() {
    super(false);
  }

  public PaimonConfig(Map<String, String> properties) {
    super(false);
    loadFromMap(properties, k -> true);
  }

  public String getJdbcDriver() {
    return get(CATALOG_JDBC_DRIVER);
  }
}
