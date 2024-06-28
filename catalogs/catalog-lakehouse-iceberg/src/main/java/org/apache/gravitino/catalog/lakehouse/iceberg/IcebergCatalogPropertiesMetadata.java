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
package org.apache.gravitino.catalog.lakehouse.iceberg;

import static org.apache.gravitino.connector.PropertyEntry.enumImmutablePropertyEntry;
import static org.apache.gravitino.connector.PropertyEntry.stringRequiredPropertyEntry;

import org.apache.gravitino.iceberg.common.IcebergCatalogBackend;
import org.apache.gravitino.iceberg.common.IcebergConstants;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.gravitino.connector.BaseCatalogPropertiesMetadata;
import org.apache.gravitino.connector.PropertyEntry;
import org.apache.gravitino.iceberg.common.authentication.AuthenticationConfig;
import org.apache.gravitino.iceberg.common.authentication.kerberos.KerberosConfig;

public class IcebergCatalogPropertiesMetadata extends BaseCatalogPropertiesMetadata {
  public static final String CATALOG_BACKEND = IcebergConstants.CATALOG_BACKEND;

  public static final String GRAVITINO_JDBC_USER = IcebergConstants.GRAVITINO_JDBC_USER;
  public static final String ICEBERG_JDBC_USER = IcebergConstants.ICEBERG_JDBC_USER;

  public static final String GRAVITINO_JDBC_PASSWORD = IcebergConstants.GRAVITINO_JDBC_PASSWORD;
  public static final String ICEBERG_JDBC_PASSWORD = IcebergConstants.ICEBERG_JDBC_PASSWORD;
  public static final String ICEBERG_JDBC_INITIALIZE = IcebergConstants.ICEBERG_JDBC_INITIALIZE;

  public static final String GRAVITINO_JDBC_DRIVER = IcebergConstants.GRAVITINO_JDBC_DRIVER;
  public static final String WAREHOUSE = IcebergConstants.WAREHOUSE;
  public static final String URI = IcebergConstants.URI;
  public static final String CATALOG_BACKEND_NAME = IcebergConstants.CATALOG_BACKEND_NAME;

  private static final Map<String, PropertyEntry<?>> PROPERTIES_METADATA;

  // Map that maintains the mapping of keys in Gravitino to that in Iceberg, for example, users
  // will only need to set the configuration 'catalog-backend' in Gravitino and Gravitino will
  // change it to `catalogType` automatically and pass it to Iceberg.
  public static final Map<String, String> GRAVITINO_CONFIG_TO_ICEBERG =
      ImmutableMap.of(
          CATALOG_BACKEND,
          CATALOG_BACKEND,
          GRAVITINO_JDBC_DRIVER,
          GRAVITINO_JDBC_DRIVER,
          GRAVITINO_JDBC_USER,
          ICEBERG_JDBC_USER,
          GRAVITINO_JDBC_PASSWORD,
          ICEBERG_JDBC_PASSWORD,
          URI,
          URI,
          WAREHOUSE,
          WAREHOUSE,
          CATALOG_BACKEND_NAME,
          CATALOG_BACKEND_NAME);

  public static final Map<String, String> KERBEROS_CONFIGURATION_FOR_HIVE_BACKEND =
      ImmutableMap.of(
          KerberosConfig.PRINCIPAL_KEY,
          KerberosConfig.PRINCIPAL_KEY,
          KerberosConfig.KET_TAB_URI_KEY,
          KerberosConfig.KET_TAB_URI_KEY,
          KerberosConfig.CHECK_INTERVAL_SEC_KEY,
          KerberosConfig.CHECK_INTERVAL_SEC_KEY,
          KerberosConfig.FETCH_TIMEOUT_SEC_KEY,
          KerberosConfig.FETCH_TIMEOUT_SEC_KEY,
          AuthenticationConfig.IMPERSONATION_ENABLE_KEY,
          AuthenticationConfig.IMPERSONATION_ENABLE_KEY,
          AuthenticationConfig.AUTH_TYPE_KEY,
          AuthenticationConfig.AUTH_TYPE_KEY);

  static {
    List<PropertyEntry<?>> propertyEntries =
        ImmutableList.of(
            enumImmutablePropertyEntry(
                CATALOG_BACKEND,
                "Iceberg catalog type choose properties",
                true,
                IcebergCatalogBackend.class,
                null,
                false,
                false),
            stringRequiredPropertyEntry(URI, "Iceberg catalog uri config", false, false),
            stringRequiredPropertyEntry(
                WAREHOUSE, "Iceberg catalog warehouse config", false, false));
    HashMap<String, PropertyEntry<?>> result = Maps.newHashMap(BASIC_CATALOG_PROPERTY_ENTRIES);
    result.putAll(Maps.uniqueIndex(propertyEntries, PropertyEntry::getName));
    result.putAll(KerberosConfig.KERBEROS_PROPERTY_ENTRIES);
    result.putAll(AuthenticationConfig.AUTHENTICATION_PROPERTY_ENTRIES);
    PROPERTIES_METADATA = ImmutableMap.copyOf(result);
  }

  @Override
  protected Map<String, PropertyEntry<?>> specificPropertyEntries() {
    return PROPERTIES_METADATA;
  }

  public Map<String, String> transformProperties(Map<String, String> properties) {
    Map<String, String> gravitinoConfig = Maps.newHashMap();
    properties.forEach(
        (key, value) -> {
          if (GRAVITINO_CONFIG_TO_ICEBERG.containsKey(key)) {
            gravitinoConfig.put(GRAVITINO_CONFIG_TO_ICEBERG.get(key), value);
          }

          if (KERBEROS_CONFIGURATION_FOR_HIVE_BACKEND.containsKey(key)) {
            gravitinoConfig.put(KERBEROS_CONFIGURATION_FOR_HIVE_BACKEND.get(key), value);
          }
        });
    return gravitinoConfig;
  }
}
