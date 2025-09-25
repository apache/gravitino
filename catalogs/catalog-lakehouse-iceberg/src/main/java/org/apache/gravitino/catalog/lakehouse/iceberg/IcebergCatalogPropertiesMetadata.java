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
import static org.apache.gravitino.connector.PropertyEntry.integerOptionalPropertyEntry;
import static org.apache.gravitino.connector.PropertyEntry.stringOptionalPropertyEntry;
import static org.apache.gravitino.connector.PropertyEntry.stringRequiredPropertyEntry;

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
import org.apache.gravitino.storage.AzureProperties;
import org.apache.gravitino.storage.OSSProperties;
import org.apache.gravitino.storage.S3Properties;

public class IcebergCatalogPropertiesMetadata extends BaseCatalogPropertiesMetadata {
  public static final String CATALOG_BACKEND = IcebergConstants.CATALOG_BACKEND;
  public static final String GRAVITINO_JDBC_USER = IcebergConstants.GRAVITINO_JDBC_USER;
  public static final String GRAVITINO_JDBC_PASSWORD = IcebergConstants.GRAVITINO_JDBC_PASSWORD;
  public static final String WAREHOUSE = IcebergConstants.WAREHOUSE;
  public static final String URI = IcebergConstants.URI;
  public static final String CATALOG_BACKEND_NAME = IcebergConstants.CATALOG_BACKEND_NAME;

  private static final Map<String, PropertyEntry<?>> PROPERTIES_METADATA;

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
                true /* required */,
                IcebergCatalogBackend.class,
                null /* defaultValue */,
                false /* hidden */,
                false /* reserved */),
            stringRequiredPropertyEntry(
                URI, "Iceberg catalog uri config", false /* immutable */, false /* hidden */),
            stringOptionalPropertyEntry(
                WAREHOUSE,
                "Iceberg catalog warehouse config",
                false /* immutable */,
                null, /* defaultValue */
                false /* hidden */),
            stringOptionalPropertyEntry(
                IcebergConstants.IO_IMPL,
                "FileIO implement for Iceberg",
                true /* immutable */,
                null /* defaultValue */,
                false /* hidden */),
            stringOptionalPropertyEntry(
                S3Properties.GRAVITINO_S3_ACCESS_KEY_ID,
                "s3 access key ID",
                false /* immutable */,
                null /* defaultValue */,
                false /* hidden */),
            stringOptionalPropertyEntry(
                S3Properties.GRAVITINO_S3_SECRET_ACCESS_KEY,
                "s3 secret access key",
                false /* immutable */,
                null /* defaultValue */,
                false /* hidden */),
            stringOptionalPropertyEntry(
                OSSProperties.GRAVITINO_OSS_ACCESS_KEY_ID,
                "OSS access key ID",
                false /* immutable */,
                null /* defaultValue */,
                false /* hidden */),
            stringOptionalPropertyEntry(
                OSSProperties.GRAVITINO_OSS_ACCESS_KEY_SECRET,
                "OSS access key secret",
                false /* immutable */,
                null /* defaultValue */,
                false /* hidden */),
            stringOptionalPropertyEntry(
                AzureProperties.GRAVITINO_AZURE_STORAGE_ACCOUNT_NAME,
                "Azure storage account name",
                false /* immutable */,
                null /* defaultValue */,
                false /* hidden */),
            stringOptionalPropertyEntry(
                AzureProperties.GRAVITINO_AZURE_STORAGE_ACCOUNT_KEY,
                "Azure storage account key",
                false /* immutable */,
                null /* defaultValue */,
                false /* hidden */),
            stringOptionalPropertyEntry(
                IcebergConstants.TABLE_METADATA_CACHE_IMPL,
                "Table metadata cache implementation",
                false /* immutable */,
                null /* defaultValue */,
                false /* hidden */),
            integerOptionalPropertyEntry(
                IcebergConstants.TABLE_METADATA_CACHE_CAPACITY,
                "Table metadata cache capacity",
                false /* immutable */,
                200 /* defaultValue */,
                false /* hidden */),
            integerOptionalPropertyEntry(
                IcebergConstants.TABLE_METADATA_CACHE_EXPIRE_MINUTES,
                "Table metadata cache TTL in minutes",
                false /* immutable */,
                60 /* defaultValue */,
                false /* hidden */));
    HashMap<String, PropertyEntry<?>> result = Maps.newHashMap();
    result.putAll(Maps.uniqueIndex(propertyEntries, PropertyEntry::getName));
    result.putAll(KerberosConfig.KERBEROS_PROPERTY_ENTRIES);
    result.putAll(AuthenticationConfig.AUTHENTICATION_PROPERTY_ENTRIES);
    PROPERTIES_METADATA = ImmutableMap.copyOf(result);
  }

  @Override
  protected Map<String, PropertyEntry<?>> specificPropertyEntries() {
    return PROPERTIES_METADATA;
  }

  public Map<String, String> transformProperties(Map<String, String> gravitinoProperties) {
    Map<String, String> icebergProperties =
        IcebergPropertiesUtils.toIcebergCatalogProperties(gravitinoProperties);
    gravitinoProperties.forEach(
        (k, v) -> {
          if (KERBEROS_CONFIGURATION_FOR_HIVE_BACKEND.containsKey(k)) {
            icebergProperties.put(KERBEROS_CONFIGURATION_FOR_HIVE_BACKEND.get(k), v);
          }
        });
    return icebergProperties;
  }
}
