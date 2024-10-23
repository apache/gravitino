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

import static org.apache.gravitino.connector.PropertyEntry.enumPropertyEntry;
import static org.apache.gravitino.connector.PropertyEntry.stringOptionalPropertyEntry;
import static org.apache.gravitino.connector.PropertyEntry.stringRequiredPropertyEntry;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.gravitino.catalog.lakehouse.paimon.authentication.AuthenticationConfig;
import org.apache.gravitino.catalog.lakehouse.paimon.authentication.kerberos.KerberosConfig;
import org.apache.gravitino.catalog.lakehouse.paimon.storage.PaimonOSSFileSystemConfig;
import org.apache.gravitino.catalog.lakehouse.paimon.storage.PaimonS3FileSystemConfig;
import org.apache.gravitino.catalog.lakehouse.paimon.utils.CatalogUtils;
import org.apache.gravitino.connector.BaseCatalogPropertiesMetadata;
import org.apache.gravitino.connector.PropertiesMetadata;
import org.apache.gravitino.connector.PropertyEntry;
import org.apache.gravitino.storage.OSSProperties;
import org.apache.gravitino.storage.S3Properties;

/**
 * Implementation of {@link PropertiesMetadata} that represents Paimon catalog properties metadata.
 */
public class PaimonCatalogPropertiesMetadata extends BaseCatalogPropertiesMetadata {

  @VisibleForTesting public static final String GRAVITINO_CATALOG_BACKEND = "catalog-backend";
  public static final String PAIMON_METASTORE = "metastore";
  public static final String WAREHOUSE = "warehouse";
  public static final String URI = "uri";
  public static final String GRAVITINO_JDBC_USER = "jdbc-user";
  public static final String PAIMON_JDBC_USER = "jdbc.user";
  public static final String GRAVITINO_JDBC_PASSWORD = "jdbc-password";
  public static final String PAIMON_JDBC_PASSWORD = "jdbc.password";

  // S3 properties needed by Paimon
  public static final String S3_ENDPOINT = "s3.endpoint";
  public static final String S3_ACCESS_KEY = "s3.access-key";
  public static final String S3_SECRET_KEY = "s3.secret-key";

  public static final Map<String, String> GRAVITINO_CONFIG_TO_PAIMON =
      ImmutableMap.of(
          GRAVITINO_CATALOG_BACKEND,
          PAIMON_METASTORE,
          WAREHOUSE,
          WAREHOUSE,
          URI,
          URI,
          GRAVITINO_JDBC_USER,
          PAIMON_JDBC_USER,
          GRAVITINO_JDBC_PASSWORD,
          PAIMON_JDBC_PASSWORD);
  private static final Map<String, PropertyEntry<?>> PROPERTIES_METADATA;
  public static final Map<String, String> KERBEROS_CONFIGURATION =
      ImmutableMap.of(
          KerberosConfig.PRINCIPAL_KEY,
          KerberosConfig.PRINCIPAL_KEY,
          KerberosConfig.KEY_TAB_URI_KEY,
          KerberosConfig.KEY_TAB_URI_KEY,
          KerberosConfig.CHECK_INTERVAL_SEC_KEY,
          KerberosConfig.CHECK_INTERVAL_SEC_KEY,
          KerberosConfig.FETCH_TIMEOUT_SEC_KEY,
          KerberosConfig.FETCH_TIMEOUT_SEC_KEY,
          AuthenticationConfig.AUTH_TYPE_KEY,
          AuthenticationConfig.AUTH_TYPE_KEY);

  public static final Map<String, String> S3_CONFIGURATION =
      ImmutableMap.of(
          S3Properties.GRAVITINO_S3_ACCESS_KEY_ID, S3_ACCESS_KEY,
          S3Properties.GRAVITINO_S3_SECRET_ACCESS_KEY, S3_SECRET_KEY,
          S3Properties.GRAVITINO_S3_ENDPOINT, S3_ENDPOINT);

  public static final Map<String, String> OSS_CONFIGURATION =
      ImmutableMap.of(
          OSSProperties.GRAVITINO_OSS_ACCESS_KEY_ID, PaimonOSSFileSystemConfig.OSS_ACCESS_KEY,
          OSSProperties.GRAVITINO_OSS_ACCESS_KEY_SECRET, PaimonOSSFileSystemConfig.OSS_SECRET_KEY,
          OSSProperties.GRAVITINO_OSS_ENDPOINT, PaimonOSSFileSystemConfig.OSS_ENDPOINT);

  static {
    List<PropertyEntry<?>> propertyEntries =
        ImmutableList.of(
            enumPropertyEntry(
                GRAVITINO_CATALOG_BACKEND,
                "Paimon catalog backend type",
                true /* required */,
                true /* immutable */,
                PaimonCatalogBackend.class /* enumClass */,
                null /* defaultValue */,
                false /* hidden */,
                false /* reserved */),
            stringRequiredPropertyEntry(
                WAREHOUSE,
                "Paimon catalog warehouse config",
                false /* immutable */,
                false /* hidden */),
            stringOptionalPropertyEntry(
                URI,
                "Paimon catalog uri config",
                false /* immutable */,
                null /* defaultValue */,
                false /* hidden */),
            stringOptionalPropertyEntry(
                GRAVITINO_JDBC_USER,
                "Gravitino Paimon catalog jdbc user",
                false /* immutable */,
                null /* defaultValue */,
                true /* hidden */),
            stringOptionalPropertyEntry(
                GRAVITINO_JDBC_PASSWORD,
                "Gravitino Paimon catalog jdbc password",
                false /* immutable */,
                null /* defaultValue */,
                true /* hidden */));
    HashMap<String, PropertyEntry<?>> result = Maps.newHashMap();
    result.putAll(Maps.uniqueIndex(propertyEntries, PropertyEntry::getName));
    result.putAll(KerberosConfig.KERBEROS_PROPERTY_ENTRIES);
    result.putAll(AuthenticationConfig.AUTHENTICATION_PROPERTY_ENTRIES);
    result.putAll(PaimonS3FileSystemConfig.S3_FILESYSTEM_PROPERTY_ENTRIES);
    result.putAll(PaimonOSSFileSystemConfig.OSS_FILESYSTEM_PROPERTY_ENTRIES);
    PROPERTIES_METADATA = ImmutableMap.copyOf(result);
  }

  @Override
  protected Map<String, PropertyEntry<?>> specificPropertyEntries() {
    return PROPERTIES_METADATA;
  }

  Map<String, String> transformProperties(Map<String, String> properties) {
    return CatalogUtils.toInnerProperty(properties, false);
  }
}
