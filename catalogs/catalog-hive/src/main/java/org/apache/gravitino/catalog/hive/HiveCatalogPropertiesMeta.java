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

package org.apache.gravitino.catalog.hive;

import com.google.common.collect.ImmutableMap;
import java.util.Map;
import org.apache.gravitino.connector.AuthorizationPropertiesMeta;
import org.apache.gravitino.connector.BaseCatalogPropertiesMetadata;
import org.apache.gravitino.connector.PropertyEntry;
import org.apache.gravitino.hive.ClientPropertiesMetadata;

public class HiveCatalogPropertiesMeta extends BaseCatalogPropertiesMetadata {

  public static final String CLIENT_POOL_SIZE = HiveConstants.CLIENT_POOL_SIZE;
  public static final String METASTORE_URIS = HiveConstants.METASTORE_URIS;

  public static final String CLIENT_POOL_CACHE_EVICTION_INTERVAL_MS =
      HiveConstants.CLIENT_POOL_CACHE_EVICTION_INTERVAL_MS;

  public static final String IMPERSONATION_ENABLE = HiveConstants.IMPERSONATION_ENABLE;

  public static final boolean DEFAULT_IMPERSONATION_ENABLE = false;

  public static final String KEY_TAB_URI = HiveConstants.KEY_TAB_URI;

  public static final String PRINCIPAL = HiveConstants.PRINCIPAL;

  public static final String CHECK_INTERVAL_SEC = HiveConstants.CHECK_INTERVAL_SEC;

  public static final String FETCH_TIMEOUT_SEC = HiveConstants.FETCH_TIMEOUT_SEC;

  public static final String LIST_ALL_TABLES = HiveConstants.LIST_ALL_TABLES;

  public static final boolean DEFAULT_LIST_ALL_TABLES = false;

  private static final ClientPropertiesMetadata CLIENT_PROPERTIES_METADATA =
      new ClientPropertiesMetadata();

  private static final Map<String, PropertyEntry<?>> HIVE_CATALOG_PROPERTY_ENTRIES =
      ImmutableMap.<String, PropertyEntry<?>>builder()
          .put(
              METASTORE_URIS,
              PropertyEntry.stringRequiredPropertyEntry(
                  METASTORE_URIS,
                  "The Hive metastore URIs",
                  false /* immutable */,
                  false /* hidden */))
          .put(
              IMPERSONATION_ENABLE,
              PropertyEntry.booleanPropertyEntry(
                  IMPERSONATION_ENABLE,
                  "Enable user impersonation for Hive catalog",
                  false /* Whether this property is required */,
                  false /* immutable */,
                  DEFAULT_IMPERSONATION_ENABLE,
                  false /* hidden */,
                  false /* reserved */))
          .put(
              KEY_TAB_URI,
              PropertyEntry.stringOptionalPropertyEntry(
                  KEY_TAB_URI,
                  "The uri of key tab for the catalog",
                  false /* immutable */,
                  null /* defaultValue */,
                  false /* hidden */))
          .put(
              PRINCIPAL,
              PropertyEntry.stringOptionalPropertyEntry(
                  PRINCIPAL,
                  "The principal for the catalog",
                  false /* immutable */,
                  null /* defaultValue */,
                  false /* hidden */))
          .put(
              CHECK_INTERVAL_SEC,
              PropertyEntry.integerOptionalPropertyEntry(
                  CHECK_INTERVAL_SEC,
                  "The interval to check validness of the principal",
                  false /* immutable */,
                  60 /* defaultValue */,
                  false /* hidden */))
          .put(
              FETCH_TIMEOUT_SEC,
              PropertyEntry.integerOptionalPropertyEntry(
                  FETCH_TIMEOUT_SEC, "The timeout to fetch key tab", false, 60, false))
          .put(
              LIST_ALL_TABLES,
              PropertyEntry.booleanPropertyEntry(
                  LIST_ALL_TABLES,
                  "Lists all tables in a database, including non-Hive tables, such as Iceberg, etc.",
                  false /* required */,
                  false /* immutable */,
                  DEFAULT_LIST_ALL_TABLES,
                  false /* hidden */,
                  false /* reserved */))
          .putAll(AuthorizationPropertiesMeta.RANGER_AUTHORIZATION_PROPERTY_ENTRIES)
          .putAll(CLIENT_PROPERTIES_METADATA.propertyEntries())
          .build();

  @Override
  protected Map<String, PropertyEntry<?>> specificPropertyEntries() {
    // Currently, Hive catalog only needs to specify the metastore URIs.
    // TODO(yuqi), we can add more properties like username for metastore
    //  (kerberos authentication) later.
    return HIVE_CATALOG_PROPERTY_ENTRIES;
  }
}
