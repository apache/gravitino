/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.catalog.hive;

import com.datastrato.gravitino.connector.BaseCatalogPropertiesMetadata;
import com.datastrato.gravitino.connector.PropertyEntry;
import com.google.common.collect.ImmutableMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class HiveCatalogPropertiesMeta extends BaseCatalogPropertiesMetadata {

  public static final String CLIENT_POOL_SIZE = "client.pool-size";
  public static final int DEFAULT_CLIENT_POOL_SIZE = 1;

  public static final String METASTORE_URIS = "metastore.uris";

  public static final String CLIENT_POOL_CACHE_EVICTION_INTERVAL_MS =
      "client.pool-cache.eviction-interval-ms";

  public static final long DEFAULT_CLIENT_POOL_CACHE_EVICTION_INTERVAL_MS =
      TimeUnit.MINUTES.toMillis(5);

  public static final String IMPERSONATION_ENABLE = "impersonation-enable";

  public static final boolean DEFAULT_IMPERSONATION_ENABLE = false;

  public static final String KET_TAB_URI = "kerberos.keytab-uri";

  public static final String PRINCIPAL = "kerberos.principal";

  public static final String CHECK_INTERVAL_SEC = "kerberos.check-interval-sec";

  public static final String FETCH_TIMEOUT_SEC = "kerberos.keytab-fetch-timeout-sec";

  private static final Map<String, PropertyEntry<?>> HIVE_CATALOG_PROPERTY_ENTRIES =
      ImmutableMap.<String, PropertyEntry<?>>builder()
          .put(
              METASTORE_URIS,
              PropertyEntry.stringRequiredPropertyEntry(
                  METASTORE_URIS, "The Hive metastore URIs", true, false))
          .put(
              CLIENT_POOL_SIZE,
              PropertyEntry.integerOptionalPropertyEntry(
                  CLIENT_POOL_SIZE,
                  "The maximum number of Hive metastore clients in the pool for Gravitino",
                  true,
                  DEFAULT_CLIENT_POOL_SIZE,
                  false))
          .put(
              CLIENT_POOL_CACHE_EVICTION_INTERVAL_MS,
              PropertyEntry.longOptionalPropertyEntry(
                  CLIENT_POOL_CACHE_EVICTION_INTERVAL_MS,
                  "The cache pool eviction interval",
                  true,
                  DEFAULT_CLIENT_POOL_CACHE_EVICTION_INTERVAL_MS,
                  false))
          .put(
              IMPERSONATION_ENABLE,
              PropertyEntry.booleanPropertyEntry(
                  IMPERSONATION_ENABLE,
                  "Enable user impersonation for Hive catalog",
                  false,
                  true,
                  DEFAULT_IMPERSONATION_ENABLE,
                  false,
                  false))
          .put(
              KET_TAB_URI,
              PropertyEntry.stringImmutablePropertyEntry(
                  KET_TAB_URI, "The uri of key tab for the catalog", false, null, false, false))
          .put(
              PRINCIPAL,
              PropertyEntry.stringImmutablePropertyEntry(
                  PRINCIPAL, "The principal for the catalog", false, null, false, false))
          .put(
              CHECK_INTERVAL_SEC,
              PropertyEntry.integerOptionalPropertyEntry(
                  CHECK_INTERVAL_SEC,
                  "The interval to check validness of the principal",
                  true,
                  60,
                  false))
          .put(
              FETCH_TIMEOUT_SEC,
              PropertyEntry.integerOptionalPropertyEntry(
                  FETCH_TIMEOUT_SEC, "The timeout to fetch key tab", true, 60, false))
          .putAll(BASIC_CATALOG_PROPERTY_ENTRIES)
          .build();

  @Override
  protected Map<String, PropertyEntry<?>> specificPropertyEntries() {
    // Currently, Hive catalog only needs to specify the metastore URIs.
    // TODO(yuqi), we can add more properties like username for metastore
    //  (kerberos authentication) later.
    return HIVE_CATALOG_PROPERTY_ENTRIES;
  }
}
