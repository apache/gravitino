/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.catalog.hive;

import com.datastrato.gravitino.catalog.BaseCatalogPropertiesMetadata;
import com.datastrato.gravitino.catalog.PropertyEntry;
import com.datastrato.gravitino.catalog.hive.HiveTablePropertiesMetadata.StorageFormat;
import com.google.common.collect.ImmutableMap;
import java.util.Map;

public class HiveCatalogPropertiesMeta extends BaseCatalogPropertiesMetadata {

  public static final String CLIENT_POOL_SIZE = "client.pool-size";
  public static final int DEFAULT_CLIENT_POOL_SIZE = 1;

  public static final String METASTORE_URIS = "metastore.uris";

  // Add these two general configurations for hive catalog.
  public static final String STORAGE_FORMAT = "hive.storage-format";
  public static final String COMPRESSION_CODEC = "hive.compression-codec";

  enum CompressionCodeC {
    NONE,
    SNAPPY,
    GZIP,
    LZO,
    LZ4,
    ZSTD,
    BZIP2,
    ZLIB
  }

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
              STORAGE_FORMAT,
              PropertyEntry.enumImmutablePropertyEntry(
                  STORAGE_FORMAT,
                  "The default storage format for Hive tables",
                  false,
                  StorageFormat.class,
                  StorageFormat.ORC,
                  false,
                  false))
          .put(
              COMPRESSION_CODEC,
              PropertyEntry.enumImmutablePropertyEntry(
                  COMPRESSION_CODEC,
                  "The compression codec to use when writing files.",
                  false,
                  CompressionCodeC.class,
                  CompressionCodeC.GZIP,
                  false,
                  false))
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
