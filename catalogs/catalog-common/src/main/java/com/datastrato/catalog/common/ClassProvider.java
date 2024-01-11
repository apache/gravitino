/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.catalog.common;

import com.datastrato.gravitino.catalog.BasePropertiesMetadata;
import com.datastrato.gravitino.catalog.PropertiesMetadata;
import com.datastrato.gravitino.catalog.PropertyEntry;
import com.datastrato.gravitino.catalog.hive.HiveCatalogPropertiesMeta;
import com.datastrato.gravitino.catalog.hive.HiveSchemaPropertiesMetadata;
import com.datastrato.gravitino.catalog.hive.HiveTablePropertiesMetadata;
import com.datastrato.gravitino.catalog.lakehouse.iceberg.IcebergCatalogPropertiesMetadata;
import com.datastrato.gravitino.catalog.lakehouse.iceberg.IcebergSchemaPropertiesMetadata;
import com.datastrato.gravitino.catalog.lakehouse.iceberg.IcebergTablePropertiesMetadata;
import java.util.HashSet;
import java.util.Set;

/**
 * {@link ClassProvider} holds the set of classes that are required for the query engine to use
 * Gravitino, currently, it mainly focuses on the classes that are required for the property
 * metadata.
 */
public class ClassProvider {

  private static final Set<Class<?>> BASE_CLASS =
      new HashSet<Class<?>>() {
        {
          add(BasePropertiesMetadata.class);
          add(PropertyEntry.class);
          add(PropertiesMetadata.class);
        }
      };

  private static final Set<Class<?>> HIVE_NEED_CLASS =
      new HashSet<Class<?>>() {
        {
          add(HiveTablePropertiesMetadata.class);
          add(HiveSchemaPropertiesMetadata.class);
          add(HiveCatalogPropertiesMeta.class);
        }
      };

  private static final Set<Class<?>> ICEBERG_NEED_CLASS =
      new HashSet<Class<?>>() {
        {
          add(IcebergTablePropertiesMetadata.class);
          add(IcebergSchemaPropertiesMetadata.class);
          add(IcebergCatalogPropertiesMetadata.class);
        }
      };

  private static final Set<Class<?>> MYSQL_NEED_CLASS =
      new HashSet<Class<?>>() {
        {
          // TODO
        }
      };

  private static final Set<Class<?>> PG_NEED_CLASS =
      new HashSet<Class<?>>() {
        {
          // TODO
        }
      };
}
