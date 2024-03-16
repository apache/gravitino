/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.catalog.common;

import com.datastrato.gravitino.catalog.hive.HiveCatalogPropertiesMeta;
import com.datastrato.gravitino.catalog.hive.HiveSchemaPropertiesMetadata;
import com.datastrato.gravitino.catalog.hive.HiveTablePropertiesMetadata;
import com.datastrato.gravitino.catalog.jdbc.JdbcTablePropertiesMetadata;
import com.datastrato.gravitino.catalog.lakehouse.iceberg.IcebergCatalogPropertiesMetadata;
import com.datastrato.gravitino.catalog.lakehouse.iceberg.IcebergSchemaPropertiesMetadata;
import com.datastrato.gravitino.catalog.lakehouse.iceberg.IcebergTablePropertiesMetadata;
import com.datastrato.gravitino.catalog.mysql.MysqlTablePropertiesMetadata;
import com.datastrato.gravitino.connector.BasePropertiesMetadata;
import com.datastrato.gravitino.connector.PropertiesMetadata;
import com.datastrato.gravitino.connector.PropertyEntry;
import java.util.HashSet;
import java.util.Set;

/**
 * The {@link ClassProvider} class serves as a container for the necessary classes used by the
 * Gravitino query engine, with a primary focus on classes related to property metadata.
 *
 * <p>Purpose of this module and class:
 *
 * <pre>
 * - Catalog-related classes are essential for the query engine to directly access catalog information.
 * - The query engine should be able to detect catalog changes and automatically reload catalog-related
 *   information to ensure synchronization.
 * - Including catalog-related jar packages directly is suboptimal for query engines as it may introduce
 *   unnecessary content.
 * </pre>
 *
 * Therefore, this module is used to store the required classes for the query engine's
 * functionality.
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
          add(MysqlTablePropertiesMetadata.class);
          add(JdbcTablePropertiesMetadata.class);
        }
      };

  private static final Set<Class<?>> PG_NEED_CLASS =
      new HashSet<Class<?>>() {
        {
          // TODO
        }
      };
}
