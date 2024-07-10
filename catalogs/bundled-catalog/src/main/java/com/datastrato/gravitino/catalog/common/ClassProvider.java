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

package com.datastrato.gravitino.catalog.common;

import com.datastrato.gravitino.catalog.hive.HiveCatalogPropertiesMeta;
import com.datastrato.gravitino.catalog.hive.HiveSchemaPropertiesMetadata;
import com.datastrato.gravitino.catalog.hive.HiveTablePropertiesMetadata;
import com.datastrato.gravitino.catalog.jdbc.JdbcTablePropertiesMetadata;
import com.datastrato.gravitino.catalog.mysql.MysqlTablePropertiesMetadata;
import com.datastrato.gravitino.connector.BasePropertiesMetadata;
import com.datastrato.gravitino.connector.PropertiesMetadata;
import com.datastrato.gravitino.connector.PropertyEntry;
import java.util.HashSet;
import java.util.Set;

/**
 * The {@link ClassProvider} class serves as a container for the necessary classes used by the
 * Apache Gravitino query engine, with a primary focus on classes related to property metadata.
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
@SuppressWarnings("UnusedVariable")
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
