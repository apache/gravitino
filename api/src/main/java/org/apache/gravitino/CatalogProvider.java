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
package org.apache.gravitino;

import java.util.Locale;
import org.apache.gravitino.annotation.Evolving;

/**
 * A Catalog provider is a class that provides a short name for a catalog. This short name is used
 * when creating a catalog.
 *
 * <p>There are two kinds of catalogs in Gravitino, one is managed catalog and another is external
 * catalog.
 *
 * <p>Managed catalog: A catalog and its subsidiary objects are all managed by Gravitino. Gravitino
 * takes care of the lifecycle of the catalog and its objects. For those catalogs, Gravitino uses
 * the type of the catalog as the provider short name. Note that for each catalog type, there is
 * only one implementation of managed catalog for that type. Currently, Gravitino has model and
 * fileset catalogs that are managed catalogs.
 *
 * <p>External catalog: A catalog its subsidiary objects are stored by an external sources, such as
 * Hive catalog, the DB and tables are stored in HMS. For those catalogs, Gravitino uses a unique
 * name as the provider short name to load the catalog. For example, Hive catalog uses "hive" as the
 * provider short name.
 */
@Evolving
public interface CatalogProvider {

  /**
   * Form the provider short name for a managed catalog. The provider short name for a managed
   * catalog is the catalog type in lowercase.
   *
   * @param type The catalog type.
   * @return The provider short name for the managed catalog.
   */
  static String shortNameForManagedCatalog(Catalog.Type type) {
    return type.name().toLowerCase(Locale.ROOT);
  }

  /**
   * The string that represents the catalog that this provider uses. This is overridden by children
   * to provide a nice alias for the catalog.
   *
   * @return The string that represents the catalog that this provider uses.
   */
  String shortName();
}
