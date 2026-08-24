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
package org.apache.gravitino.lance.common.ops;

import java.util.List;

/**
 * Filters listed Lance metadata names down to the subset the current caller is allowed to see.
 *
 * <p>Listing is paginated inside the namespace operations, so unauthorized entries must be removed
 * before a page is cut. Implementations are supplied by the Lance REST server when authorization is
 * enabled; otherwise {@link #NOOP} keeps the listing untouched.
 */
public interface LanceMetadataFilter {

  /** A filter that returns every listed name unchanged. */
  LanceMetadataFilter NOOP =
      new LanceMetadataFilter() {
        @Override
        public List<String> filterCatalogs(List<String> catalogNames) {
          return catalogNames;
        }

        @Override
        public List<String> filterSchemas(String catalogName, List<String> schemaNames) {
          return schemaNames;
        }
      };

  /**
   * Filters the catalogs listed for the root namespace.
   *
   * @param catalogNames the catalog names to filter.
   * @return the catalog names the current caller may see.
   */
  List<String> filterCatalogs(List<String> catalogNames);

  /**
   * Filters the schemas listed under a catalog.
   *
   * @param catalogName the catalog holding the schemas.
   * @param schemaNames the schema names to filter.
   * @return the schema names the current caller may see.
   */
  List<String> filterSchemas(String catalogName, List<String> schemaNames);
}
