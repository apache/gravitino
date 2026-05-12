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

package org.apache.gravitino.flink.connector.utils;

import java.util.List;
import java.util.Map;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.ResolvedCatalogTable;

/** Typed compatibility hook that is implemented per Flink version. */
public interface CatalogCompat {

  /**
   * Create a {@link CatalogTable} using the active Flink version's constructor and behavior.
   *
   * @param schema table schema
   * @param comment table comment
   * @param partitionKeys partition column names
   * @param options table options
   * @return a version-compatible catalog table
   */
  CatalogTable createCatalogTable(
      Schema schema, String comment, List<String> partitionKeys, Map<String, String> options);

  /**
   * Serialize a resolved catalog table using the active Flink version's helper implementation.
   *
   * @param resolvedTable resolved catalog table
   * @return serialized table properties
   */
  Map<String, String> serializeCatalogTable(ResolvedCatalogTable resolvedTable);
}
