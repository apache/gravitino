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
package org.apache.gravitino.iceberg.common.ops;

import java.util.ArrayList;
import java.util.List;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.iceberg.common.utils.IcebergIdentifierUtils;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.rest.responses.ListNamespacesResponse;
import org.apache.iceberg.rest.responses.ListTablesResponse;

/**
 * Lists Iceberg namespaces / tables through {@link IcebergCatalogWrapper} and maps them to
 * Gravitino identifiers / logical schema names.
 */
public final class IcebergCatalogListings {

  private IcebergCatalogListings() {}

  /**
   * Lists top-level Iceberg namespaces under the catalog as logical schema names.
   *
   * @param wrapper catalog wrapper
   * @param separator hierarchical schema separator
   * @return logical schema names
   */
  public static List<String> listLogicalSchemas(IcebergCatalogWrapper wrapper, String separator) {
    ListNamespacesResponse listed = wrapper.listNamespace(Namespace.empty());
    List<String> schemas = new ArrayList<>();
    for (Namespace icebergNs : listed.namespaces()) {
      if (icebergNs == null || icebergNs.isEmpty()) {
        continue;
      }
      schemas.add(IcebergIdentifierUtils.icebergNamespaceToSchemaName(icebergNs, separator));
    }
    return schemas;
  }

  /**
   * Lists tables in the given logical schema as Gravitino table identifiers.
   *
   * @param wrapper catalog wrapper
   * @param metalake metalake name
   * @param catalogName catalog name
   * @param schemaName logical schema name
   * @param separator hierarchical schema separator
   * @return Gravitino table name identifiers
   */
  public static List<NameIdentifier> listTables(
      IcebergCatalogWrapper wrapper,
      String metalake,
      String catalogName,
      String schemaName,
      String separator) {
    Namespace icebergNs =
        IcebergIdentifierUtils.getIcebergNamespaceFromSchemaName(schemaName, separator);
    ListTablesResponse listed = wrapper.listTable(icebergNs);
    List<NameIdentifier> tables = new ArrayList<>();
    for (TableIdentifier icebergTable : listed.identifiers()) {
      tables.add(
          IcebergIdentifierUtils.toGravitinoTableIdentifier(
              metalake, catalogName, icebergTable, separator));
    }
    return tables;
  }
}
