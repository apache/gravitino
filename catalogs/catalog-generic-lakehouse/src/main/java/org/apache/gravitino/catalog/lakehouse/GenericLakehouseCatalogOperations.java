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
package org.apache.gravitino.catalog.lakehouse;

import java.util.Map;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.connector.CatalogInfo;
import org.apache.gravitino.connector.CatalogOperations;
import org.apache.gravitino.connector.HasPropertyMetadata;
import org.apache.gravitino.connector.SupportsSchemas;
import org.apache.gravitino.exceptions.NoSuchCatalogException;
import org.apache.gravitino.exceptions.NoSuchSchemaException;
import org.apache.gravitino.exceptions.NoSuchTableException;
import org.apache.gravitino.exceptions.NonEmptySchemaException;
import org.apache.gravitino.exceptions.SchemaAlreadyExistsException;
import org.apache.gravitino.exceptions.TableAlreadyExistsException;
import org.apache.gravitino.rel.Column;
import org.apache.gravitino.rel.Table;
import org.apache.gravitino.rel.TableCatalog;
import org.apache.gravitino.rel.TableChange;
import org.apache.gravitino.rel.expressions.distributions.Distribution;
import org.apache.gravitino.rel.expressions.sorts.SortOrder;
import org.apache.gravitino.rel.expressions.transforms.Transform;
import org.apache.gravitino.rel.indexes.Index;

/** Operations for interacting with a generic lakehouse catalog in Apache Gravitino. */
public class GenericLakehouseCatalogOperations
    implements CatalogOperations, SupportsSchemas, TableCatalog {

  /**
   * Initializes the generic lakehouse catalog operations with the provided configuration.
   *
   * @param conf The configuration map for the generic catalog operations.
   * @param info The catalog info associated with this operation instance.
   * @param propertiesMetadata The properties metadata of generic lakehouse catalog.
   * @throws RuntimeException if initialization fails.
   */
  @Override
  public void initialize(
      Map<String, String> conf, CatalogInfo info, HasPropertyMetadata propertiesMetadata)
      throws RuntimeException {
    // TODO: Implement initialization logic
  }

  @Override
  public void close() {}

  @Override
  public void testConnection(
      NameIdentifier catalogIdent,
      org.apache.gravitino.Catalog.Type type,
      String provider,
      String comment,
      Map<String, String> properties)
      throws Exception {
    throw new UnsupportedOperationException("Not implemented yet.");
  }

  @Override
  public org.apache.gravitino.NameIdentifier[] listSchemas(org.apache.gravitino.Namespace namespace)
      throws NoSuchCatalogException {
    throw new UnsupportedOperationException("Not implemented yet.");
  }

  @Override
  public org.apache.gravitino.Schema createSchema(
      org.apache.gravitino.NameIdentifier ident, String comment, Map<String, String> properties)
      throws NoSuchCatalogException, SchemaAlreadyExistsException {
    throw new UnsupportedOperationException("Not implemented yet.");
  }

  @Override
  public org.apache.gravitino.Schema loadSchema(org.apache.gravitino.NameIdentifier ident)
      throws NoSuchSchemaException {
    throw new UnsupportedOperationException("Not implemented yet.");
  }

  @Override
  public org.apache.gravitino.Schema alterSchema(
      org.apache.gravitino.NameIdentifier ident, org.apache.gravitino.SchemaChange... changes)
      throws NoSuchSchemaException {
    throw new UnsupportedOperationException("Not implemented yet.");
  }

  @Override
  public boolean dropSchema(org.apache.gravitino.NameIdentifier ident, boolean cascade)
      throws NonEmptySchemaException {
    throw new UnsupportedOperationException("Not implemented yet.");
  }

  @Override
  public NameIdentifier[] listTables(Namespace namespace) throws NoSuchSchemaException {
    throw new UnsupportedOperationException("Not implemented yet.");
  }

  @Override
  public Table loadTable(NameIdentifier ident) throws NoSuchTableException {
    throw new UnsupportedOperationException("Not implemented yet.");
  }

  @Override
  public Table createTable(
      NameIdentifier ident,
      Column[] columns,
      String comment,
      Map<String, String> properties,
      Transform[] partitions,
      Distribution distribution,
      SortOrder[] sortOrders,
      Index[] indexes)
      throws NoSuchSchemaException, TableAlreadyExistsException {
    throw new UnsupportedOperationException("Not implemented yet.");
  }

  @Override
  public Table alterTable(NameIdentifier ident, TableChange... changes)
      throws NoSuchTableException, IllegalArgumentException {
    throw new UnsupportedOperationException("Not implemented yet.");
  }

  @Override
  public boolean dropTable(NameIdentifier ident) {
    throw new UnsupportedOperationException("Not implemented yet.");
  }
}
