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
package org.apache.gravitino.catalog.lakehouse.hudi;

import com.google.common.annotations.VisibleForTesting;
import java.util.Map;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.Schema;
import org.apache.gravitino.SchemaChange;
import org.apache.gravitino.catalog.lakehouse.hudi.backend.HudiCatalogBackend;
import org.apache.gravitino.catalog.lakehouse.hudi.ops.HudiCatalogBackendOps;
import org.apache.gravitino.catalog.lakehouse.hudi.utils.CatalogUtils;
import org.apache.gravitino.connector.CatalogInfo;
import org.apache.gravitino.connector.CatalogOperations;
import org.apache.gravitino.connector.HasPropertyMetadata;
import org.apache.gravitino.connector.SupportsSchemas;
import org.apache.gravitino.exceptions.ConnectionFailedException;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Operations for Interacting with Hudi Catalog. */
public class HudiCatalogOperations implements CatalogOperations, SupportsSchemas, TableCatalog {

  private static final Logger LOG = LoggerFactory.getLogger(HudiCatalogOperations.class);

  @VisibleForTesting HudiCatalogBackendOps hudiCatalogBackendOps;

  /**
   * Load the Hudi Catalog Backend and initialize the Hudi Catalog Operations.
   *
   * @param config The configuration of this Catalog.
   * @param info The information of this Catalog.
   * @param propertiesMetadata The properties metadata of this Catalog.
   * @throws RuntimeException if failed to initialize the Hudi Catalog Operations.
   */
  @Override
  public void initialize(
      Map<String, String> config, CatalogInfo info, HasPropertyMetadata propertiesMetadata)
      throws RuntimeException {
    HudiCatalogBackend hudiCatalogBackend = CatalogUtils.loadHudiCatalogBackend(config);
    hudiCatalogBackendOps = hudiCatalogBackend.backendOps();
  }

  /**
   * Performs `listSchemas` operation on the Hudi Catalog to test the catalog connection.
   *
   * @param catalogIdent the name of the catalog.
   * @param type the type of the catalog.
   * @param provider the provider of the catalog.
   * @param comment the comment of the catalog.
   * @param properties the properties of the catalog.
   * @throws Exception if failed to run `listSchemas` operation on the Hudi Catalog.
   */
  @Override
  public void testConnection(
      NameIdentifier catalogIdent,
      Catalog.Type type,
      String provider,
      String comment,
      Map<String, String> properties)
      throws Exception {
    try {
      hudiCatalogBackendOps.listSchemas(
          Namespace.of(catalogIdent.namespace().level(0), catalogIdent.name()));
    } catch (Exception e) {
      throw new ConnectionFailedException(
          e, "Failed to run listSchemas on Hudi catalog: %s", e.getMessage());
    }
  }

  @Override
  public void close() {
    if (hudiCatalogBackendOps != null) {
      try {
        hudiCatalogBackendOps.close();
      } catch (Exception e) {
        LOG.warn("Failed to close Hudi catalog", e);
      }
    }
  }

  /**
   * List the schemas in the Hudi Catalog.
   *
   * @param namespace The namespace to list.
   * @return The list of schemas in the Hudi Catalog.
   * @throws NoSuchCatalogException if the catalog does not exist.
   */
  @Override
  public NameIdentifier[] listSchemas(Namespace namespace) throws NoSuchCatalogException {
    return hudiCatalogBackendOps.listSchemas(namespace);
  }

  /**
   * Create a schema in the Hudi Catalog.
   *
   * @param ident The name identifier of the schema.
   * @param comment The comment of the schema.
   * @param properties The properties of the schema.
   * @return The created schema.
   * @throws NoSuchCatalogException if the catalog does not exist.
   * @throws SchemaAlreadyExistsException if the schema already exists.
   */
  @Override
  public Schema createSchema(NameIdentifier ident, String comment, Map<String, String> properties)
      throws NoSuchCatalogException, SchemaAlreadyExistsException {
    return hudiCatalogBackendOps.createSchema(ident, comment, properties);
  }

  /**
   * Load a schema from the Hudi Catalog.
   *
   * @param ident The name identifier of the schema.
   * @return The loaded schema.
   * @throws NoSuchSchemaException if the schema does not exist.
   */
  @Override
  public Schema loadSchema(NameIdentifier ident) throws NoSuchSchemaException {
    return hudiCatalogBackendOps.loadSchema(ident);
  }

  /**
   * Alter a schema in the Hudi Catalog.
   *
   * @param ident The name identifier of the schema.
   * @param changes The metadata changes to apply.
   * @return The altered schema.
   * @throws NoSuchSchemaException if the schema does not exist.
   */
  @Override
  public Schema alterSchema(NameIdentifier ident, SchemaChange... changes)
      throws NoSuchSchemaException {
    return hudiCatalogBackendOps.alterSchema(ident, changes);
  }

  /**
   * Drop a schema in the Hudi Catalog.
   *
   * @param ident The name identifier of the schema.
   * @param cascade If true, recursively drop all objects within the schema.
   * @return True if the schema is dropped successfully, false if the schema does not exist.
   * @throws NonEmptySchemaException if the schema is not empty and cascade is false.
   */
  @Override
  public boolean dropSchema(NameIdentifier ident, boolean cascade) throws NonEmptySchemaException {
    return hudiCatalogBackendOps.dropSchema(ident, cascade);
  }

  /**
   * List tables in the Hudi Catalog.
   *
   * @param namespace A namespace.
   * @return The list of tables in the Hudi Catalog.
   * @throws NoSuchSchemaException if the schema does not exist.
   */
  @Override
  public NameIdentifier[] listTables(Namespace namespace) throws NoSuchSchemaException {
    return hudiCatalogBackendOps.listTables(namespace);
  }

  /**
   * Load a table from the Hudi Catalog.
   *
   * @param ident A table identifier.
   * @return The loaded table.
   * @throws NoSuchTableException if the table does not exist.
   */
  @Override
  public Table loadTable(NameIdentifier ident) throws NoSuchTableException {
    return hudiCatalogBackendOps.loadTable(ident);
  }

  /**
   * Create a table in the Hudi Catalog.
   *
   * @param ident A table identifier.
   * @param columns The columns of the new table.
   * @param comment The table comment.
   * @param properties The table properties.
   * @param partitions The table partitioning.
   * @param distribution The distribution of the table
   * @param sortOrders The sort orders of the table
   * @param indexes The table indexes.
   * @return The created table.
   * @throws NoSuchSchemaException if the schema does not exist.
   * @throws TableAlreadyExistsException if the table already exists.
   */
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
    return hudiCatalogBackendOps.createTable(
        ident, columns, comment, properties, partitions, distribution, sortOrders, indexes);
  }

  /**
   * Alter a table in the Hudi Catalog.
   *
   * @param ident A table identifier.
   * @param changes Table changes to apply to the table.
   * @return The altered table.
   * @throws NoSuchTableException if the table does not exist.
   * @throws IllegalArgumentException if the table changes are invalid.
   */
  @Override
  public Table alterTable(NameIdentifier ident, TableChange... changes)
      throws NoSuchTableException, IllegalArgumentException {
    return hudiCatalogBackendOps.alterTable(ident, changes);
  }

  /**
   * Drop a table in the Hudi Catalog.
   *
   * @param ident A table identifier.
   * @return True if the table is dropped successfully, false if the table does not exist.
   */
  @Override
  public boolean dropTable(NameIdentifier ident) {
    return hudiCatalogBackendOps.dropTable(ident);
  }
}
