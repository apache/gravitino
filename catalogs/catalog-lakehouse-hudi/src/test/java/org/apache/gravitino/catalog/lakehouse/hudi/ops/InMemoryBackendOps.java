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
package org.apache.gravitino.catalog.lakehouse.hudi.ops;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.SchemaChange;
import org.apache.gravitino.catalog.lakehouse.hudi.HudiSchema;
import org.apache.gravitino.catalog.lakehouse.hudi.HudiTable;
import org.apache.gravitino.catalog.lakehouse.hudi.TestHudiSchema;
import org.apache.gravitino.catalog.lakehouse.hudi.TestHudiTable;
import org.apache.gravitino.exceptions.NoSuchCatalogException;
import org.apache.gravitino.exceptions.NoSuchSchemaException;
import org.apache.gravitino.exceptions.NoSuchTableException;
import org.apache.gravitino.exceptions.NonEmptySchemaException;
import org.apache.gravitino.exceptions.SchemaAlreadyExistsException;
import org.apache.gravitino.exceptions.TableAlreadyExistsException;
import org.apache.gravitino.rel.Column;
import org.apache.gravitino.rel.Table;
import org.apache.gravitino.rel.TableChange;
import org.apache.gravitino.rel.expressions.distributions.Distribution;
import org.apache.gravitino.rel.expressions.sorts.SortOrder;
import org.apache.gravitino.rel.expressions.transforms.Transform;
import org.apache.gravitino.rel.indexes.Index;

public class InMemoryBackendOps implements HudiCatalogBackendOps {
  private final ConcurrentMap<NameIdentifier, TestHudiSchema> schemas;
  private final ConcurrentMap<NameIdentifier, TestHudiTable> tables;

  public InMemoryBackendOps() {
    this.schemas = new ConcurrentHashMap<>();
    this.tables = new ConcurrentHashMap<>();
  }

  @Override
  public void initialize(Map<String, String> properties) {
    // Do nothing
  }

  @Override
  public HudiSchema loadSchema(NameIdentifier schemaIdent) throws NoSuchSchemaException {
    TestHudiSchema schema = schemas.get(schemaIdent);
    if (schema == null) {
      throw new NoSuchSchemaException("Schema %s does not exist", schemaIdent);
    }
    return schema;
  }

  @Override
  public NameIdentifier[] listSchemas(Namespace namespace) throws NoSuchCatalogException {
    return schemas.keySet().toArray(new NameIdentifier[0]);
  }

  @Override
  public HudiSchema createSchema(
      NameIdentifier ident, String comment, Map<String, String> properties)
      throws NoSuchCatalogException, SchemaAlreadyExistsException {
    if (schemas.containsKey(ident)) {
      throw new SchemaAlreadyExistsException("Schema %s already exists", ident);
    }

    HudiSchema<TestHudiSchema> schema =
        TestHudiSchema.builder()
            .withName(ident.name())
            .withComment(comment)
            .withProperties(properties)
            .build();
    schemas.put(ident, schema.fromHudiSchema());
    return schema;
  }

  @Override
  public HudiSchema alterSchema(NameIdentifier ident, SchemaChange... changes)
      throws NoSuchSchemaException {
    throw new UnsupportedOperationException("Not implemented yet");
  }

  @Override
  public boolean dropSchema(NameIdentifier ident, boolean cascade) throws NonEmptySchemaException {
    throw new UnsupportedOperationException("Not implemented yet");
  }

  @Override
  public NameIdentifier[] listTables(Namespace namespace) throws NoSuchSchemaException {
    return tables.keySet().toArray(new NameIdentifier[0]);
  }

  @Override
  public Table loadTable(NameIdentifier ident) throws NoSuchTableException {
    TestHudiTable table = tables.get(ident);
    if (table == null) {
      throw new NoSuchTableException("Table %s does not exist", ident);
    }
    return table;
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
    if (tables.containsKey(ident)) {
      throw new TableAlreadyExistsException("Table %s already exists", ident);
    }

    HudiTable<TestHudiTable> table =
        TestHudiTable.builder()
            .withName(ident.name())
            .withComment(comment)
            .withProperties(properties)
            .build();
    tables.put(ident, table.fromHudiTable());
    return table;
  }

  @Override
  public Table alterTable(NameIdentifier ident, TableChange... changes)
      throws NoSuchTableException, IllegalArgumentException {
    throw new UnsupportedOperationException("Not implemented yet");
  }

  @Override
  public boolean dropTable(NameIdentifier ident) {
    throw new UnsupportedOperationException("Not implemented yet");
  }

  @Override
  public void close() throws Exception {
    schemas.clear();
    tables.clear();
  }
}
