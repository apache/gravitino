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
package org.apache.gravitino.catalog.hudi.backend.hms;

import java.util.Map;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.SchemaChange;
import org.apache.gravitino.catalog.hudi.ops.HudiCatalogOps;
import org.apache.gravitino.exceptions.NoSuchCatalogException;
import org.apache.gravitino.exceptions.NoSuchSchemaException;
import org.apache.gravitino.exceptions.NoSuchTableException;
import org.apache.gravitino.exceptions.NonEmptySchemaException;
import org.apache.gravitino.exceptions.SchemaAlreadyExistsException;
import org.apache.gravitino.exceptions.TableAlreadyExistsException;
import org.apache.gravitino.rel.Column;
import org.apache.gravitino.rel.TableChange;
import org.apache.gravitino.rel.expressions.distributions.Distribution;
import org.apache.gravitino.rel.expressions.sorts.SortOrder;
import org.apache.gravitino.rel.expressions.transforms.Transform;
import org.apache.gravitino.rel.indexes.Index;

public class HudiHMSBackendOps implements HudiCatalogOps {

  @Override
  public void initialize(Map<String, String> properties) {
    // todo: initialize the catalogOps
  }

  @Override
  public HudiHMSSchema loadSchema(NameIdentifier schemaIdent) throws NoSuchSchemaException {
    throw new UnsupportedOperationException("Not implemented yet");
  }

  @Override
  public NameIdentifier[] listSchemas(Namespace namespace) throws NoSuchCatalogException {
    throw new UnsupportedOperationException("Not implemented yet");
  }

  @Override
  public HudiHMSSchema createSchema(
      NameIdentifier ident, String comment, Map<String, String> properties)
      throws NoSuchCatalogException, SchemaAlreadyExistsException {
    throw new UnsupportedOperationException("Not implemented yet");
  }

  @Override
  public HudiHMSSchema alterSchema(NameIdentifier ident, SchemaChange... changes)
      throws NoSuchSchemaException {
    throw new UnsupportedOperationException("Not implemented yet");
  }

  @Override
  public boolean dropSchema(NameIdentifier ident, boolean cascade) throws NonEmptySchemaException {
    throw new UnsupportedOperationException("Not implemented yet");
  }

  @Override
  public NameIdentifier[] listTables(Namespace namespace) throws NoSuchSchemaException {
    throw new UnsupportedOperationException("Not implemented yet");
  }

  @Override
  public HudiHMSTable loadTable(NameIdentifier ident) throws NoSuchTableException {
    throw new UnsupportedOperationException("Not implemented yet");
  }

  @Override
  public HudiHMSTable createTable(
      NameIdentifier ident,
      Column[] columns,
      String comment,
      Map<String, String> properties,
      Transform[] partitions,
      Distribution distribution,
      SortOrder[] sortOrders,
      Index[] indexes)
      throws NoSuchSchemaException, TableAlreadyExistsException {
    throw new UnsupportedOperationException("Not implemented yet");
  }

  @Override
  public HudiHMSTable alterTable(NameIdentifier ident, TableChange... changes)
      throws NoSuchTableException, IllegalArgumentException {
    throw new UnsupportedOperationException("Not implemented yet");
  }

  @Override
  public boolean dropTable(NameIdentifier ident) {
    throw new UnsupportedOperationException("Not implemented yet");
  }

  @Override
  public void close() {
    // todo: close the HMS connection
  }
}
