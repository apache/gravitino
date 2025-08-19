/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */

package org.apache.gravitino.iceberg.service;

import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.catalog.lakehouse.iceberg.IcebergConstants;
import org.apache.iceberg.Schema;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.rest.requests.CreateTableRequest;
import org.apache.iceberg.types.Types.IntegerType;
import org.apache.iceberg.types.Types.NestedField;
import org.apache.iceberg.types.Types.StringType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

@SuppressWarnings("deprecation")
public class TestIcebergRESTUtils {

  @Test
  void testGetGravitinoNameIdentifier() {
    String metalakeName = "metalake";
    String catalogName = "catalog";
    TableIdentifier tableIdentifier = TableIdentifier.of("ns1", "ns2", "table");
    NameIdentifier nameIdentifier =
        IcebergRestUtils.getGravitinoNameIdentifier(metalakeName, catalogName, tableIdentifier);
    Assertions.assertEquals(
        NameIdentifier.of(metalakeName, catalogName, "ns1", "ns2", "table"), nameIdentifier);
  }

  @Test
  void testGetCatalogName() {
    String prefix = "catalog/";
    Assertions.assertEquals("catalog", IcebergRestUtils.getCatalogName(prefix));
    Assertions.assertEquals(
        IcebergConstants.ICEBERG_REST_DEFAULT_CATALOG, IcebergRestUtils.getCatalogName(""));
  }

  @Test
  void testSerdeIcebergRESTObject() {
    Schema tableSchema =
        new Schema(
            NestedField.of(1, false, "foo1", StringType.get()),
            NestedField.of(2, true, "foo2", IntegerType.get()));
    CreateTableRequest createTableRequest =
        CreateTableRequest.builder().withName("table").withSchema(tableSchema).build();
    CreateTableRequest clonedIcebergRESTObject =
        IcebergRestUtils.cloneIcebergRESTObject(createTableRequest, CreateTableRequest.class);
    Assertions.assertEquals(createTableRequest.name(), clonedIcebergRESTObject.name());
    Assertions.assertEquals(
        createTableRequest.schema().columns().size(),
        clonedIcebergRESTObject.schema().columns().size());
    for (int i = 0; i < createTableRequest.schema().columns().size(); i++) {
      NestedField field = createTableRequest.schema().columns().get(i);
      NestedField clonedField = clonedIcebergRESTObject.schema().columns().get(i);
      Assertions.assertEquals(field, clonedField);
    }
  }
}
