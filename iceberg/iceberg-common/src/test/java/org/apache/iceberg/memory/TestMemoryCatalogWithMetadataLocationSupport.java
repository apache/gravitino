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

package org.apache.iceberg.memory;

import java.util.Collections;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.Schema;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestMemoryCatalogWithMetadataLocationSupport {
  private static final Schema SCHEMA =
      new Schema(Types.NestedField.required(1, "id", Types.IntegerType.get()));

  @Test
  void testMetadataLocation() {
    MemoryCatalogWithMetadataLocationSupport catalog =
        new MemoryCatalogWithMetadataLocationSupport();
    catalog.initialize("memory", Collections.emptyMap());
    catalog.createNamespace(Namespace.of("ns"));
    TableIdentifier tableIdentifier = TableIdentifier.of("ns", "t");
    BaseTable table = (BaseTable) catalog.createTable(tableIdentifier, SCHEMA);
    String tableLocation = table.operations().current().metadataFileLocation();
    Assertions.assertEquals(tableLocation, catalog.metadataLocation(tableIdentifier));

    // update table
    table.updateSchema().renameColumn("id", "new_id").commit();
    Assertions.assertNotEquals(tableLocation, catalog.metadataLocation(tableIdentifier));
    tableLocation = table.operations().current().metadataFileLocation();
    Assertions.assertEquals(tableLocation, catalog.metadataLocation(tableIdentifier));

    // delete table
    catalog.dropTable(tableIdentifier);
    Assertions.assertNull(catalog.metadataLocation(tableIdentifier));
  }
}
