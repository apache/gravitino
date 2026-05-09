/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file to
 * you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.gravitino.storage.relational;

import org.apache.gravitino.Entity;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.catalog.HierarchicalSchemaUtil;
import org.apache.gravitino.meta.GenericEntity;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestRelationalSchemaNamingBridge {

  @Test
  public void convertMetadataObjectDottedFullNameLeavesWrongSegmentCountUnchanged() {
    Assertions.assertEquals(
        "only.two",
        RelationalSchemaNamingBridge.convertMetadataObjectDottedFullName(
            "only.two", MetadataObject.Type.TABLE, false));
  }

  @Test
  public void convertMetadataObjectDottedFullNameTablePhysicalSchemaToLogical() {
    String sep = HierarchicalSchemaUtil.schemaSeparator();
    String logicalSchema = "a" + sep + "b";
    String physicalSchema = HierarchicalSchemaUtil.logicalToPhysical(logicalSchema, sep);
    String storageFullName = "catalog." + physicalSchema + ".t1";
    String apiFullName =
        RelationalSchemaNamingBridge.convertMetadataObjectDottedFullName(
            storageFullName, MetadataObject.Type.TABLE, false);
    Assertions.assertEquals("catalog." + logicalSchema + ".t1", apiFullName);
  }

  @Test
  public void genericEntityMetadataFullNameForApiConvertsDottedTablePathWithoutNamespace() {
    String sep = HierarchicalSchemaUtil.schemaSeparator();
    String logicalSchema = "ns" + sep + "sub";
    String physicalSchema = HierarchicalSchemaUtil.logicalToPhysical(logicalSchema, sep);
    GenericEntity stub =
        GenericEntity.builder()
            .withId(1L)
            .withEntityType(Entity.EntityType.TABLE)
            .withName("cat." + physicalSchema + ".tbl")
            .build();
    GenericEntity api = RelationalSchemaNamingBridge.genericEntityMetadataFullNameForApi(stub);
    Assertions.assertEquals("cat." + logicalSchema + ".tbl", api.name());
    Assertions.assertEquals(stub.id(), api.id());
    Assertions.assertEquals(stub.type(), api.type());
  }
}
