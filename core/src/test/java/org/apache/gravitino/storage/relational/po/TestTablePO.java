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
package org.apache.gravitino.storage.relational.po;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestTablePO {

  @Test
  void testCopyBuilderCarriesEveryField() throws Exception {
    TablePO source = fullyPopulated();

    TablePO copy = TablePO.builder(source).build();

    // The copy builder lists the fields by hand, so a field added to TablePO without a matching
    // line there would be silently blanked on every path that copies a row. Compare reflectively
    // instead of naming the fields again here: this fails when the two drift apart.
    List<String> dropped = new ArrayList<>();
    for (Field field : TablePO.class.getDeclaredFields()) {
      if (Modifier.isStatic(field.getModifiers())) {
        continue;
      }
      field.setAccessible(true);
      if (!java.util.Objects.equals(field.get(source), field.get(copy))) {
        dropped.add(field.getName());
      }
    }
    Assertions.assertTrue(
        dropped.isEmpty(), () -> "TablePO.builder(TablePO) does not copy these fields: " + dropped);
  }

  /** Every field set to a distinct value, so a missed copy cannot pass by coincidence. */
  private TablePO fullyPopulated() throws Exception {
    TablePO.Builder builder =
        TablePO.builder()
            .withTableId(1L)
            .withTableName("table")
            .withMetalakeId(2L)
            .withCatalogId(3L)
            .withSchemaId(4L)
            .withAuditInfo("audit")
            .withCurrentVersion(5L)
            .withLastVersion(6L)
            .withDeletedAt(7L)
            .withFormat("format")
            .withProperties("properties")
            .withPartitions("partitions")
            .withSortOrders("sortOrders")
            .withDistribution("distribution")
            .withIndexes("indexes")
            .withComment("comment");
    TablePO po = builder.build();

    // Guard the guard: if a new field is added and left unset above, the comparison would trivially
    // hold with null on both sides and prove nothing.
    for (Field field : TablePO.class.getDeclaredFields()) {
      if (Modifier.isStatic(field.getModifiers())) {
        continue;
      }
      field.setAccessible(true);
      Assertions.assertNotNull(
          field.get(po),
          "Set TablePO." + field.getName() + " in this test so the copy check covers it");
    }
    return po;
  }
}
