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
package org.apache.gravitino.storage.relational;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.gravitino.Entity;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.meta.SchemaEntity;
import org.apache.gravitino.storage.RandomIdGenerator;
import org.apache.gravitino.utils.NamespaceUtil;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.TestTemplate;

/**
 * Tests schema insert paths that use relational {@code batchInsertSchemaMeta} (single or batch).
 */
public class TestJDBCBackendBatchInsert extends TestJDBCBackend {

  @TestTemplate
  public void testBatchInsertSingleSchemaViaBackend() throws IOException {
    String metalakeName = "metalake_batch_insert_single";
    String catalogName = "catalog_batch_insert_single";
    createAndInsertMakeLake(metalakeName);
    createAndInsertCatalog(metalakeName, catalogName);

    SchemaEntity schema =
        createSchemaEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            NamespaceUtil.ofSchema(metalakeName, catalogName),
            "flat_schema_batch",
            AUDIT_INFO);
    backend.insert(schema, false);

    SchemaEntity loaded =
        (SchemaEntity)
            backend.get(
                NameIdentifier.of(metalakeName, catalogName, "flat_schema_batch"),
                Entity.EntityType.SCHEMA);
    Assertions.assertEquals(schema.id(), loaded.id());
    Assertions.assertEquals("flat_schema_batch", loaded.name());
    Assertions.assertEquals(schema.namespace(), loaded.namespace());

    List<SchemaEntity> listed =
        backend.list(
            NamespaceUtil.ofSchema(metalakeName, catalogName), Entity.EntityType.SCHEMA, true);
    Assertions.assertEquals(1, listed.size());
    Assertions.assertEquals("flat_schema_batch", listed.get(0).name());
  }

  @TestTemplate
  public void testBatchInsertHierarchicalSchemaCreatesAncestorsAndLeafViaBackend()
      throws IOException {
    String metalakeName = "metalake_batch_insert_nested";
    String catalogName = "catalog_batch_insert_nested";
    createAndInsertMakeLake(metalakeName);
    createAndInsertCatalog(metalakeName, catalogName);

    String logicalLeaf = "ns_a:ns_b:leaf";
    SchemaEntity hierarchical =
        SchemaEntity.builder()
            .withId(RandomIdGenerator.INSTANCE.nextId())
            .withName(logicalLeaf)
            .withNamespace(NamespaceUtil.ofSchema(metalakeName, catalogName))
            .withComment("nested")
            .withProperties(Collections.emptyMap())
            .withAuditInfo(AUDIT_INFO)
            .build();
    backend.insert(hierarchical, false);

    List<SchemaEntity> schemas =
        backend.list(
            NamespaceUtil.ofSchema(metalakeName, catalogName), Entity.EntityType.SCHEMA, true);
    Set<String> logicalNames = schemas.stream().map(SchemaEntity::name).collect(Collectors.toSet());

    Assertions.assertTrue(logicalNames.contains("ns_a"));
    Assertions.assertTrue(logicalNames.contains("ns_a:ns_b"));
    Assertions.assertTrue(logicalNames.contains(logicalLeaf));

    SchemaEntity loaded =
        (SchemaEntity)
            backend.get(
                NameIdentifier.of(metalakeName, catalogName, logicalLeaf),
                Entity.EntityType.SCHEMA);
    Assertions.assertEquals(logicalLeaf, loaded.name());
    Assertions.assertEquals("nested", loaded.comment());
  }

  @TestTemplate
  public void testBatchInsertHierarchicalSecondLeafReusesAncestorsViaBackend() throws IOException {
    String metalakeName = "metalake_batch_insert_reuse";
    String catalogName = "catalog_batch_insert_reuse";
    createAndInsertMakeLake(metalakeName);
    createAndInsertCatalog(metalakeName, catalogName);

    String leaf1 = "ns_a:ns_b:leaf1";
    String leaf2 = "ns_a:ns_b:leaf2";
    String ancestorA = "ns_a";
    String ancestorAB = "ns_a:ns_b";

    SchemaEntity first =
        SchemaEntity.builder()
            .withId(RandomIdGenerator.INSTANCE.nextId())
            .withName(leaf1)
            .withNamespace(NamespaceUtil.ofSchema(metalakeName, catalogName))
            .withComment("first")
            .withProperties(Collections.emptyMap())
            .withAuditInfo(AUDIT_INFO)
            .build();
    backend.insert(first, false);

    long idA =
        ((SchemaEntity)
                backend.get(
                    NameIdentifier.of(metalakeName, catalogName, ancestorA),
                    Entity.EntityType.SCHEMA))
            .id();
    long idAB =
        ((SchemaEntity)
                backend.get(
                    NameIdentifier.of(metalakeName, catalogName, ancestorAB),
                    Entity.EntityType.SCHEMA))
            .id();

    SchemaEntity second =
        SchemaEntity.builder()
            .withId(RandomIdGenerator.INSTANCE.nextId())
            .withName(leaf2)
            .withNamespace(NamespaceUtil.ofSchema(metalakeName, catalogName))
            .withComment("second")
            .withProperties(Collections.emptyMap())
            .withAuditInfo(AUDIT_INFO)
            .build();
    backend.insert(second, false);

    Assertions.assertEquals(
        idA,
        ((SchemaEntity)
                backend.get(
                    NameIdentifier.of(metalakeName, catalogName, ancestorA),
                    Entity.EntityType.SCHEMA))
            .id());
    Assertions.assertEquals(
        idAB,
        ((SchemaEntity)
                backend.get(
                    NameIdentifier.of(metalakeName, catalogName, ancestorAB),
                    Entity.EntityType.SCHEMA))
            .id());
  }
}
