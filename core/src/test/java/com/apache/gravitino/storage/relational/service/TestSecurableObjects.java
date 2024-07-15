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
package com.apache.gravitino.storage.relational.service;

import com.apache.gravitino.Namespace;
import com.apache.gravitino.authorization.AuthorizationUtils;
import com.apache.gravitino.authorization.Privileges;
import com.apache.gravitino.authorization.SecurableObject;
import com.apache.gravitino.authorization.SecurableObjects;
import com.apache.gravitino.meta.AuditInfo;
import com.apache.gravitino.meta.BaseMetalake;
import com.apache.gravitino.meta.CatalogEntity;
import com.apache.gravitino.meta.FilesetEntity;
import com.apache.gravitino.meta.RoleEntity;
import com.apache.gravitino.meta.SchemaEntity;
import com.apache.gravitino.meta.TableEntity;
import com.apache.gravitino.meta.TopicEntity;
import com.apache.gravitino.storage.RandomIdGenerator;
import com.apache.gravitino.storage.relational.TestJDBCBackend;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import java.io.IOException;
import java.time.Instant;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestSecurableObjects extends TestJDBCBackend {
  RoleMetaService roleMetaService = RoleMetaService.getInstance();

  @Test
  public void testAllTypeSecurableObjects() throws IOException {
    String metalakeName = "metalake";
    AuditInfo auditInfo =
        AuditInfo.builder().withCreator("creator").withCreateTime(Instant.now()).build();
    BaseMetalake metalake =
        createBaseMakeLake(RandomIdGenerator.INSTANCE.nextId(), metalakeName, auditInfo);
    backend.insert(metalake, false);

    CatalogEntity catalog =
        createCatalog(
            RandomIdGenerator.INSTANCE.nextId(), Namespace.of("metalake"), "catalog", auditInfo);
    backend.insert(catalog, false);

    SchemaEntity schema =
        createSchemaEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            Namespace.of("metalake", "catalog"),
            "schema",
            auditInfo);
    backend.insert(schema, false);

    FilesetEntity fileset =
        createFilesetEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            Namespace.of("metalake", "catalog", "schema"),
            "fileset",
            auditInfo);
    backend.insert(fileset, false);
    TableEntity table =
        createTableEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            Namespace.of("metalake", "catalog", "schema"),
            "table",
            auditInfo);
    backend.insert(table, false);
    TopicEntity topic =
        createTopicEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            Namespace.of("metalake", "catalog", "schema"),
            "topic",
            auditInfo);
    backend.insert(topic, false);

    SecurableObject catalogObject =
        SecurableObjects.ofCatalog(
            "catalog",
            Lists.newArrayList(Privileges.UseCatalog.allow(), Privileges.DropCatalog.deny()));

    SecurableObject schemaObject =
        SecurableObjects.ofSchema(
            catalogObject, "schema", Lists.newArrayList(Privileges.UseSchema.allow()));
    SecurableObject tableObject =
        SecurableObjects.ofTable(
            schemaObject, "table", Lists.newArrayList(Privileges.ReadTable.allow()));
    SecurableObject filesetObject =
        SecurableObjects.ofFileset(
            schemaObject, "fileset", Lists.newArrayList(Privileges.ReadFileset.allow()));
    SecurableObject topicObject =
        SecurableObjects.ofTopic(
            schemaObject, "topic", Lists.newArrayList(Privileges.ReadTopic.deny()));
    SecurableObject allMetalakesObject =
        SecurableObjects.ofAllMetalakes(Lists.newArrayList(Privileges.UseMetalake.allow()));

    RoleEntity role1 =
        createRoleEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            AuthorizationUtils.ofRoleNamespace(metalakeName),
            "role1",
            auditInfo,
            Lists.newArrayList(
                catalogObject,
                schemaObject,
                tableObject,
                filesetObject,
                topicObject,
                allMetalakesObject),
            ImmutableMap.of("k1", "v1"));

    Assertions.assertDoesNotThrow(() -> roleMetaService.insertRole(role1, false));
    Assertions.assertEquals(role1, roleMetaService.getRoleByIdentifier(role1.nameIdentifier()));
  }
}
