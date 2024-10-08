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
package org.apache.gravitino.client;

import static org.apache.hc.core5.http.HttpStatus.SC_INTERNAL_SERVER_ERROR;
import static org.apache.hc.core5.http.HttpStatus.SC_NOT_FOUND;
import static org.apache.hc.core5.http.HttpStatus.SC_OK;

import com.fasterxml.jackson.core.JsonProcessingException;
import java.util.Collections;
import java.util.Locale;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.MetadataObjects;
import org.apache.gravitino.Metalake;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.Schema;
import org.apache.gravitino.authorization.SupportsRoles;
import org.apache.gravitino.dto.AuditDTO;
import org.apache.gravitino.dto.SchemaDTO;
import org.apache.gravitino.dto.file.FilesetDTO;
import org.apache.gravitino.dto.messaging.TopicDTO;
import org.apache.gravitino.dto.rel.ColumnDTO;
import org.apache.gravitino.dto.rel.TableDTO;
import org.apache.gravitino.dto.responses.ErrorResponse;
import org.apache.gravitino.dto.responses.NameListResponse;
import org.apache.gravitino.exceptions.NotFoundException;
import org.apache.gravitino.file.Fileset;
import org.apache.gravitino.messaging.Topic;
import org.apache.gravitino.rel.Table;
import org.apache.gravitino.rel.types.Types;
import org.apache.hc.core5.http.Method;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class TestSupportRoles extends TestBase {
  private static final String METALAKE_NAME = "metalake";

  private static Catalog relationalCatalog;

  private static Catalog filesetCatalog;

  private static Catalog messagingCatalog;

  private static Schema genericSchema;

  private static Table relationalTable;

  private static Fileset genericFileset;

  private static Topic genericTopic;
  private static Metalake metalake;

  @BeforeAll
  public static void setUp() throws Exception {
    TestBase.setUp();
    metalake = TestGravitinoMetalake.createMetalake(client, METALAKE_NAME);

    relationalCatalog =
        new RelationalCatalog(
            Namespace.of(METALAKE_NAME),
            "catalog1",
            Catalog.Type.RELATIONAL,
            "test",
            "comment",
            Collections.emptyMap(),
            AuditDTO.builder().build(),
            client.restClient());

    filesetCatalog =
        new FilesetCatalog(
            Namespace.of(METALAKE_NAME),
            "catalog2",
            Catalog.Type.FILESET,
            "test",
            "comment",
            Collections.emptyMap(),
            AuditDTO.builder().build(),
            client.restClient());

    messagingCatalog =
        new MessagingCatalog(
            Namespace.of(METALAKE_NAME),
            "catalog3",
            Catalog.Type.MESSAGING,
            "test",
            "comment",
            Collections.emptyMap(),
            AuditDTO.builder().build(),
            client.restClient());

    genericSchema =
        new GenericSchema(
            SchemaDTO.builder()
                .withName("schema1")
                .withComment("comment1")
                .withProperties(Collections.emptyMap())
                .withAudit(AuditDTO.builder().withCreator("test").build())
                .build(),
            client.restClient(),
            METALAKE_NAME,
            "catalog1");

    relationalTable =
        RelationalTable.from(
            Namespace.of(METALAKE_NAME, "catalog1", "schema1"),
            TableDTO.builder()
                .withName("table1")
                .withComment("comment1")
                .withColumns(
                    new ColumnDTO[] {
                      ColumnDTO.builder()
                          .withName("col1")
                          .withDataType(Types.IntegerType.get())
                          .build()
                    })
                .withProperties(Collections.emptyMap())
                .withAudit(AuditDTO.builder().withCreator("test").build())
                .build(),
            client.restClient());

    genericFileset =
        new GenericFileset(
            FilesetDTO.builder()
                .name("fileset1")
                .comment("comment1")
                .type(Fileset.Type.EXTERNAL)
                .storageLocation("s3://bucket/path")
                .properties(Collections.emptyMap())
                .audit(AuditDTO.builder().withCreator("test").build())
                .build(),
            client.restClient(),
            Namespace.of(METALAKE_NAME, "catalog1", "schema1"));

    genericTopic =
        new GenericTopic(
            TopicDTO.builder()
                .withName("topic1")
                .withComment("comment1")
                .withProperties(Collections.emptyMap())
                .withAudit(AuditDTO.builder().withCreator("test").build())
                .build(),
            client.restClient(),
            Namespace.of(METALAKE_NAME, "catalog1", "schema1"));
  }

  @Test
  public void testListRolesForMetalake() throws JsonProcessingException {
    testListRoles(
        metalake.supportsRoles(),
        MetadataObjects.of(null, metalake.name(), MetadataObject.Type.METALAKE));
  }

  @Test
  public void testListRolesForCatalog() throws JsonProcessingException {
    testListRoles(
        relationalCatalog.supportsRoles(),
        MetadataObjects.of(null, relationalCatalog.name(), MetadataObject.Type.CATALOG));

    testListRoles(
        filesetCatalog.supportsRoles(),
        MetadataObjects.of(null, filesetCatalog.name(), MetadataObject.Type.CATALOG));

    testListRoles(
        messagingCatalog.supportsRoles(),
        MetadataObjects.of(null, messagingCatalog.name(), MetadataObject.Type.CATALOG));
  }

  @Test
  public void testListRolesForSchema() throws JsonProcessingException {
    testListRoles(
        genericSchema.supportsRoles(),
        MetadataObjects.of("catalog1", genericSchema.name(), MetadataObject.Type.SCHEMA));
  }

  @Test
  public void testListRolesForTable() throws JsonProcessingException {
    testListRoles(
        relationalTable.supportsRoles(),
        MetadataObjects.of("catalog1.schema1", relationalTable.name(), MetadataObject.Type.TABLE));
  }

  @Test
  public void testListRolesForFileset() throws JsonProcessingException {
    testListRoles(
        genericFileset.supportsRoles(),
        MetadataObjects.of("catalog1.schema1", genericFileset.name(), MetadataObject.Type.FILESET));
  }

  @Test
  public void testListRolesForTopic() throws JsonProcessingException {
    testListRoles(
        genericTopic.supportsRoles(),
        MetadataObjects.of("catalog1.schema1", genericTopic.name(), MetadataObject.Type.TOPIC));
  }

  private void testListRoles(SupportsRoles supportsRoles, MetadataObject metadataObject)
      throws JsonProcessingException {
    String path =
        "/api/metalakes/"
            + METALAKE_NAME
            + "/objects/"
            + metadataObject.type().name().toLowerCase(Locale.ROOT)
            + "/"
            + metadataObject.fullName()
            + "/roles";

    String[] roles = new String[] {"role1", "role2"};
    NameListResponse resp = new NameListResponse(roles);
    buildMockResource(Method.GET, path, null, resp, SC_OK);

    String[] actualTags = supportsRoles.listBindingRoleNames();
    Assertions.assertArrayEquals(roles, actualTags);

    // Return empty list
    NameListResponse resp1 = new NameListResponse(new String[0]);
    buildMockResource(Method.GET, path, null, resp1, SC_OK);

    String[] actualRoles1 = supportsRoles.listBindingRoleNames();
    Assertions.assertArrayEquals(new String[0], actualRoles1);

    // Test throw NotFoundException
    ErrorResponse errorResp =
        ErrorResponse.notFound(NotFoundException.class.getSimpleName(), "mock error");
    buildMockResource(Method.GET, path, null, errorResp, SC_NOT_FOUND);

    Throwable ex =
        Assertions.assertThrows(NotFoundException.class, supportsRoles::listBindingRoleNames);
    Assertions.assertTrue(ex.getMessage().contains("mock error"));

    // Test throw internal error
    ErrorResponse errorResp1 = ErrorResponse.internalError("mock error");
    buildMockResource(Method.GET, path, null, errorResp1, SC_INTERNAL_SERVER_ERROR);

    Throwable ex1 =
        Assertions.assertThrows(RuntimeException.class, supportsRoles::listBindingRoleNames);
    Assertions.assertTrue(ex1.getMessage().contains("mock error"));
  }
}
