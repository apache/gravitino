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
import org.apache.gravitino.Namespace;
import org.apache.gravitino.Schema;
import org.apache.gravitino.dto.AuditDTO;
import org.apache.gravitino.dto.SchemaDTO;
import org.apache.gravitino.dto.file.FilesetDTO;
import org.apache.gravitino.dto.messaging.TopicDTO;
import org.apache.gravitino.dto.rel.ColumnDTO;
import org.apache.gravitino.dto.rel.TableDTO;
import org.apache.gravitino.dto.requests.TagsAssociateRequest;
import org.apache.gravitino.dto.responses.ErrorResponse;
import org.apache.gravitino.dto.responses.NameListResponse;
import org.apache.gravitino.dto.responses.TagListResponse;
import org.apache.gravitino.dto.responses.TagResponse;
import org.apache.gravitino.dto.tag.TagDTO;
import org.apache.gravitino.exceptions.NoSuchTagException;
import org.apache.gravitino.exceptions.NotFoundException;
import org.apache.gravitino.file.Fileset;
import org.apache.gravitino.messaging.Topic;
import org.apache.gravitino.rel.Column;
import org.apache.gravitino.rel.Table;
import org.apache.gravitino.rel.types.Types;
import org.apache.gravitino.tag.SupportsTags;
import org.apache.gravitino.tag.Tag;
import org.apache.hc.core5.http.Method;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class TestSupportTags extends TestBase {

  private static final String METALAKE_NAME = "metalake";

  private static Catalog relationalCatalog;

  private static Catalog filesetCatalog;

  private static Catalog messagingCatalog;

  private static Schema genericSchema;

  private static Table relationalTable;

  private static Column genericColumn;

  private static Fileset genericFileset;

  private static Topic genericTopic;

  @BeforeAll
  public static void setUp() throws Exception {
    TestBase.setUp();
    TestGravitinoMetalake.createMetalake(client, METALAKE_NAME);

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

    genericColumn = relationalTable.columns()[0];

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
  public void testListTagsForCatalog() throws JsonProcessingException {
    testListTags(
        relationalCatalog.supportsTags(),
        MetadataObjects.of(null, relationalCatalog.name(), MetadataObject.Type.CATALOG));

    testListTags(
        filesetCatalog.supportsTags(),
        MetadataObjects.of(null, filesetCatalog.name(), MetadataObject.Type.CATALOG));

    testListTags(
        messagingCatalog.supportsTags(),
        MetadataObjects.of(null, messagingCatalog.name(), MetadataObject.Type.CATALOG));
  }

  @Test
  public void testListTagsForSchema() throws JsonProcessingException {
    testListTags(
        genericSchema.supportsTags(),
        MetadataObjects.of("catalog1", genericSchema.name(), MetadataObject.Type.SCHEMA));
  }

  @Test
  public void testListTagsForTable() throws JsonProcessingException {
    testListTags(
        relationalTable.supportsTags(),
        MetadataObjects.of("catalog1.schema1", relationalTable.name(), MetadataObject.Type.TABLE));
  }

  @Test
  public void testListTagsForColumn() throws JsonProcessingException {
    testListTags(
        genericColumn.supportsTags(),
        MetadataObjects.of(
            "catalog1.schema1.table1", genericColumn.name(), MetadataObject.Type.COLUMN));
  }

  @Test
  public void testListTagsForFileset() throws JsonProcessingException {
    testListTags(
        genericFileset.supportsTags(),
        MetadataObjects.of("catalog1.schema1", genericFileset.name(), MetadataObject.Type.FILESET));
  }

  @Test
  public void testListTagsForTopic() throws JsonProcessingException {
    testListTags(
        genericTopic.supportsTags(),
        MetadataObjects.of("catalog1.schema1", genericTopic.name(), MetadataObject.Type.TOPIC));
  }

  @Test
  public void testListTagsInfoForCatalog() throws JsonProcessingException {
    testListTagsInfo(
        relationalCatalog.supportsTags(),
        MetadataObjects.of(null, relationalCatalog.name(), MetadataObject.Type.CATALOG));

    testListTagsInfo(
        filesetCatalog.supportsTags(),
        MetadataObjects.of(null, filesetCatalog.name(), MetadataObject.Type.CATALOG));

    testListTagsInfo(
        messagingCatalog.supportsTags(),
        MetadataObjects.of(null, messagingCatalog.name(), MetadataObject.Type.CATALOG));
  }

  @Test
  public void testListTagsInfoForSchema() throws JsonProcessingException {
    testListTagsInfo(
        genericSchema.supportsTags(),
        MetadataObjects.of("catalog1", genericSchema.name(), MetadataObject.Type.SCHEMA));
  }

  @Test
  public void testListTagsInfoForTable() throws JsonProcessingException {
    testListTagsInfo(
        relationalTable.supportsTags(),
        MetadataObjects.of("catalog1.schema1", relationalTable.name(), MetadataObject.Type.TABLE));
  }

  @Test
  public void testListTagsInfoForColumn() throws JsonProcessingException {
    testListTagsInfo(
        genericColumn.supportsTags(),
        MetadataObjects.of(
            "catalog1.schema1.table1", genericColumn.name(), MetadataObject.Type.COLUMN));
  }

  @Test
  public void testListTagsInfoForFileset() throws JsonProcessingException {
    testListTagsInfo(
        genericFileset.supportsTags(),
        MetadataObjects.of("catalog1.schema1", genericFileset.name(), MetadataObject.Type.FILESET));
  }

  @Test
  public void testListTagsInfoForTopic() throws JsonProcessingException {
    testListTagsInfo(
        genericTopic.supportsTags(),
        MetadataObjects.of("catalog1.schema1", genericTopic.name(), MetadataObject.Type.TOPIC));
  }

  @Test
  public void testGetTagForCatalog() throws JsonProcessingException {
    testGetTag(
        relationalCatalog.supportsTags(),
        MetadataObjects.of(null, relationalCatalog.name(), MetadataObject.Type.CATALOG));

    testGetTag(
        filesetCatalog.supportsTags(),
        MetadataObjects.of(null, filesetCatalog.name(), MetadataObject.Type.CATALOG));

    testGetTag(
        messagingCatalog.supportsTags(),
        MetadataObjects.of(null, messagingCatalog.name(), MetadataObject.Type.CATALOG));
  }

  @Test
  public void testGetTagForSchema() throws JsonProcessingException {
    testGetTag(
        genericSchema.supportsTags(),
        MetadataObjects.of("catalog1", genericSchema.name(), MetadataObject.Type.SCHEMA));
  }

  @Test
  public void testGetTagForTable() throws JsonProcessingException {
    testGetTag(
        relationalTable.supportsTags(),
        MetadataObjects.of("catalog1.schema1", relationalTable.name(), MetadataObject.Type.TABLE));
  }

  @Test
  public void testGetTagForColumn() throws JsonProcessingException {
    testGetTag(
        genericColumn.supportsTags(),
        MetadataObjects.of(
            "catalog1.schema1.table1", genericColumn.name(), MetadataObject.Type.COLUMN));
  }

  @Test
  public void testGetTagForFileset() throws JsonProcessingException {
    testGetTag(
        genericFileset.supportsTags(),
        MetadataObjects.of("catalog1.schema1", genericFileset.name(), MetadataObject.Type.FILESET));
  }

  @Test
  public void testGetTagForTopic() throws JsonProcessingException {
    testGetTag(
        genericTopic.supportsTags(),
        MetadataObjects.of("catalog1.schema1", genericTopic.name(), MetadataObject.Type.TOPIC));
  }

  @Test
  public void testAssociateTagsForCatalog() throws JsonProcessingException {
    testAssociateTags(
        relationalCatalog.supportsTags(),
        MetadataObjects.of(null, relationalCatalog.name(), MetadataObject.Type.CATALOG));

    testAssociateTags(
        filesetCatalog.supportsTags(),
        MetadataObjects.of(null, filesetCatalog.name(), MetadataObject.Type.CATALOG));

    testAssociateTags(
        messagingCatalog.supportsTags(),
        MetadataObjects.of(null, messagingCatalog.name(), MetadataObject.Type.CATALOG));
  }

  @Test
  public void testAssociateTagsForSchema() throws JsonProcessingException {
    testAssociateTags(
        genericSchema.supportsTags(),
        MetadataObjects.of("catalog1", genericSchema.name(), MetadataObject.Type.SCHEMA));
  }

  @Test
  public void testAssociateTagsForTable() throws JsonProcessingException {
    testAssociateTags(
        relationalTable.supportsTags(),
        MetadataObjects.of("catalog1.schema1", relationalTable.name(), MetadataObject.Type.TABLE));
  }

  @Test
  public void testAssociateTagsForColumn() throws JsonProcessingException {
    testAssociateTags(
        genericColumn.supportsTags(),
        MetadataObjects.of(
            "catalog1.schema1.table1", genericColumn.name(), MetadataObject.Type.COLUMN));
  }

  @Test
  public void testAssociateTagsForFileset() throws JsonProcessingException {
    testAssociateTags(
        genericFileset.supportsTags(),
        MetadataObjects.of("catalog1.schema1", genericFileset.name(), MetadataObject.Type.FILESET));
  }

  @Test
  public void testAssociateTagsForTopic() throws JsonProcessingException {
    testAssociateTags(
        genericTopic.supportsTags(),
        MetadataObjects.of("catalog1.schema1", genericTopic.name(), MetadataObject.Type.TOPIC));
  }

  private void testListTags(SupportsTags supportsTags, MetadataObject metadataObject)
      throws JsonProcessingException {
    String path =
        "/api/metalakes/"
            + METALAKE_NAME
            + "/objects/"
            + metadataObject.type().name().toLowerCase(Locale.ROOT)
            + "/"
            + metadataObject.fullName()
            + "/tags";

    String[] tags = new String[] {"tag1", "tag2"};
    NameListResponse resp = new NameListResponse(tags);
    buildMockResource(Method.GET, path, null, resp, SC_OK);

    String[] actualTags = supportsTags.listTags();
    Assertions.assertArrayEquals(tags, actualTags);

    // Return empty list
    NameListResponse resp1 = new NameListResponse(new String[0]);
    buildMockResource(Method.GET, path, null, resp1, SC_OK);

    String[] actualTags1 = supportsTags.listTags();
    Assertions.assertArrayEquals(new String[0], actualTags1);

    // Test throw NotFoundException
    ErrorResponse errorResp =
        ErrorResponse.notFound(NotFoundException.class.getSimpleName(), "mock error");
    buildMockResource(Method.GET, path, null, errorResp, SC_NOT_FOUND);

    Throwable ex = Assertions.assertThrows(NotFoundException.class, supportsTags::listTags);
    Assertions.assertTrue(ex.getMessage().contains("mock error"));

    // Test throw internal error
    ErrorResponse errorResp1 = ErrorResponse.internalError("mock error");
    buildMockResource(Method.GET, path, null, errorResp1, SC_INTERNAL_SERVER_ERROR);

    Throwable ex1 = Assertions.assertThrows(RuntimeException.class, supportsTags::listTags);
    Assertions.assertTrue(ex1.getMessage().contains("mock error"));
  }

  private void testListTagsInfo(SupportsTags supportsTags, MetadataObject metadataObject)
      throws JsonProcessingException {
    String path =
        "/api/metalakes/"
            + METALAKE_NAME
            + "/objects/"
            + metadataObject.type().name().toLowerCase(Locale.ROOT)
            + "/"
            + metadataObject.fullName()
            + "/tags";

    TagDTO tag1 =
        TagDTO.builder()
            .withName("tag1")
            .withAudit(AuditDTO.builder().withCreator("test").build())
            .build();
    TagDTO tag2 =
        TagDTO.builder()
            .withName("tag2")
            .withAudit(AuditDTO.builder().withCreator("test").build())
            .build();
    TagDTO[] tags = new TagDTO[] {tag1, tag2};
    TagListResponse resp = new TagListResponse(tags);
    buildMockResource(Method.GET, path, null, resp, SC_OK);

    Tag[] actualTags = supportsTags.listTagsInfo();
    Assertions.assertEquals(2, actualTags.length);
    Assertions.assertEquals("tag1", actualTags[0].name());
    Assertions.assertEquals("tag2", actualTags[1].name());

    // Return empty list
    TagListResponse resp1 = new TagListResponse(new TagDTO[0]);
    buildMockResource(Method.GET, path, null, resp1, SC_OK);

    Tag[] actualTags1 = supportsTags.listTagsInfo();
    Assertions.assertArrayEquals(new Tag[0], actualTags1);

    // Test throw NotFoundException
    ErrorResponse errorResp =
        ErrorResponse.notFound(NotFoundException.class.getSimpleName(), "mock error");
    buildMockResource(Method.GET, path, null, errorResp, SC_NOT_FOUND);

    Throwable ex = Assertions.assertThrows(NotFoundException.class, supportsTags::listTags);
    Assertions.assertTrue(ex.getMessage().contains("mock error"));

    // Test throw internal error
    ErrorResponse errorResp1 = ErrorResponse.internalError("mock error");
    buildMockResource(Method.GET, path, null, errorResp1, SC_INTERNAL_SERVER_ERROR);

    Throwable ex1 = Assertions.assertThrows(RuntimeException.class, supportsTags::listTags);
    Assertions.assertTrue(ex1.getMessage().contains("mock error"));
  }

  private void testGetTag(SupportsTags supportsTags, MetadataObject metadataObject)
      throws JsonProcessingException {
    String path =
        "/api/metalakes/"
            + METALAKE_NAME
            + "/objects/"
            + metadataObject.type().name().toLowerCase(Locale.ROOT)
            + "/"
            + metadataObject.fullName()
            + "/tags/tag1";

    TagDTO tag1 =
        TagDTO.builder()
            .withName("tag1")
            .withAudit(AuditDTO.builder().withCreator("test").build())
            .build();

    TagResponse resp = new TagResponse(tag1);
    buildMockResource(Method.GET, path, null, resp, SC_OK);

    Tag actualTag = supportsTags.getTag("tag1");
    Assertions.assertEquals("tag1", actualTag.name());

    // Test throw NoSuchTagException
    ErrorResponse errorResp =
        ErrorResponse.notFound(NoSuchTagException.class.getSimpleName(), "mock error");
    buildMockResource(Method.GET, path, null, errorResp, SC_NOT_FOUND);

    Throwable ex =
        Assertions.assertThrows(NoSuchTagException.class, () -> supportsTags.getTag("tag1"));
    Assertions.assertTrue(ex.getMessage().contains("mock error"));

    // Test throw internal error
    ErrorResponse errorResp1 = ErrorResponse.internalError("mock error");
    buildMockResource(Method.GET, path, null, errorResp1, SC_INTERNAL_SERVER_ERROR);

    Throwable ex1 =
        Assertions.assertThrows(RuntimeException.class, () -> supportsTags.getTag("tag1"));
    Assertions.assertTrue(ex1.getMessage().contains("mock error"));
  }

  private void testAssociateTags(SupportsTags supportsTags, MetadataObject metadataObject)
      throws JsonProcessingException {
    String path =
        "/api/metalakes/"
            + METALAKE_NAME
            + "/objects/"
            + metadataObject.type().name().toLowerCase(Locale.ROOT)
            + "/"
            + metadataObject.fullName()
            + "/tags";

    String[] tagsToAdd = new String[] {"tag1", "tag2"};
    String[] tagsToRemove = new String[] {"tag3", "tag4"};
    TagsAssociateRequest request = new TagsAssociateRequest(tagsToAdd, tagsToRemove);

    NameListResponse resp = new NameListResponse(tagsToAdd);
    buildMockResource(Method.POST, path, request, resp, SC_OK);

    String[] actualTags = supportsTags.associateTags(tagsToAdd, tagsToRemove);
    Assertions.assertArrayEquals(tagsToAdd, actualTags);

    // Test throw internal error
    ErrorResponse errorResp1 = ErrorResponse.internalError("mock error");
    buildMockResource(Method.POST, path, request, errorResp1, SC_INTERNAL_SERVER_ERROR);

    Throwable ex1 =
        Assertions.assertThrows(
            RuntimeException.class, () -> supportsTags.associateTags(tagsToAdd, tagsToRemove));
    Assertions.assertTrue(ex1.getMessage().contains("mock error"));
  }
}
