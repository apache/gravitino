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

package org.apache.gravitino.cli.output;

import static org.junit.Assert.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import org.apache.gravitino.Audit;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.Metalake;
import org.apache.gravitino.Schema;
import org.apache.gravitino.authorization.Group;
import org.apache.gravitino.authorization.Role;
import org.apache.gravitino.authorization.User;
import org.apache.gravitino.cli.CommandContext;
import org.apache.gravitino.cli.TestCliUtil;
import org.apache.gravitino.cli.outputs.PlainFormat;
import org.apache.gravitino.file.Fileset;
import org.apache.gravitino.messaging.Topic;
import org.apache.gravitino.model.Model;
import org.apache.gravitino.rel.Column;
import org.apache.gravitino.rel.Table;
import org.apache.gravitino.rel.expressions.literals.Literal;
import org.apache.gravitino.rel.types.Type;
import org.apache.gravitino.rel.types.Types;
import org.apache.gravitino.tag.Tag;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.shaded.com.google.common.collect.ImmutableList;
import org.testcontainers.shaded.com.google.common.collect.ImmutableMap;

public class TestPlainFormat {

  private final ByteArrayOutputStream outContent = new ByteArrayOutputStream();
  private final ByteArrayOutputStream errContent = new ByteArrayOutputStream();
  private final PrintStream originalOut = System.out;
  private final PrintStream originalErr = System.err;

  @BeforeEach
  void setUp() {
    System.setOut(new PrintStream(outContent));
    System.setErr(new PrintStream(errContent));
  }

  @AfterEach
  public void restoreStreams() {
    System.setOut(originalOut);
    System.setErr(originalErr);
  }

  @Test
  void testMetalakeDetailsWithPlainFormat() {
    CommandContext mockContext = getMockContext();
    Metalake mockMetalake = getMockMetalake();

    PlainFormat.output(mockMetalake, mockContext);
    String output = new String(outContent.toByteArray(), StandardCharsets.UTF_8).trim();
    Assertions.assertEquals("demo_metalake,This is a demo metalake", output);
  }

  @Test
  void testListMetalakeWithPlainFormat() {
    CommandContext mockContext = getMockContext();
    Metalake mockMetalake1 = getMockMetalake("metalake1", "This is a metalake");
    Metalake mockMetalake2 = getMockMetalake("metalake2", "This is another metalake");

    PlainFormat.output(new Metalake[] {mockMetalake1, mockMetalake2}, mockContext);
    String output = new String(outContent.toByteArray(), StandardCharsets.UTF_8).trim();
    Assertions.assertEquals("metalake1\n" + "metalake2", output);
  }

  @Test
  void testCatalogDetailsWithPlainFormat() {
    CommandContext mockContext = getMockContext();
    Catalog mockCatalog = getMockCatalog();

    PlainFormat.output(mockCatalog, mockContext);
    String output = new String(outContent.toByteArray(), StandardCharsets.UTF_8).trim();
    Assertions.assertEquals("demo_catalog,RELATIONAL,demo_provider,This is a demo catalog", output);
  }

  @Test
  void testListCatalogWithPlainFormat() {
    CommandContext mockContext = getMockContext();
    Catalog mockCatalog1 =
        getMockCatalog("catalog1", Catalog.Type.FILESET, "provider1", "This is a catalog");
    Catalog mockCatalog2 =
        getMockCatalog("catalog2", Catalog.Type.RELATIONAL, "provider2", "This is another catalog");

    PlainFormat.output(new Catalog[] {mockCatalog1, mockCatalog2}, mockContext);
    String output = new String(outContent.toByteArray(), StandardCharsets.UTF_8).trim();
    Assertions.assertEquals("catalog1\n" + "catalog2", output);
  }

  @Test
  void testSchemaDetailsWithPlainFormat() {
    CommandContext mockContext = getMockContext();
    Schema mockSchema = getMockSchema();
    PlainFormat.output(mockSchema, mockContext);
    String output = new String(outContent.toByteArray(), StandardCharsets.UTF_8).trim();
    Assertions.assertEquals("demo_schema,This is a demo schema", output);
  }

  @Test
  void testListSchemaWithPlainFormat() {
    CommandContext mockContext = getMockContext();
    Schema mockSchema1 = getMockSchema("schema1", "This is a schema");
    Schema mockSchema2 = getMockSchema("schema2", "This is another schema");

    PlainFormat.output(new Schema[] {mockSchema1, mockSchema2}, mockContext);
    String output = new String(outContent.toByteArray(), StandardCharsets.UTF_8).trim();
    Assertions.assertEquals("schema1\n" + "schema2", output);
  }

  @Test
  void testTableDetailsWithPlainFormat() {
    CommandContext mockContext = getMockContext();
    Table mockTable = getMockTable();
    PlainFormat.output(mockTable, mockContext);
    String output = new String(outContent.toByteArray(), StandardCharsets.UTF_8).trim();
    Assertions.assertEquals("demo_table,This is a demo table", output);
  }

  @Test
  void testAuditWithTableFormat() {
    CommandContext mockContext = getMockContext();
    Audit mockAudit = mock(Audit.class);
    when(mockAudit.creator()).thenReturn("demo_user");
    when(mockAudit.createTime()).thenReturn(Instant.ofEpochMilli(1611111111111L));
    when(mockAudit.lastModifier()).thenReturn("demo_user");
    when(mockAudit.lastModifiedTime()).thenReturn(Instant.ofEpochMilli(1611111111111L));

    PlainFormat.output(mockAudit, mockContext);
    String output = new String(outContent.toByteArray(), StandardCharsets.UTF_8).trim();
    Assertions.assertEquals(
        "demo_user,2021-01-20T02:51:51.111Z,demo_user,2021-01-20T02:51:51.111Z", output);
  }

  @Test
  void testAuditWithTableFormatWithNullValues() {
    CommandContext mockContext = getMockContext();
    Audit mockAudit = mock(Audit.class);
    when(mockAudit.creator()).thenReturn("demo_user");
    when(mockAudit.createTime()).thenReturn(null);
    when(mockAudit.lastModifier()).thenReturn(null);
    when(mockAudit.lastModifiedTime()).thenReturn(null);

    PlainFormat.output(mockAudit, mockContext);
    String output = new String(outContent.toByteArray(), StandardCharsets.UTF_8).trim();
    Assertions.assertEquals("demo_user,N/A,N/A,N/A", output);
  }

  @Test
  void testListTableWithPlainFormat() {
    CommandContext mockContext = getMockContext();
    Table mockTable1 = getMockTable("table1", "This is a table");
    Table mockTable2 = getMockTable("table2", "This is another table");

    PlainFormat.output(new Table[] {mockTable1, mockTable2}, mockContext);
    String output = new String(outContent.toByteArray(), StandardCharsets.UTF_8).trim();
    Assertions.assertEquals("table1\n" + "table2", output);
  }

  @Test
  void testListColumnWithTableFormat() {
    CommandContext mockContext = getMockContext();
    org.apache.gravitino.rel.Column mockColumn1 =
        TestCliUtil.getMockColumn(
            "column1",
            Types.IntegerType.get(),
            "This is a int " + "column",
            false,
            true,
            new Literal<Integer>() {
              @Override
              public Integer value() {
                return 4;
              }

              @Override
              public Type dataType() {
                return null;
              }
            });
    org.apache.gravitino.rel.Column mockColumn2 =
        TestCliUtil.getMockColumn(
            "column2",
            Types.StringType.get(),
            "This is a string " + "column",
            true,
            false,
            new Literal<String>() {
              @Override
              public String value() {
                return "default value";
              }

              @Override
              public Type dataType() {
                return null;
              }
            });

    PlainFormat.output(
        new org.apache.gravitino.rel.Column[] {mockColumn1, mockColumn2}, mockContext);
    String output = new String(outContent.toByteArray(), StandardCharsets.UTF_8).trim();
    Assertions.assertEquals(
        "name,datatype,default_value,comment,nullable,auto_increment\n"
            + "column1,integer,4,This is a int column,false,true\n"
            + "column2,string,default value,This is a string column,true,",
        output);
  }

  @Test
  void testListColumnWithTableFormatAndEmptyDefaultValues() {
    CommandContext mockContext = getMockContext();
    org.apache.gravitino.rel.Column mockColumn1 =
        TestCliUtil.getMockColumn(
            "column1", Types.IntegerType.get(), "", false, true, Column.DEFAULT_VALUE_NOT_SET);
    org.apache.gravitino.rel.Column mockColumn2 =
        TestCliUtil.getMockColumn(
            "column2",
            Types.StringType.get(),
            "",
            true,
            false,
            Column.DEFAULT_VALUE_OF_CURRENT_TIMESTAMP);

    PlainFormat.output(
        new org.apache.gravitino.rel.Column[] {mockColumn1, mockColumn2}, mockContext);
    String output = new String(outContent.toByteArray(), StandardCharsets.UTF_8).trim();
    Assertions.assertEquals(
        "name,datatype,default_value,comment,nullable,auto_increment\n"
            + "column1,integer,,,false,true\n"
            + "column2,string,current_timestamp(),,true,",
        output);
  }

  @Test
  void testTagDetailsWithPlainFormat() {
    CommandContext mockContext = getMockContext();
    Tag mockTag =
        TestCliUtil.getMockTag("tag1", "comment for tag1", ImmutableMap.of("k1", "v1", "k2", "v2"));

    PlainFormat.output(mockTag, mockContext);
    String output = new String(outContent.toByteArray(), StandardCharsets.UTF_8).trim();
    Assertions.assertEquals("tag1,comment for tag1", output);
  }

  @Test
  void testTagDetailsWithPlainFormatWithNullValues() {
    CommandContext mockContext = getMockContext();
    Tag mockTag = TestCliUtil.getMockTag("tag1", null);

    PlainFormat.output(mockTag, mockContext);
    String output = new String(outContent.toByteArray(), StandardCharsets.UTF_8).trim();
    Assertions.assertEquals("tag1,N/A", output);
  }

  @Test
  void testListAllTagsWithPlainFormat() {
    CommandContext mockContext = getMockContext();

    Tag mockTag1 = TestCliUtil.getMockTag("tag1", "comment for tag1");
    Tag mockTag2 = TestCliUtil.getMockTag("tag2", "comment for tag2");
    Tag mockTag3 = TestCliUtil.getMockTag("tag3", "comment for tag3");

    PlainFormat.output(new Tag[] {mockTag1, mockTag2, mockTag3}, mockContext);
    String output = new String(outContent.toByteArray(), StandardCharsets.UTF_8).trim();
    Assertions.assertEquals("tag1\n" + "tag2\n" + "tag3", output);
  }

  @Test
  void testListTagPropertiesWithPlainFormat() {
    CommandContext mockContext = getMockContext();

    Tag mockTag1 =
        TestCliUtil.getMockTag("tag1", "comment for tag1", ImmutableMap.of("k1", "v1", "k2", "v2"));

    PlainFormat.output(mockTag1.properties(), mockContext);
    String output = new String(outContent.toByteArray(), StandardCharsets.UTF_8).trim();
    Assertions.assertEquals("k1,v1\n" + "k2,v2", output);
  }

  @Test
  void testOutputWithUnsupportType() {
    CommandContext mockContext = getMockContext();
    Object mockObject = new Object();

    assertThrows(
        IllegalArgumentException.class,
        () -> {
          PlainFormat.output(mockObject, mockContext);
        });
  }

  private CommandContext getMockContext() {
    CommandContext mockContext = mock(CommandContext.class);

    return mockContext;
  }

  private Metalake getMockMetalake() {
    return getMockMetalake("demo_metalake", "This is a demo metalake");
  }

  private Metalake getMockMetalake(String name, String comment) {
    Metalake mockMetalake = mock(Metalake.class);
    when(mockMetalake.name()).thenReturn(name);
    when(mockMetalake.comment()).thenReturn(comment);

    return mockMetalake;
  }

  private Catalog getMockCatalog() {
    return getMockCatalog(
        "demo_catalog", Catalog.Type.RELATIONAL, "demo_provider", "This is a demo catalog");
  }

  private Catalog getMockCatalog(String name, Catalog.Type type, String provider, String comment) {
    Catalog mockCatalog = mock(Catalog.class);
    when(mockCatalog.name()).thenReturn(name);
    when(mockCatalog.type()).thenReturn(type);
    when(mockCatalog.provider()).thenReturn(provider);
    when(mockCatalog.comment()).thenReturn(comment);

    return mockCatalog;
  }

  private Schema getMockSchema() {
    return getMockSchema("demo_schema", "This is a demo schema");
  }

  private Schema getMockSchema(String name, String comment) {
    Schema mockSchema = mock(Schema.class);
    when(mockSchema.name()).thenReturn(name);
    when(mockSchema.comment()).thenReturn(comment);

    return mockSchema;
  }

  private Table getMockTable() {
    return getMockTable("demo_table", "This is a demo table");
  }

  private Table getMockTable(String name, String comment) {
    Table mockTable = mock(Table.class);
    org.apache.gravitino.rel.Column mockColumnInt =
        TestCliUtil.getMockColumn(
            "id",
            Types.IntegerType.get(),
            "This is a int column",
            false,
            true,
            Column.DEFAULT_VALUE_NOT_SET);
    org.apache.gravitino.rel.Column mockColumnString =
        TestCliUtil.getMockColumn(
            "name",
            Types.StringType.get(),
            "This is a string column",
            true,
            true,
            Column.DEFAULT_VALUE_NOT_SET);

    when(mockTable.name()).thenReturn(name);
    when(mockTable.comment()).thenReturn(comment);
    when(mockTable.columns())
        .thenReturn(new org.apache.gravitino.rel.Column[] {mockColumnInt, mockColumnString});

    return mockTable;
  }

  @Test
  void testModelDetailsWithPlainFormat() {
    CommandContext mockContext = getMockContext();
    Model mockModel = TestCliUtil.getMockModel("demo_model", "This is a demo model", 1);

    PlainFormat.output(mockModel, mockContext);
    String output = new String(outContent.toByteArray(), StandardCharsets.UTF_8).trim();
    Assertions.assertEquals("Model name demo_model, latest version: 1", output);
  }

  @Test
  void testListModelWithPlainFormat() {
    CommandContext mockContext = getMockContext();
    Model model1 = TestCliUtil.getMockModel("model1", "This is a model", 1);
    Model model2 = TestCliUtil.getMockModel("model2", "This is another model", 2);
    Model model3 = TestCliUtil.getMockModel("model3", "This is a third model", 3);

    PlainFormat.output(new Model[] {model1, model2, model3}, mockContext);
    String output = new String(outContent.toByteArray(), StandardCharsets.UTF_8).trim();
    Assertions.assertEquals("model1,model2,model3", output);
  }

  @Test
  void testUserDetailsWithPlainFormat() {
    CommandContext mockContext = getMockContext();
    User mockUser = TestCliUtil.getMockUser("demo_user", ImmutableList.of("admin", "user"));
    PlainFormat.output(mockUser, mockContext);
    String output = new String(outContent.toByteArray(), StandardCharsets.UTF_8).trim();
    Assertions.assertEquals("admin,user", output);
  }

  @Test
  void testListUsersWithPlainFormat() {
    CommandContext mockContext = getMockContext();
    User user1 = TestCliUtil.getMockUser("user1", ImmutableList.of("admin", "user"));
    User user2 = TestCliUtil.getMockUser("user2", ImmutableList.of("admin"));
    User user3 = TestCliUtil.getMockUser("user3", ImmutableList.of("user"));

    PlainFormat.output(new User[] {user1, user2, user3}, mockContext);
    String output = new String(outContent.toByteArray(), StandardCharsets.UTF_8).trim();
    Assertions.assertEquals("user1,user2,user3", output);
  }

  @Test
  void testGroupDetailsWithPlainFormat() {
    CommandContext mockContext = getMockContext();
    Group mockGroup =
        TestCliUtil.getMockGroup("demo_group", ImmutableList.of("admin", "scientist"));
    PlainFormat.output(mockGroup, mockContext);
    String output = new String(outContent.toByteArray(), StandardCharsets.UTF_8).trim();
    Assertions.assertEquals("admin,scientist", output);
  }

  @Test
  void testListGroupsWithPlainFormat() {
    CommandContext mockContext = getMockContext();
    Group group1 = TestCliUtil.getMockGroup("group1", ImmutableList.of("admin", "user"));
    Group group2 = TestCliUtil.getMockGroup("group2", ImmutableList.of("admin"));
    Group group3 = TestCliUtil.getMockGroup("group3", ImmutableList.of("user"));

    PlainFormat.output(new Group[] {group1, group2, group3}, mockContext);
    String output = new String(outContent.toByteArray(), StandardCharsets.UTF_8).trim();
    Assertions.assertEquals("group1,group2,group3", output);
  }

  @Test
  void testRoleDetailsWithPlainFormat() {
    CommandContext mockContext = TestCliUtil.getMockContext();
    Role admin = TestCliUtil.getMockRole("admin");

    PlainFormat.output(admin, mockContext);
    String output = new String(outContent.toByteArray(), StandardCharsets.UTF_8).trim();
    Assertions.assertEquals(
        "demo_table,TABLE,\n"
            + "ALLOW create table\n"
            + "ALLOW select table\n"
            + "demo_fileset,FILESET,\n"
            + "ALLOW create fileset\n"
            + "ALLOW read fileset",
        output);
  }

  @Test
  void testListRolesWithPlainFormat() {
    CommandContext mockContext = TestCliUtil.getMockContext();
    Role admin = TestCliUtil.getMockRole("admin");
    Role user = TestCliUtil.getMockRole("user");

    PlainFormat.output(new Role[] {admin, user}, mockContext);
    String output = new String(outContent.toByteArray(), StandardCharsets.UTF_8).trim();
    Assertions.assertEquals("admin,user", output);
  }

  @Test
  void testTopicDetailsWithPlainFormat() {
    CommandContext mockContext = TestCliUtil.getMockContext();
    Topic mockTopic = TestCliUtil.getMockTopic("demo_topic", "This is a demo topic");
    PlainFormat.output(mockTopic, mockContext);
    String output = new String(outContent.toByteArray(), StandardCharsets.UTF_8).trim();
    Assertions.assertEquals("demo_topic,This is a demo topic", output);
  }

  @Test
  void testListTopicsWithPlainFormat() {
    CommandContext mockContext = TestCliUtil.getMockContext();
    Topic topic1 = TestCliUtil.getMockTopic("topic1", "This is a topic");
    Topic topic2 = TestCliUtil.getMockTopic("topic2", "This is another topic");
    Topic topic3 = TestCliUtil.getMockTopic("topic3", "This is a third topic");

    PlainFormat.output(new Topic[] {topic1, topic2, topic3}, mockContext);
    String output = new String(outContent.toByteArray(), StandardCharsets.UTF_8).trim();
    Assertions.assertEquals("topic1,topic2,topic3", output);
  }

  @Test
  void testFilesetDetailsWithPlainFormat() {
    CommandContext mockContext = TestCliUtil.getMockContext();
    Fileset mockFileset =
        TestCliUtil.getMockFileset(
            "demo_fileset",
            Fileset.Type.MANAGED,
            "This is a demo fileset",
            "hdfs" + "://demo_location");
    PlainFormat.output(mockFileset, mockContext);
    String output = new String(outContent.toByteArray(), StandardCharsets.UTF_8).trim();
    Assertions.assertEquals(
        "demo_fileset,managed,This is a demo fileset,hdfs://demo_location", output);
  }

  @Test
  void testListFilesetsWithPlainFormat() {
    Fileset fileset1 =
        TestCliUtil.getMockFileset(
            "fileset1",
            Fileset.Type.MANAGED,
            "This is a demo fileset",
            "hdfs" + "://demo_location");
    Fileset fileset2 =
        TestCliUtil.getMockFileset(
            "fileset2",
            Fileset.Type.EXTERNAL,
            "This is another demo fileset",
            "s3" + "://demo_location");
    Fileset fileset3 =
        TestCliUtil.getMockFileset(
            "fileset3",
            Fileset.Type.MANAGED,
            "This is a third demo fileset",
            "hdfs" + "://demo_location");

    CommandContext mockContext = TestCliUtil.getMockContext();
    PlainFormat.output(new Fileset[] {fileset1, fileset2, fileset3}, mockContext);
    String output = new String(outContent.toByteArray(), StandardCharsets.UTF_8).trim();
    Assertions.assertEquals("fileset1,fileset2,fileset3", output);
  }
}
