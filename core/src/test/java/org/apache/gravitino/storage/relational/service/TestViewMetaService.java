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
package org.apache.gravitino.storage.relational.service;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.common.collect.ImmutableMap;
import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.gravitino.EntityAlreadyExistsException;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.exceptions.NoSuchEntityException;
import org.apache.gravitino.integration.test.util.GravitinoITUtils;
import org.apache.gravitino.meta.AuditInfo;
import org.apache.gravitino.meta.ViewEntity;
import org.apache.gravitino.rel.Column;
import org.apache.gravitino.rel.Representation;
import org.apache.gravitino.rel.SQLRepresentation;
import org.apache.gravitino.rel.types.Types;
import org.apache.gravitino.storage.RandomIdGenerator;
import org.apache.gravitino.storage.relational.TestJDBCBackend;
import org.apache.gravitino.storage.relational.session.SqlSessionFactoryHelper;
import org.apache.gravitino.utils.NamespaceUtil;
import org.apache.ibatis.session.SqlSession;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;

public class TestViewMetaService extends TestJDBCBackend {

  private final String metalakeName = GravitinoITUtils.genRandomName("tst_metalake");
  private final String catalogName = GravitinoITUtils.genRandomName("tst_view_catalog");
  private final String schemaName = GravitinoITUtils.genRandomName("tst_view_schema");

  @BeforeEach
  public void prepare() throws IOException {
    createAndInsertMakeLake(metalakeName);
    createAndInsertCatalog(metalakeName, catalogName);
    createAndInsertSchema(metalakeName, catalogName, schemaName);
  }

  @TestTemplate
  public void testInsertAlreadyExistsException() throws IOException {
    Namespace ns = NamespaceUtil.ofView(metalakeName, catalogName, schemaName);
    ViewEntity view =
        createViewEntity(RandomIdGenerator.INSTANCE.nextId(), ns, "test_view", AUDIT_INFO);
    ViewEntity viewCopy =
        createViewEntity(RandomIdGenerator.INSTANCE.nextId(), ns, "test_view", AUDIT_INFO);

    ViewMetaService.getInstance().insertView(view, false);
    assertThrows(
        EntityAlreadyExistsException.class,
        () -> ViewMetaService.getInstance().insertView(viewCopy, false));
  }

  @TestTemplate
  public void testInsertAndGetView() throws IOException {
    String viewName = GravitinoITUtils.genRandomName("test_view");
    Namespace ns = NamespaceUtil.ofView(metalakeName, catalogName, schemaName);
    ViewEntity view =
        createViewEntity(RandomIdGenerator.INSTANCE.nextId(), ns, viewName, AUDIT_INFO);

    ViewMetaService.getInstance().insertView(view, false);

    NameIdentifier viewIdent = NameIdentifier.of(metalakeName, catalogName, schemaName, viewName);
    ViewEntity loaded = ViewMetaService.getInstance().getViewByIdentifier(viewIdent);

    assertNotNull(loaded);
    assertEquals(view.id(), loaded.id());
    assertEquals(view.name(), loaded.name());
    assertEquals(view.comment(), loaded.comment());
    assertEquals(view.defaultCatalog(), loaded.defaultCatalog());
    assertEquals(view.defaultSchema(), loaded.defaultSchema());
    assertEquals(view.columns().length, loaded.columns().length);
    assertEquals(view.representations().length, loaded.representations().length);
    assertEquals(view.auditInfo().creator(), loaded.auditInfo().creator());
  }

  @TestTemplate
  public void testListViews() throws IOException {
    Namespace ns = NamespaceUtil.ofView(metalakeName, catalogName, schemaName);

    String viewName1 = GravitinoITUtils.genRandomName("test_view1");
    ViewEntity view1 =
        createViewEntity(RandomIdGenerator.INSTANCE.nextId(), ns, viewName1, AUDIT_INFO);

    String viewName2 = GravitinoITUtils.genRandomName("test_view2");
    ViewEntity view2 =
        createViewEntity(RandomIdGenerator.INSTANCE.nextId(), ns, viewName2, AUDIT_INFO);

    ViewMetaService.getInstance().insertView(view1, false);
    ViewMetaService.getInstance().insertView(view2, false);

    List<ViewEntity> views = ViewMetaService.getInstance().listViewsByNamespace(ns);

    assertEquals(2, views.size());
    assertTrue(views.stream().anyMatch(v -> v.name().equals(viewName1)));
    assertTrue(views.stream().anyMatch(v -> v.name().equals(viewName2)));
  }

  @TestTemplate
  public void testUpdateView() throws IOException {
    String viewName = GravitinoITUtils.genRandomName("test_view");
    Namespace ns = NamespaceUtil.ofView(metalakeName, catalogName, schemaName);
    ViewEntity view =
        createViewEntity(RandomIdGenerator.INSTANCE.nextId(), ns, viewName, AUDIT_INFO);

    ViewMetaService.getInstance().insertView(view, false);

    NameIdentifier viewIdent = NameIdentifier.of(metalakeName, catalogName, schemaName, viewName);
    ViewEntity updated =
        ViewEntity.builder()
            .withId(view.id())
            .withName(view.name())
            .withNamespace(ns)
            .withComment("updated comment")
            .withColumns(view.columns())
            .withRepresentations(view.representations())
            .withDefaultCatalog("updated_catalog")
            .withDefaultSchema("updated_schema")
            .withProperties(view.properties())
            .withAuditInfo(AUDIT_INFO)
            .build();

    ViewEntity result = ViewMetaService.getInstance().updateView(viewIdent, e -> updated);
    assertEquals("updated comment", result.comment());
    assertEquals("updated_catalog", result.defaultCatalog());
    assertEquals("updated_schema", result.defaultSchema());

    Map<Integer, Long> versions = listViewVersions(view.id());
    assertEquals(2, versions.size());
    assertTrue(versions.containsKey(1));
    assertTrue(versions.containsKey(2));
  }

  @TestTemplate
  public void testDeleteView() throws IOException {
    String viewName = GravitinoITUtils.genRandomName("test_view");
    Namespace ns = NamespaceUtil.ofView(metalakeName, catalogName, schemaName);
    ViewEntity view =
        createViewEntity(RandomIdGenerator.INSTANCE.nextId(), ns, viewName, AUDIT_INFO);

    ViewMetaService.getInstance().insertView(view, false);

    NameIdentifier viewIdent = NameIdentifier.of(metalakeName, catalogName, schemaName, viewName);
    assertTrue(ViewMetaService.getInstance().deleteView(viewIdent));

    assertThrows(
        NoSuchEntityException.class,
        () -> ViewMetaService.getInstance().getViewByIdentifier(viewIdent));
  }

  @TestTemplate
  public void testGetNonExistentView() {
    NameIdentifier viewIdent =
        NameIdentifier.of(metalakeName, catalogName, schemaName, "non_existent_view");
    assertThrows(
        NoSuchEntityException.class,
        () -> ViewMetaService.getInstance().getViewByIdentifier(viewIdent));
  }

  @TestTemplate
  public void testInsertViewWithOverwrite() throws IOException {
    String viewName = GravitinoITUtils.genRandomName("test_view");
    Namespace ns = NamespaceUtil.ofView(metalakeName, catalogName, schemaName);
    ViewEntity view =
        createViewEntity(RandomIdGenerator.INSTANCE.nextId(), ns, viewName, AUDIT_INFO);

    ViewMetaService.getInstance().insertView(view, false);

    ViewEntity newView =
        ViewEntity.builder()
            .withId(view.id())
            .withName(view.name())
            .withNamespace(ns)
            .withComment("overwritten comment")
            .withColumns(view.columns())
            .withRepresentations(view.representations())
            .withDefaultCatalog(view.defaultCatalog())
            .withDefaultSchema(view.defaultSchema())
            .withProperties(view.properties())
            .withAuditInfo(AUDIT_INFO)
            .build();

    ViewMetaService.getInstance().insertView(newView, true);

    NameIdentifier viewIdent = NameIdentifier.of(metalakeName, catalogName, schemaName, viewName);
    ViewEntity loaded = ViewMetaService.getInstance().getViewByIdentifier(viewIdent);
    assertEquals("overwritten comment", loaded.comment());
  }

  @TestTemplate
  public void testViewLifeCycle() throws IOException {
    String viewName = GravitinoITUtils.genRandomName("test_view");
    Namespace ns = NamespaceUtil.ofView(metalakeName, catalogName, schemaName);
    ViewEntity view =
        createViewEntity(RandomIdGenerator.INSTANCE.nextId(), ns, viewName, AUDIT_INFO);
    ViewMetaService.getInstance().insertView(view, false);

    NameIdentifier viewIdent = NameIdentifier.of(metalakeName, catalogName, schemaName, viewName);
    ViewEntity v2 =
        ViewEntity.builder()
            .withId(view.id())
            .withName(view.name())
            .withNamespace(ns)
            .withComment("v2")
            .withColumns(view.columns())
            .withRepresentations(view.representations())
            .withDefaultCatalog(view.defaultCatalog())
            .withDefaultSchema(view.defaultSchema())
            .withProperties(view.properties())
            .withAuditInfo(AUDIT_INFO)
            .build();
    ViewMetaService.getInstance().updateView(viewIdent, e -> v2);

    assertTrue(ViewMetaService.getInstance().deleteView(viewIdent));

    int deleted =
        ViewMetaService.getInstance()
            .deleteViewMetasByLegacyTimeline(Instant.now().toEpochMilli() + 1000, 100);
    assertTrue(deleted >= 2);
    assertEquals(0, listViewVersions(view.id()).size());
  }

  private ViewEntity createViewEntity(
      Long id, Namespace namespace, String name, AuditInfo auditInfo) {
    Column[] columns =
        new Column[] {
          Column.of("c1", Types.IntegerType.get(), "first column"),
          Column.of("c2", Types.StringType.get(), "second column")
        };
    Representation[] reps =
        new Representation[] {
          SQLRepresentation.builder().withDialect("spark").withSql("SELECT c1, c2 FROM t").build(),
          SQLRepresentation.builder().withDialect("trino").withSql("SELECT c1, c2 FROM t").build()
        };
    return ViewEntity.builder()
        .withId(id)
        .withName(name)
        .withNamespace(namespace)
        .withComment("test view comment")
        .withColumns(columns)
        .withRepresentations(reps)
        .withDefaultCatalog(null)
        .withDefaultSchema(null)
        .withProperties(ImmutableMap.of("k1", "v1"))
        .withAuditInfo(auditInfo)
        .build();
  }

  private Map<Integer, Long> listViewVersions(Long viewId) {
    Map<Integer, Long> versionDeletedTime = new HashMap<>();
    try (SqlSession sqlSession =
            SqlSessionFactoryHelper.getInstance().getSqlSessionFactory().openSession(true);
        Connection connection = sqlSession.getConnection();
        Statement statement = connection.createStatement();
        ResultSet rs =
            statement.executeQuery(
                String.format(
                    "SELECT version, deleted_at FROM view_version_info WHERE view_id = %d",
                    viewId))) {
      while (rs.next()) {
        versionDeletedTime.put(rs.getInt("version"), rs.getLong("deleted_at"));
      }
    } catch (SQLException e) {
      throw new RuntimeException("SQL execution failed", e);
    }
    return versionDeletedTime;
  }
}
