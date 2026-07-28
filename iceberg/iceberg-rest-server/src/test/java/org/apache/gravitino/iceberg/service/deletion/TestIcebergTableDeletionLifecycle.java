/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.gravitino.iceberg.service.deletion;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.gravitino.Configs;
import org.apache.gravitino.Entity;
import org.apache.gravitino.GravitinoEnv;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.iceberg.common.IcebergConfig;
import org.apache.gravitino.iceberg.service.authorization.IcebergRESTServerContext;
import org.apache.gravitino.iceberg.service.provider.IcebergConfigProvider;
import org.apache.gravitino.listener.api.event.IcebergRequestContext;
import org.apache.gravitino.meta.TableEntity;
import org.apache.gravitino.storage.RandomIdGenerator;
import org.apache.gravitino.storage.relational.TestJDBCBackend;
import org.apache.gravitino.storage.relational.mapper.EntityChangeLogMapper;
import org.apache.gravitino.storage.relational.po.EntityDeletionPO;
import org.apache.gravitino.storage.relational.po.TablePO;
import org.apache.gravitino.storage.relational.po.cache.EntityChangeRecord;
import org.apache.gravitino.storage.relational.po.cache.OperateType;
import org.apache.gravitino.storage.relational.service.EntityDeletionService;
import org.apache.gravitino.storage.relational.service.TableDeletionService;
import org.apache.gravitino.storage.relational.session.SqlSessionFactoryHelper;
import org.apache.gravitino.storage.relational.utils.SessionUtils;
import org.apache.gravitino.utils.NamespaceUtil;
import org.apache.ibatis.session.SqlSession;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;

/** Cross-backend transaction tests for the Iceberg-specific DELETE coordinator. */
public class TestIcebergTableDeletionLifecycle extends TestJDBCBackend {

  private static final String METALAKE = "iceberg_deletion_metalake";
  private static final String CATALOG = "iceberg_deletion_catalog";
  private static final String SCHEMA = "sales";
  private static final String TABLE = "orders";

  private IcebergRequestContext requestContext;
  private TableIdentifier icebergIdentifier;
  private NameIdentifier gravitinoIdentifier;

  @BeforeEach
  public void setUpLifecycle() throws IOException {
    when(GravitinoEnv.getInstance().config().get(Configs.ENABLE_AUTHORIZATION)).thenReturn(false);
    createParentEntities(METALAKE, CATALOG, SCHEMA, AUDIT_INFO);
    Namespace namespace = NamespaceUtil.ofTable(METALAKE, CATALOG, SCHEMA);
    gravitinoIdentifier = NameIdentifier.of(namespace, TABLE);
    TableEntity table =
        createTableEntity(RandomIdGenerator.INSTANCE.nextId(), namespace, TABLE, AUDIT_INFO);
    backend.insert(table, false);

    requestContext = mock(IcebergRequestContext.class);
    icebergIdentifier = TableIdentifier.of(org.apache.iceberg.catalog.Namespace.of(SCHEMA), TABLE);
    when(requestContext.catalogName()).thenReturn(CATALOG);

    IcebergConfigProvider provider = mock(IcebergConfigProvider.class);
    when(provider.getMetalakeName()).thenReturn(METALAKE);
    when(provider.getDefaultCatalogName()).thenReturn(CATALOG);
    IcebergRESTServerContext.create(provider, false, true, true, null);
  }

  @TestTemplate
  public void testDeleteStoresOnlyTheActionAndTombstonesTheExistingRoot()
      throws IOException, SQLException {
    IcebergTableDeletionLifecycle lifecycle = lifecycle(true, 86_400_000L);
    long originalTableId = tableId();
    long beforeChange = maxChangeId();

    lifecycle.delete(requestContext, icebergIdentifier, true);

    assertFalse(backend.exists(gravitinoIdentifier, Entity.EntityType.TABLE));
    IcebergRetainedTableDeletion retained = onlyRetained();
    EntityDeletionPO deletion = retained.getDeletion();
    TablePO table = retained.getTable();
    assertEquals(originalTableId, table.getTableId());
    assertEquals(TABLE, table.getTableName());
    assertEquals(deletion.getDeletionId(), table.getDeletionId());
    assertEquals(IcebergTableDeletionLifecycle.DELETED, deletion.getState());
    assertEquals(table.getDeletedAt() + 86_400_000L, deletion.getRetentionExpiresAt());
    assertNull(deletion.getPurgeJobId());
    assertTrue(lifecycle.isNameReserved(CATALOG, icebergIdentifier));
    assertEquals(
        Set.of(TABLE), lifecycle.reservedTableNames(CATALOG, icebergIdentifier.namespace()));
    assertEquals(
        deletion.getDeletionId(),
        lifecycle.findActive(CATALOG, icebergIdentifier).getDeletion().getDeletionId());
    assertTrue(
        lifecycle
            .reservedTableNames(CATALOG, org.apache.iceberg.catalog.Namespace.of("missing-parent"))
            .isEmpty());
    assertChange(beforeChange, OperateType.DROP);
  }

  @TestTemplate
  public void testRepeatedDeleteReusesTheActiveDeletion() throws SQLException {
    IcebergTableDeletionLifecycle lifecycle = lifecycle(true, 86_400_000L);

    lifecycle.delete(requestContext, icebergIdentifier, false);
    String deletionId = onlyRetained().getDeletion().getDeletionId();
    lifecycle.delete(requestContext, icebergIdentifier, true);

    assertEquals(deletionId, onlyRetained().getDeletion().getDeletionId());
    assertEquals(1L, selectLong("SELECT COUNT(*) FROM entity_deletion"));
  }

  @TestTemplate
  public void testDeleteMissingTableIsNotFound() {
    IcebergTableDeletionLifecycle lifecycle = lifecycle(true, 86_400_000L);
    TableIdentifier missing =
        TableIdentifier.of(org.apache.iceberg.catalog.Namespace.of(SCHEMA), "missing");

    assertThrows(
        NoSuchTableException.class, () -> lifecycle.delete(requestContext, missing, false));
  }

  @TestTemplate
  public void testDisabledSoftDeleteKeepsBothRequestsOnLegacyPaths() throws SQLException {
    IcebergTableDeletionLifecycle disabled = lifecycle(false, 86_400_000L);

    assertFalse(disabled.manages(false));
    assertFalse(disabled.manages(true));
    assertThrows(
        IllegalStateException.class,
        () -> disabled.delete(requestContext, icebergIdentifier, true));
    assertEquals(0L, selectLong("SELECT COUNT(*) FROM entity_deletion"));
  }

  @TestTemplate
  public void testUnavailableLifecyclePreservesLegacyRouting() {
    IcebergTableDeletionLifecycle lifecycle = lifecycle(true, 86_400_000L, false);

    assertFalse(lifecycle.manages(false));
    assertFalse(lifecycle.manages(true));
    assertFalse(lifecycle.isNameReserved(CATALOG, icebergIdentifier));
    assertTrue(lifecycle.reservedTableNames(CATALOG, icebergIdentifier.namespace()).isEmpty());
  }

  private IcebergTableDeletionLifecycle lifecycle(boolean enabled, long retentionMs) {
    return lifecycle(enabled, retentionMs, true);
  }

  private IcebergTableDeletionLifecycle lifecycle(
      boolean enabled, long retentionMs, boolean available) {
    Map<String, String> properties = new HashMap<>();
    properties.put("soft-delete.enabled", String.valueOf(enabled));
    properties.put("soft-delete.retention-ms", String.valueOf(retentionMs));
    return new IcebergTableDeletionLifecycle(new IcebergConfig(properties), available);
  }

  private IcebergRetainedTableDeletion onlyRetained() throws SQLException {
    String deletionId = selectString("SELECT deletion_id FROM entity_deletion");
    EntityDeletionPO deletion = EntityDeletionService.getInstance().get(deletionId);
    TablePO table = TableDeletionService.getInstance().getRetainedTable(deletionId);
    assertNotNull(deletion);
    assertNotNull(table);
    return IcebergRetainedTableDeletion.builder().deletion(deletion).table(table).build();
  }

  private long tableId() throws SQLException {
    return selectLong("SELECT table_id FROM table_meta WHERE table_name = '" + TABLE + "'");
  }

  private long maxChangeId() {
    return SessionUtils.doWithCommitAndFetchResult(
        EntityChangeLogMapper.class, EntityChangeLogMapper::selectMaxChangeId);
  }

  private void assertChange(long after, OperateType expected) {
    List<EntityChangeRecord> changes =
        SessionUtils.doWithCommitAndFetchResult(
            EntityChangeLogMapper.class, mapper -> mapper.selectEntityChanges(after, 10));
    assertEquals(1, changes.size());
    assertEquals(expected, changes.get(0).getOperateType());
    assertEquals(gravitinoIdentifier.toString(), changes.get(0).getFullName());
  }

  private String selectString(String sql) throws SQLException {
    try (SqlSession session =
            SqlSessionFactoryHelper.getInstance().getSqlSessionFactory().openSession(true);
        Connection connection = session.getConnection();
        Statement statement = connection.createStatement();
        ResultSet resultSet = statement.executeQuery(sql)) {
      assertTrue(resultSet.next());
      return resultSet.getString(1);
    }
  }

  private long selectLong(String sql) throws SQLException {
    try (SqlSession session =
            SqlSessionFactoryHelper.getInstance().getSqlSessionFactory().openSession(true);
        Connection connection = session.getConnection();
        Statement statement = connection.createStatement();
        ResultSet resultSet = statement.executeQuery(sql)) {
      assertTrue(resultSet.next());
      return resultSet.getLong(1);
    }
  }
}
