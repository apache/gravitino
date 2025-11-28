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

import static org.apache.gravitino.file.Fileset.LOCATION_NAME_UNKNOWN;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.Entity;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.authorization.Privileges;
import org.apache.gravitino.authorization.SecurableObject;
import org.apache.gravitino.authorization.SecurableObjects;
import org.apache.gravitino.file.Fileset;
import org.apache.gravitino.integration.test.util.CloseContainerExtension;
import org.apache.gravitino.integration.test.util.PrintFuncNameExtension;
import org.apache.gravitino.meta.AuditInfo;
import org.apache.gravitino.meta.BaseMetalake;
import org.apache.gravitino.meta.CatalogEntity;
import org.apache.gravitino.meta.FilesetEntity;
import org.apache.gravitino.meta.GroupEntity;
import org.apache.gravitino.meta.ModelEntity;
import org.apache.gravitino.meta.PolicyEntity;
import org.apache.gravitino.meta.RoleEntity;
import org.apache.gravitino.meta.SchemaEntity;
import org.apache.gravitino.meta.SchemaVersion;
import org.apache.gravitino.meta.TableEntity;
import org.apache.gravitino.meta.TopicEntity;
import org.apache.gravitino.meta.UserEntity;
import org.apache.gravitino.policy.Policy;
import org.apache.gravitino.policy.PolicyContents;
import org.apache.gravitino.storage.RandomIdGenerator;
import org.apache.gravitino.storage.relational.session.SqlSessionFactoryHelper;
import org.apache.ibatis.session.SqlSession;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.extension.ExtendWith;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@ExtendWith({
  BackendTestExtension.class,
  PrintFuncNameExtension.class,
  CloseContainerExtension.class
})
public abstract class TestJDBCBackend {
  protected static final AuditInfo AUDIT_INFO =
      AuditInfo.builder().withCreator("creator").withCreateTime(Instant.now()).build();

  protected String backendType;
  protected RelationalBackend backend;

  public void setBackend(RelationalBackend backend) {
    this.backend = backend;
  }

  public void setBackendType(String backendType) {
    this.backendType = backendType;
  }

  @BeforeEach
  public void init() throws SQLException {
    truncateAllTables();
  }

  private void truncateAllTables() throws SQLException {
    try (SqlSession sqlSession =
        SqlSessionFactoryHelper.getInstance().getSqlSessionFactory().openSession(true)) {
      try (Connection connection = sqlSession.getConnection()) {
        try (Statement statement = connection.createStatement()) {
          if ("postgresql".equalsIgnoreCase(backendType)) {
            truncateAllTablesForPostgreSQL(connection);
          } else {
            String query = "SHOW TABLES";
            List<String> tableList = new ArrayList<>();
            try (ResultSet rs = statement.executeQuery(query)) {
              while (rs.next()) {
                tableList.add(rs.getString(1));
              }
            }
            for (String table : tableList) {
              statement.execute("TRUNCATE TABLE " + table);
            }
          }
        }
      }
    }
  }

  private void truncateAllTablesForPostgreSQL(Connection connection) throws SQLException {
    List<String> tableList = new ArrayList<>();
    try (Statement statement = connection.createStatement()) {
      String query =
          "SELECT table_name FROM information_schema.tables WHERE table_schema = current_schema()";
      try (ResultSet rs = statement.executeQuery(query)) {
        while (rs.next()) {
          tableList.add(rs.getString(1));
        }
      }

      if (tableList.isEmpty()) {
        return;
      }

      // Merge all TRUNCATE operations into a single DO block to improve efficiency
      StringBuilder pgTruncateCommand = new StringBuilder("DO $$ BEGIN\n");
      for (String table : tableList) {
        pgTruncateCommand.append(
            String.format("TRUNCATE TABLE %s RESTART IDENTITY CASCADE;", table));
      }
      pgTruncateCommand.append("END $$;");
      statement.execute(pgTruncateCommand.toString());
    }
  }

  protected BaseMetalake createAndInsertMakeLake(String metalakeName) throws IOException {
    BaseMetalake metalake =
        createBaseMakeLake(RandomIdGenerator.INSTANCE.nextId(), metalakeName, AUDIT_INFO);
    backend.insert(metalake, false);
    return metalake;
  }

  protected CatalogEntity createAndInsertCatalog(String metalakeName, String catalogName)
      throws IOException {
    CatalogEntity catalog =
        createCatalog(
            RandomIdGenerator.INSTANCE.nextId(),
            Namespace.of(metalakeName),
            catalogName,
            AUDIT_INFO);
    backend.insert(catalog, false);
    return catalog;
  }

  protected SchemaEntity createAndInsertSchema(
      String metalakeName, String catalogName, String schemaName) throws IOException {
    SchemaEntity schema =
        createSchemaEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            Namespace.of(metalakeName, catalogName),
            schemaName,
            AUDIT_INFO);
    backend.insert(schema, false);
    return schema;
  }

  protected boolean legacyRecordExistsInDB(Long id, Entity.EntityType entityType) {
    String tableName;
    String idColumnName;

    switch (entityType) {
      case METALAKE:
        tableName = "metalake_meta";
        idColumnName = "metalake_id";
        break;
      case CATALOG:
        tableName = "catalog_meta";
        idColumnName = "catalog_id";
        break;
      case SCHEMA:
        tableName = "schema_meta";
        idColumnName = "schema_id";
        break;
      case TABLE:
        tableName = "table_meta";
        idColumnName = "table_id";
        break;
      case FILESET:
        tableName = "fileset_meta";
        idColumnName = "fileset_id";
        break;
      case TOPIC:
        tableName = "topic_meta";
        idColumnName = "topic_id";
        break;
      case MODEL:
        tableName = "model_meta";
        idColumnName = "model_id";
        break;
      case ROLE:
        tableName = "role_meta";
        idColumnName = "role_id";
        break;
      case USER:
        tableName = "user_meta";
        idColumnName = "user_id";
        break;
      case GROUP:
        tableName = "group_meta";
        idColumnName = "group_id";
        break;
      case TAG:
        tableName = "tag_meta";
        idColumnName = "tag_id";
        break;
      case POLICY:
        tableName = "policy_meta";
        idColumnName = "policy_id";
        break;
      default:
        throw new IllegalArgumentException("Unsupported entity type: " + entityType);
    }

    try (SqlSession sqlSession =
            SqlSessionFactoryHelper.getInstance().getSqlSessionFactory().openSession(true);
        Connection connection = sqlSession.getConnection();
        Statement statement = connection.createStatement();
        ResultSet rs =
            statement.executeQuery(
                String.format(
                    "SELECT * FROM %s WHERE %s = %d AND deleted_at != 0",
                    tableName, idColumnName, id))) {
      return rs.next();
    } catch (SQLException e) {
      throw new RuntimeException("SQL execution failed", e);
    }
  }

  protected Map<Integer, Long> listFilesetVersions(Long filesetId) {
    Map<Integer, Long> versionDeletedTime = new HashMap<>();
    try (SqlSession sqlSession =
            SqlSessionFactoryHelper.getInstance().getSqlSessionFactory().openSession(true);
        Connection connection = sqlSession.getConnection();
        Statement statement = connection.createStatement();
        ResultSet rs =
            statement.executeQuery(
                String.format(
                    "SELECT version, deleted_at FROM fileset_version_info WHERE fileset_id = %d",
                    filesetId))) {
      while (rs.next()) {
        versionDeletedTime.put(rs.getInt("version"), rs.getLong("deleted_at"));
      }
    } catch (SQLException e) {
      throw new RuntimeException("SQL execution failed", e);
    }
    return versionDeletedTime;
  }

  protected Map<Integer, Long> listPolicyVersions(Long policyId) {
    Map<Integer, Long> versionDeletedTime = new HashMap<>();
    try (SqlSession sqlSession =
            SqlSessionFactoryHelper.getInstance().getSqlSessionFactory().openSession(true);
        Connection connection = sqlSession.getConnection();
        Statement statement = connection.createStatement();
        ResultSet rs =
            statement.executeQuery(
                String.format(
                    "SELECT version, deleted_at FROM policy_version_info WHERE policy_id = %d",
                    policyId))) {
      while (rs.next()) {
        versionDeletedTime.put(rs.getInt("version"), rs.getLong("deleted_at"));
      }
    } catch (SQLException e) {
      throw new RuntimeException("SQL execution failed", e);
    }
    return versionDeletedTime;
  }

  protected Integer countOwnerRel(Long metalakeId) {
    try (SqlSession sqlSession =
            SqlSessionFactoryHelper.getInstance().getSqlSessionFactory().openSession(true);
        Connection connection = sqlSession.getConnection();
        Statement statement1 = connection.createStatement();
        ResultSet rs1 =
            statement1.executeQuery(
                String.format(
                    "SELECT count(*) FROM owner_meta WHERE metalake_id = %d", metalakeId))) {
      if (rs1.next()) {
        return rs1.getInt(1);
      } else {
        throw new RuntimeException("Doesn't contain data");
      }
    } catch (SQLException se) {
      throw new RuntimeException("SQL execution failed", se);
    }
  }

  protected Integer countActiveOwnerRel(long ownerId) {
    try (SqlSession sqlSession =
            SqlSessionFactoryHelper.getInstance().getSqlSessionFactory().openSession(true);
        Connection connection = sqlSession.getConnection();
        Statement statement1 = connection.createStatement();
        ResultSet rs1 =
            statement1.executeQuery(
                String.format(
                    "SELECT count(*) FROM owner_meta WHERE owner_id = %d AND deleted_at = 0",
                    ownerId))) {
      if (rs1.next()) {
        return rs1.getInt(1);
      } else {
        throw new RuntimeException("Doesn't contain data");
      }
    } catch (SQLException se) {
      throw new RuntimeException("SQL execution failed", se);
    }
  }

  protected BaseMetalake createBaseMakeLake(Long id, String name, AuditInfo auditInfo) {
    return BaseMetalake.builder()
        .withId(id)
        .withName(name)
        .withAuditInfo(auditInfo)
        .withComment("")
        .withProperties(null)
        .withVersion(SchemaVersion.V_0_1)
        .build();
  }

  protected CatalogEntity createCatalog(
      Long id, Namespace namespace, String name, AuditInfo auditInfo) {
    return CatalogEntity.builder()
        .withId(id)
        .withName(name)
        .withNamespace(namespace)
        .withType(Catalog.Type.RELATIONAL)
        .withProvider("test")
        .withComment("")
        .withProperties(null)
        .withAuditInfo(auditInfo)
        .build();
  }

  protected PolicyEntity createPolicy(Long id, Namespace ns, String name, AuditInfo auditInfo) {
    return PolicyEntity.builder()
        .withId(id)
        .withNamespace(ns)
        .withName(name)
        .withPolicyType(Policy.BuiltInType.CUSTOM)
        .withComment("")
        .withEnabled(true)
        .withContent(
            PolicyContents.custom(
                ImmutableMap.of("filed1", 123),
                ImmutableSet.of(
                    MetadataObject.Type.CATALOG,
                    MetadataObject.Type.SCHEMA,
                    MetadataObject.Type.TABLE,
                    MetadataObject.Type.FILESET,
                    MetadataObject.Type.MODEL,
                    MetadataObject.Type.TOPIC),
                null))
        .withAuditInfo(auditInfo)
        .build();
  }

  protected SchemaEntity createSchemaEntity(
      Long id, Namespace namespace, String name, AuditInfo auditInfo) {
    return SchemaEntity.builder()
        .withId(id)
        .withName(name)
        .withNamespace(namespace)
        .withComment("")
        .withProperties(null)
        .withAuditInfo(auditInfo)
        .build();
  }

  protected TableEntity createTableEntity(
      Long id, Namespace namespace, String name, AuditInfo auditInfo) {
    return TableEntity.builder()
        .withId(id)
        .withName(name)
        .withNamespace(namespace)
        .withAuditInfo(auditInfo)
        .build();
  }

  protected FilesetEntity createFilesetEntity(
      Long id, Namespace namespace, String name, AuditInfo auditInfo) {
    return FilesetEntity.builder()
        .withId(id)
        .withName(name)
        .withNamespace(namespace)
        .withFilesetType(Fileset.Type.MANAGED)
        .withStorageLocations(ImmutableMap.of(LOCATION_NAME_UNKNOWN, "/tmp"))
        .withComment("")
        .withProperties(new HashMap<>())
        .withAuditInfo(auditInfo)
        .build();
  }

  protected TopicEntity createTopicEntity(
      Long id, Namespace namespace, String name, AuditInfo auditInfo) {
    return TopicEntity.builder()
        .withId(id)
        .withName(name)
        .withNamespace(namespace)
        .withComment("test comment")
        .withProperties(ImmutableMap.of("key", "value"))
        .withAuditInfo(auditInfo)
        .build();
  }

  protected UserEntity createUserEntity(
      Long id, Namespace namespace, String name, AuditInfo auditInfo) {
    return UserEntity.builder()
        .withId(id)
        .withName(name)
        .withNamespace(namespace)
        .withRoleNames(null)
        .withRoleIds(null)
        .withAuditInfo(auditInfo)
        .build();
  }

  protected UserEntity createUserEntity(
      Long id,
      Namespace namespace,
      String name,
      AuditInfo auditInfo,
      List<String> roleNames,
      List<Long> roleIds) {
    return UserEntity.builder()
        .withId(id)
        .withName(name)
        .withNamespace(namespace)
        .withRoleNames(roleNames)
        .withRoleIds(roleIds)
        .withAuditInfo(auditInfo)
        .build();
  }

  protected RoleEntity createRoleEntity(
      Long id, Namespace namespace, String name, AuditInfo auditInfo, String catalogName) {
    SecurableObject securableObject =
        SecurableObjects.ofCatalog(catalogName, Lists.newArrayList(Privileges.UseCatalog.allow()));

    return RoleEntity.builder()
        .withId(id)
        .withName(name)
        .withNamespace(namespace)
        .withProperties(null)
        .withAuditInfo(auditInfo)
        .withSecurableObjects(Lists.newArrayList(securableObject))
        .build();
  }

  protected GroupEntity createGroupEntity(
      Long id,
      Namespace namespace,
      String name,
      AuditInfo auditInfo,
      List<String> roleNames,
      List<Long> roleIds) {
    return GroupEntity.builder()
        .withId(id)
        .withName(name)
        .withNamespace(namespace)
        .withRoleNames(roleNames)
        .withRoleIds(roleIds)
        .withAuditInfo(auditInfo)
        .build();
  }

  protected RoleEntity createRoleEntity(
      Long id,
      Namespace namespace,
      String name,
      AuditInfo auditInfo,
      List<SecurableObject> securableObjects,
      Map<String, String> properties) {
    return RoleEntity.builder()
        .withId(id)
        .withName(name)
        .withProperties(properties)
        .withNamespace(namespace)
        .withAuditInfo(auditInfo)
        .withSecurableObjects(securableObjects)
        .build();
  }

  protected ModelEntity createModelEntity(
      Long id,
      Namespace namespace,
      String name,
      String comment,
      Integer latestVersion,
      Map<String, String> properties,
      AuditInfo auditInfo) {
    return ModelEntity.builder()
        .withId(id)
        .withName(name)
        .withNamespace(namespace)
        .withComment(comment)
        .withLatestVersion(latestVersion)
        .withProperties(properties)
        .withAuditInfo(auditInfo)
        .build();
  }

  protected void createParentEntities(
      String metalakeName, String catalogName, String schemaName, AuditInfo auditInfo)
      throws IOException {
    createAndInsertMakeLake(metalakeName);
    createAndInsertCatalog(metalakeName, catalogName);
    createAndInsertSchema(metalakeName, catalogName, schemaName);
  }
}
