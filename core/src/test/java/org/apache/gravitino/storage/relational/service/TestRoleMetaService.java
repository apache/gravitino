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

import static org.apache.gravitino.SupportsRelationOperations.Type.OWNER_REL;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import org.apache.gravitino.Entity;
import org.apache.gravitino.EntityAlreadyExistsException;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.authorization.AuthorizationUtils;
import org.apache.gravitino.authorization.Privileges;
import org.apache.gravitino.authorization.SecurableObject;
import org.apache.gravitino.authorization.SecurableObjects;
import org.apache.gravitino.exceptions.NoSuchEntityException;
import org.apache.gravitino.meta.AuditInfo;
import org.apache.gravitino.meta.BaseMetalake;
import org.apache.gravitino.meta.CatalogEntity;
import org.apache.gravitino.meta.FilesetEntity;
import org.apache.gravitino.meta.GroupEntity;
import org.apache.gravitino.meta.RoleEntity;
import org.apache.gravitino.meta.TableEntity;
import org.apache.gravitino.meta.TopicEntity;
import org.apache.gravitino.meta.UserEntity;
import org.apache.gravitino.storage.RandomIdGenerator;
import org.apache.gravitino.storage.relational.TestJDBCBackend;
import org.apache.gravitino.storage.relational.mapper.GroupMetaMapper;
import org.apache.gravitino.storage.relational.mapper.UserMetaMapper;
import org.apache.gravitino.storage.relational.po.GroupPO;
import org.apache.gravitino.storage.relational.po.UserPO;
import org.apache.gravitino.storage.relational.session.SqlSessionFactoryHelper;
import org.apache.gravitino.storage.relational.utils.SessionUtils;
import org.apache.gravitino.utils.CollectionUtils;
import org.apache.ibatis.session.SqlSession;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.TestTemplate;

class TestRoleMetaService extends TestJDBCBackend {

  private static final String METALAKE_NAME = "metalake_for_role_test";

  @TestTemplate
  public void testMetaLifeCycleFromCreationToDeletion() throws IOException {
    BaseMetalake metalake = createAndInsertMakeLake(METALAKE_NAME);
    String catalogName = "catalog";
    createAndInsertCatalog(METALAKE_NAME, catalogName);

    String anotherMetalakeName = "another_metalake_for_role_test";
    String anotherCatalogName = "another_catalog";
    createAndInsertMakeLake(anotherMetalakeName);
    createAndInsertCatalog(anotherMetalakeName, anotherCatalogName);

    RoleEntity role =
        createRoleEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            AuthorizationUtils.ofRoleNamespace(METALAKE_NAME),
            "role",
            AUDIT_INFO,
            catalogName);
    backend.insert(role, false);
    UserEntity user =
        createUserEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            AuthorizationUtils.ofUserNamespace(METALAKE_NAME),
            "user",
            AUDIT_INFO,
            Lists.newArrayList(role.name()),
            Lists.newArrayList(role.id()));
    backend.insert(user, false);
    GroupEntity group =
        createGroupEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            AuthorizationUtils.ofGroupNamespace(METALAKE_NAME),
            "group",
            AUDIT_INFO,
            Lists.newArrayList(role.name()),
            Lists.newArrayList(role.id()));
    backend.insert(group, false);

    RoleEntity anotherRole =
        createRoleEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            AuthorizationUtils.ofRoleNamespace(anotherMetalakeName),
            "another-role",
            AUDIT_INFO,
            anotherCatalogName);
    backend.insert(anotherRole, false);
    UserEntity anotherUser =
        createUserEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            AuthorizationUtils.ofUserNamespace(anotherMetalakeName),
            "another-user",
            AUDIT_INFO,
            Lists.newArrayList(anotherRole.name()),
            Lists.newArrayList(anotherRole.id()));
    backend.insert(anotherUser, false);

    GroupEntity anotherGroup =
        createGroupEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            AuthorizationUtils.ofGroupNamespace(anotherMetalakeName),
            "another-group",
            AUDIT_INFO,
            Lists.newArrayList(anotherRole.name()),
            Lists.newArrayList(anotherRole.id()));
    backend.insert(anotherGroup, false);

    RoleEntity roleEntity = backend.get(role.nameIdentifier(), Entity.EntityType.ROLE);
    assertEquals(role, roleEntity);

    // meta data soft delete
    backend.delete(metalake.nameIdentifier(), Entity.EntityType.METALAKE, true);

    // check existence after soft delete
    assertFalse(backend.exists(role.nameIdentifier(), Entity.EntityType.ROLE));
    assertTrue(backend.exists(anotherRole.nameIdentifier(), Entity.EntityType.ROLE));

    // check legacy record after soft delete
    assertTrue(legacyRecordExistsInDB(role.id(), Entity.EntityType.ROLE));
    assertEquals(2, countRoleRels(role.id()));
    assertEquals(2, countRoleRels(anotherRole.id()));

    // meta data hard delete
    for (Entity.EntityType entityType : Entity.EntityType.values()) {
      backend.hardDeleteLegacyData(entityType, Instant.now().toEpochMilli() + 1000);
    }
    assertFalse(legacyRecordExistsInDB(role.id(), Entity.EntityType.ROLE));
    assertEquals(0, countRoleRels(role.id()));
    assertEquals(2, countRoleRels(anotherRole.id()));
  }

  @TestTemplate
  public void testGetRoleIdByMetalakeIdAndName() throws IOException {
    String catalogName = "catalog";
    String roleNameWithDot = "role.with.dot";
    String roleNameWithoutDot = "roleWithoutDot";
    createAndInsertMakeLake(METALAKE_NAME);
    createAndInsertCatalog(METALAKE_NAME, catalogName);

    RoleEntity roleWithDot =
        createRoleEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            AuthorizationUtils.ofRoleNamespace(METALAKE_NAME),
            roleNameWithDot,
            AUDIT_INFO,
            catalogName);
    backend.insert(roleWithDot, false);

    RoleEntity roleWithoutDot =
        createRoleEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            AuthorizationUtils.ofRoleNamespace(METALAKE_NAME),
            roleNameWithoutDot,
            AUDIT_INFO,
            catalogName);
    backend.insert(roleWithoutDot, false);

    Long metalakeId = MetalakeMetaService.getInstance().getMetalakeIdByName(METALAKE_NAME);

    Long roleIdWithDot =
        RoleMetaService.getInstance().getRoleIdByMetalakeIdAndName(metalakeId, roleNameWithDot);
    assertEquals(roleWithDot.id(), roleIdWithDot);

    Long roleIdWithoutDot =
        RoleMetaService.getInstance().getRoleIdByMetalakeIdAndName(metalakeId, roleNameWithoutDot);
    assertEquals(roleWithoutDot.id(), roleIdWithoutDot);

    UserEntity user =
        createUserEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            AuthorizationUtils.ofUserNamespace(METALAKE_NAME),
            "user",
            AUDIT_INFO);
    backend.insert(user, false);

    backend.insertRelation(
        OWNER_REL,
        roleWithoutDot.nameIdentifier(),
        roleWithoutDot.type(),
        user.nameIdentifier(),
        user.type(),
        true);
    assertEquals(1, countActiveOwnerRel(user.id()));
  }

  @TestTemplate
  void testGetRoleByIdentifier() throws IOException {
    createAndInsertMakeLake(METALAKE_NAME);
    String catalogName = "catalog";
    createAndInsertCatalog(METALAKE_NAME, catalogName);

    RoleMetaService roleMetaService = RoleMetaService.getInstance();
    // get not exist role
    Assertions.assertThrows(
        NoSuchEntityException.class,
        () ->
            roleMetaService.getRoleByIdentifier(AuthorizationUtils.ofRole(METALAKE_NAME, "role1")));

    // get role
    RoleEntity role1 =
        createRoleEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            AuthorizationUtils.ofRoleNamespace(METALAKE_NAME),
            "role1",
            AUDIT_INFO,
            SecurableObjects.ofCatalog(
                catalogName, Lists.newArrayList(Privileges.UseCatalog.allow())),
            ImmutableMap.of("k1", "v1"));
    roleMetaService.insertRole(role1, false);

    assertEquals(role1, roleMetaService.getRoleByIdentifier(role1.nameIdentifier()));
  }

  @TestTemplate
  void testListRoles() throws IOException {
    createAndInsertMakeLake(METALAKE_NAME);
    String catalogName = "catalog";
    createAndInsertCatalog(METALAKE_NAME, catalogName);

    RoleEntity role1 =
        createRoleEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            AuthorizationUtils.ofRoleNamespace(METALAKE_NAME),
            "role1",
            AUDIT_INFO,
            SecurableObjects.ofCatalog(
                catalogName, Lists.newArrayList(Privileges.UseCatalog.allow())),
            ImmutableMap.of("k1", "v1"));

    RoleEntity role2 =
        createRoleEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            AuthorizationUtils.ofRoleNamespace(METALAKE_NAME),
            "role2",
            AUDIT_INFO,
            SecurableObjects.ofCatalog(
                catalogName, Lists.newArrayList(Privileges.UseCatalog.allow())),
            ImmutableMap.of("k1", "v1"));

    backend.insert(role1, false);
    backend.insert(role2, false);

    RoleMetaService roleMetaService = RoleMetaService.getInstance();
    List<RoleEntity> actualRoles =
        roleMetaService.listRolesByNamespace(AuthorizationUtils.ofRoleNamespace(METALAKE_NAME));
    actualRoles.sort(Comparator.comparing(RoleEntity::name));
    List<RoleEntity> expectRoles = Lists.newArrayList(role1, role2);
    assertEquals(expectRoles.size(), actualRoles.size());
    for (int index = 0; index < expectRoles.size(); index++) {
      RoleEntity expectRole = expectRoles.get(index);
      RoleEntity actualRole = actualRoles.get(index);
      assertEquals(expectRole.name(), actualRole.name());
    }
  }

  @TestTemplate
  void testInsertRole() throws IOException {
    createAndInsertMakeLake(METALAKE_NAME);
    String catalogName = "catalog";
    createAndInsertCatalog(METALAKE_NAME, catalogName);
    String anotherCatalogName = "anotherCatalog";
    createAndInsertCatalog(METALAKE_NAME, anotherCatalogName);
    String overwriteCatalogName = "catalogOverwrite";
    createAndInsertCatalog(METALAKE_NAME, overwriteCatalogName);
    String schemaName = "schema";
    createAndInsertSchema(METALAKE_NAME, catalogName, schemaName);

    TopicEntity topic =
        createTopicEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            Namespace.of(METALAKE_NAME, catalogName, schemaName),
            "topic",
            AUDIT_INFO);
    backend.insert(topic, false);

    FilesetEntity fileset =
        createFilesetEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            Namespace.of(METALAKE_NAME, catalogName, schemaName),
            "fileset",
            AUDIT_INFO);
    backend.insert(fileset, false);

    TableEntity table =
        createTableEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            Namespace.of(METALAKE_NAME, catalogName, schemaName),
            "table",
            AUDIT_INFO);
    backend.insert(table, false);

    RoleMetaService roleMetaService = RoleMetaService.getInstance();

    // Test with different securable objects
    SecurableObject metalakeObject =
        SecurableObjects.ofMetalake(
            METALAKE_NAME, Lists.newArrayList(Privileges.CreateCatalog.allow()));
    SecurableObject catalogObject =
        SecurableObjects.ofCatalog(
            catalogName,
            Lists.newArrayList(Privileges.UseCatalog.allow(), Privileges.CreateSchema.deny()));
    SecurableObject schemaObject =
        SecurableObjects.ofSchema(
            catalogObject,
            schemaName,
            Lists.newArrayList(Privileges.UseSchema.allow(), Privileges.CreateTable.allow()));
    SecurableObject topicObject =
        SecurableObjects.ofTopic(
            schemaObject, "topic", Lists.newArrayList(Privileges.ConsumeTopic.allow()));
    SecurableObject filesetObject =
        SecurableObjects.ofFileset(
            schemaObject, "fileset", Lists.newArrayList(Privileges.ReadFileset.allow()));
    SecurableObject tableObject =
        SecurableObjects.ofTable(
            schemaObject, "table", Lists.newArrayList(Privileges.SelectTable.allow()));

    // insert role
    ArrayList<SecurableObject> securableObjects =
        Lists.newArrayList(
            catalogObject,
            metalakeObject,
            schemaObject,
            filesetObject,
            topicObject,
            tableObject,
            SecurableObjects.ofCatalog(
                anotherCatalogName, Lists.newArrayList(Privileges.UseCatalog.allow())));

    securableObjects.sort(Comparator.comparing(SecurableObject::fullName));

    // insert role
    RoleEntity role1 =
        createRoleEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            AuthorizationUtils.ofRoleNamespace(METALAKE_NAME),
            "role1",
            AUDIT_INFO,
            securableObjects,
            ImmutableMap.of("k1", "v1"));
    Assertions.assertThrows(
        NoSuchEntityException.class,
        () -> roleMetaService.getRoleByIdentifier(role1.nameIdentifier()));
    Assertions.assertDoesNotThrow(() -> roleMetaService.insertRole(role1, false));
    assertEquals(role1, roleMetaService.getRoleByIdentifier(role1.nameIdentifier()));

    // insert duplicate role
    RoleEntity role1Exist =
        createRoleEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            AuthorizationUtils.ofRoleNamespace(METALAKE_NAME),
            "role1",
            AUDIT_INFO,
            SecurableObjects.ofCatalog(
                catalogName, Lists.newArrayList(Privileges.UseCatalog.allow())),
            ImmutableMap.of("k1", "v1"));
    Assertions.assertThrows(
        EntityAlreadyExistsException.class, () -> roleMetaService.insertRole(role1Exist, false));

    // insert overwrite
    RoleEntity role1Overwrite =
        createRoleEntity(
            role1.id(),
            AuthorizationUtils.ofRoleNamespace(METALAKE_NAME),
            "role1Overwrite",
            AUDIT_INFO,
            SecurableObjects.ofCatalog(
                overwriteCatalogName, Lists.newArrayList(Privileges.UseCatalog.allow())),
            ImmutableMap.of("k2", "v2"));
    Assertions.assertDoesNotThrow(() -> roleMetaService.insertRole(role1Overwrite, true));
    assertEquals(
        "role1Overwrite",
        roleMetaService.getRoleByIdentifier(role1Overwrite.nameIdentifier()).name());
    assertEquals(
        role1Overwrite, roleMetaService.getRoleByIdentifier(role1Overwrite.nameIdentifier()));
  }

  @TestTemplate
  void testDeleteRole() throws IOException {
    createAndInsertMakeLake(METALAKE_NAME);
    createAndInsertCatalog(METALAKE_NAME, "catalog");

    UserMetaService userMetaService = UserMetaService.getInstance();
    GroupMetaService groupMetaService = GroupMetaService.getInstance();
    RoleMetaService roleMetaService = RoleMetaService.getInstance();

    // delete not exist role
    Assertions.assertThrows(
        NoSuchEntityException.class,
        () -> roleMetaService.deleteRole(AuthorizationUtils.ofRole(METALAKE_NAME, "role1")));

    // delete role
    RoleEntity role1 =
        createRoleEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            AuthorizationUtils.ofRoleNamespace(METALAKE_NAME),
            "role1",
            AUDIT_INFO,
            SecurableObjects.ofCatalog(
                "catalog", Lists.newArrayList(Privileges.UseCatalog.allow())),
            ImmutableMap.of("k1", "v1"));
    Assertions.assertThrows(
        NoSuchEntityException.class,
        () -> roleMetaService.getRoleByIdentifier(role1.nameIdentifier()));
    Assertions.assertDoesNotThrow(() -> roleMetaService.insertRole(role1, false));
    assertEquals(role1, roleMetaService.getRoleByIdentifier(role1.nameIdentifier()));
    Assertions.assertTrue(roleMetaService.deleteRole(role1.nameIdentifier()));
    Assertions.assertThrows(
        NoSuchEntityException.class,
        () -> roleMetaService.getRoleByIdentifier(role1.nameIdentifier()));

    // delete user & groups when delete role
    RoleEntity role2 =
        createRoleEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            AuthorizationUtils.ofRoleNamespace(METALAKE_NAME),
            "role2",
            AUDIT_INFO,
            SecurableObjects.ofCatalog(
                "catalog", Lists.newArrayList(Privileges.UseCatalog.allow())),
            ImmutableMap.of("k1", "v1"));
    roleMetaService.insertRole(role2, false);
    GroupEntity group1 =
        createGroupEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            AuthorizationUtils.ofGroupNamespace(METALAKE_NAME),
            "group1",
            AUDIT_INFO,
            Lists.newArrayList(role2.name()),
            Lists.newArrayList(role2.id()));
    GroupEntity group2 =
        createGroupEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            AuthorizationUtils.ofGroupNamespace(METALAKE_NAME),
            "group2",
            AUDIT_INFO,
            Lists.newArrayList(role2.name()),
            Lists.newArrayList(role2.id()));
    groupMetaService.insertGroup(group1, false);
    groupMetaService.insertGroup(group2, false);
    assertEquals(
        group1.name(), groupMetaService.getGroupByIdentifier(group1.nameIdentifier()).name());
    assertEquals(
        group1.roleNames(),
        groupMetaService.getGroupByIdentifier(group1.nameIdentifier()).roleNames());
    assertEquals(
        group2.name(), groupMetaService.getGroupByIdentifier(group2.nameIdentifier()).name());
    assertEquals(
        group2.roleNames(),
        groupMetaService.getGroupByIdentifier(group2.nameIdentifier()).roleNames());
    UserEntity user1 =
        createUserEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            AuthorizationUtils.ofUserNamespace(METALAKE_NAME),
            "user1",
            AUDIT_INFO,
            Lists.newArrayList(role2.name()),
            Lists.newArrayList(role2.id()));
    userMetaService.insertUser(user1, false);
    assertEquals(user1.name(), userMetaService.getUserByIdentifier(user1.nameIdentifier()).name());
    assertEquals(
        user1.roleNames(), userMetaService.getUserByIdentifier(user1.nameIdentifier()).roleNames());

    Assertions.assertTrue(roleMetaService.deleteRole(role2.nameIdentifier()));
    Assertions.assertThrows(
        NoSuchEntityException.class,
        () -> roleMetaService.getRoleByIdentifier(role2.nameIdentifier()));

    List<UserPO> userRoleRels =
        SessionUtils.doWithCommitAndFetchResult(
            UserMetaMapper.class, mapper -> mapper.listUsersByRoleId(role2.id()));
    assertEquals(user1.name(), userMetaService.getUserByIdentifier(user1.nameIdentifier()).name());
    Assertions.assertTrue(userRoleRels.isEmpty());

    List<GroupPO> groupRoleRels =
        SessionUtils.doWithCommitAndFetchResult(
            GroupMetaMapper.class, mapper -> mapper.listGroupsByRoleId(role2.id()));
    assertEquals(
        group1.name(), groupMetaService.getGroupByIdentifier(group1.nameIdentifier()).name());
    Assertions.assertTrue(groupRoleRels.isEmpty());
  }

  @TestTemplate
  void listRolesBySecurableObject() throws IOException {
    createAndInsertMakeLake(METALAKE_NAME);
    CatalogEntity catalog = createAndInsertCatalog(METALAKE_NAME, "catalog");

    RoleEntity role1 =
        createRoleEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            AuthorizationUtils.ofRoleNamespace(METALAKE_NAME),
            "role1",
            AUDIT_INFO,
            "catalog");

    RoleEntity role2 =
        createRoleEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            AuthorizationUtils.ofRoleNamespace(METALAKE_NAME),
            "role2",
            AUDIT_INFO,
            "catalog");

    RoleMetaService roleMetaService = RoleMetaService.getInstance();
    roleMetaService.insertRole(role1, false);
    roleMetaService.insertRole(role2, false);

    List<RoleEntity> roleEntities =
        roleMetaService.listRolesByMetadataObject(catalog.nameIdentifier(), catalog.type(), true);
    roleEntities.sort(Comparator.comparing(RoleEntity::name));
    assertEquals(Lists.newArrayList(role1, role2), roleEntities);
  }

  @TestTemplate
  void testDeleteMetalake() throws IOException {
    BaseMetalake metalake = createAndInsertMakeLake(METALAKE_NAME);
    CatalogEntity catalog = createAndInsertCatalog(METALAKE_NAME, "catalog");

    UserMetaService userMetaService = UserMetaService.getInstance();
    GroupMetaService groupMetaService = GroupMetaService.getInstance();
    RoleMetaService roleMetaService = RoleMetaService.getInstance();

    RoleEntity role1 =
        createRoleEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            AuthorizationUtils.ofRoleNamespace(METALAKE_NAME),
            "role1",
            AUDIT_INFO,
            "catalog");
    RoleEntity role2 =
        createRoleEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            AuthorizationUtils.ofRoleNamespace(METALAKE_NAME),
            "role2",
            AUDIT_INFO,
            "catalog");
    GroupEntity group1 =
        createGroupEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            AuthorizationUtils.ofGroupNamespace(METALAKE_NAME),
            "group1",
            AUDIT_INFO,
            Lists.newArrayList(role1.name(), role2.name()),
            Lists.newArrayList(role1.id(), role2.id()));
    UserEntity user1 =
        createUserEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            AuthorizationUtils.ofUserNamespace(METALAKE_NAME),
            "user1",
            AUDIT_INFO,
            Lists.newArrayList(role1.name(), role2.name()),
            Lists.newArrayList(role1.id(), role2.id()));
    roleMetaService.insertRole(role1, false);
    roleMetaService.insertRole(role2, false);
    groupMetaService.insertGroup(group1, false);
    userMetaService.insertUser(user1, false);

    assertEquals(role1, roleMetaService.getRoleByIdentifier(role1.nameIdentifier()));
    assertEquals(role2, roleMetaService.getRoleByIdentifier(role2.nameIdentifier()));
    assertEquals(
        group1.name(), groupMetaService.getGroupByIdentifier(group1.nameIdentifier()).name());
    assertEquals(user1.name(), userMetaService.getUserByIdentifier(user1.nameIdentifier()).name());

    List<UserPO> userRole1Rels =
        SessionUtils.doWithCommitAndFetchResult(
            UserMetaMapper.class, mapper -> mapper.listUsersByRoleId(role1.id()));
    List<UserPO> userRole2Rels =
        SessionUtils.doWithCommitAndFetchResult(
            UserMetaMapper.class, mapper -> mapper.listUsersByRoleId(role2.id()));
    List<GroupPO> groupRole1Rels =
        SessionUtils.doWithCommitAndFetchResult(
            GroupMetaMapper.class, mapper -> mapper.listGroupsByRoleId(role1.id()));
    List<GroupPO> groupRole2Rels =
        SessionUtils.doWithCommitAndFetchResult(
            GroupMetaMapper.class, mapper -> mapper.listGroupsByRoleId(role2.id()));
    assertEquals(1, userRole1Rels.size());
    assertEquals(1, userRole2Rels.size());
    assertEquals(1, groupRole1Rels.size());
    assertEquals(1, groupRole2Rels.size());

    Assertions.assertTrue(
        CatalogMetaService.getInstance().deleteCatalog(catalog.nameIdentifier(), false));

    Assertions.assertTrue(
        MetalakeMetaService.getInstance().deleteMetalake(metalake.nameIdentifier(), false));

    Assertions.assertThrows(
        NoSuchEntityException.class,
        () -> roleMetaService.getRoleByIdentifier(role1.nameIdentifier()));
    Assertions.assertThrows(
        NoSuchEntityException.class,
        () -> roleMetaService.getRoleByIdentifier(role2.nameIdentifier()));
    Assertions.assertThrows(
        NoSuchEntityException.class,
        () -> groupMetaService.getGroupByIdentifier(group1.nameIdentifier()));
    Assertions.assertThrows(
        NoSuchEntityException.class,
        () -> userMetaService.getUserByIdentifier(user1.nameIdentifier()));

    Assertions.assertTrue(
        SessionUtils.doWithCommitAndFetchResult(
                UserMetaMapper.class, mapper -> mapper.listUsersByRoleId(role1.id()))
            .isEmpty());
    Assertions.assertTrue(
        SessionUtils.doWithCommitAndFetchResult(
                UserMetaMapper.class, mapper -> mapper.listUsersByRoleId(role2.id()))
            .isEmpty());
    Assertions.assertTrue(
        SessionUtils.doWithCommitAndFetchResult(
                GroupMetaMapper.class, mapper -> mapper.listGroupsByRoleId(role1.id()))
            .isEmpty());
    Assertions.assertTrue(
        SessionUtils.doWithCommitAndFetchResult(
                GroupMetaMapper.class, mapper -> mapper.listGroupsByRoleId(role2.id()))
            .isEmpty());
  }

  @TestTemplate
  void testUpdateRole() throws IOException {
    createAndInsertMakeLake(METALAKE_NAME);
    createAndInsertCatalog(METALAKE_NAME, "catalog");

    RoleMetaService roleMetaService = RoleMetaService.getInstance();
    RoleEntity roleEntity =
        createRoleEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            AuthorizationUtils.ofRoleNamespace(METALAKE_NAME),
            "role1",
            AUDIT_INFO,
            "catalog");
    roleMetaService.insertRole(roleEntity, false);

    // grant privileges to the role
    Function<RoleEntity, RoleEntity> grantUpdater =
        role -> {
          AuditInfo updateAuditInfo =
              AuditInfo.builder()
                  .withCreator(role.auditInfo().creator())
                  .withCreateTime(role.auditInfo().createTime())
                  .withLastModifier("grantRole")
                  .withLastModifiedTime(Instant.now())
                  .build();

          List<SecurableObject> securableObjects = Lists.newArrayList(role.securableObjects());
          securableObjects.add(
              SecurableObjects.ofMetalake(
                  METALAKE_NAME, Lists.newArrayList(Privileges.CreateTable.allow())));

          return RoleEntity.builder()
              .withId(role.id())
              .withName(role.name())
              .withNamespace(role.namespace())
              .withProperties(ImmutableMap.of("k1", "v1"))
              .withSecurableObjects(securableObjects)
              .withAuditInfo(updateAuditInfo)
              .build();
        };

    Assertions.assertNotNull(roleMetaService.updateRole(roleEntity.nameIdentifier(), grantUpdater));
    RoleEntity grantRole = roleMetaService.getRoleByIdentifier(roleEntity.nameIdentifier());

    assertEquals(grantRole.id(), roleEntity.id());
    assertEquals(grantRole.name(), roleEntity.name());
    assertEquals("creator", grantRole.auditInfo().creator());
    assertEquals("grantRole", grantRole.auditInfo().lastModifier());
    Assertions.assertTrue(
        CollectionUtils.isEqualCollection(
            Lists.newArrayList(
                SecurableObjects.ofCatalog(
                    "catalog", Lists.newArrayList(Privileges.UseCatalog.allow())),
                SecurableObjects.ofMetalake(
                    METALAKE_NAME, Lists.newArrayList(Privileges.CreateTable.allow()))),
            grantRole.securableObjects()));

    // revoke privileges from the role
    Function<RoleEntity, RoleEntity> revokeUpdater =
        role -> {
          AuditInfo updateAuditInfo =
              AuditInfo.builder()
                  .withCreator(role.auditInfo().creator())
                  .withCreateTime(role.auditInfo().createTime())
                  .withLastModifier("revokeRole")
                  .withLastModifiedTime(Instant.now())
                  .build();

          List<SecurableObject> securableObjects = Lists.newArrayList(role.securableObjects());
          securableObjects.removeIf(
              securableObject ->
                  securableObject.type() == SecurableObject.Type.CATALOG
                      && securableObject.privileges().contains(Privileges.UseCatalog.allow()));

          return RoleEntity.builder()
              .withId(role.id())
              .withName(role.name())
              .withNamespace(role.namespace())
              .withAuditInfo(updateAuditInfo)
              .withProperties(role.properties())
              .withSecurableObjects(securableObjects)
              .withAuditInfo(updateAuditInfo)
              .build();
        };
    roleMetaService.updateRole(roleEntity.nameIdentifier(), revokeUpdater);

    RoleEntity revokeRole = roleMetaService.getRoleByIdentifier(roleEntity.nameIdentifier());
    assertEquals(revokeRole.id(), roleEntity.id());
    assertEquals(revokeRole.name(), roleEntity.name());
    assertEquals("creator", revokeRole.auditInfo().creator());
    assertEquals("revokeRole", revokeRole.auditInfo().lastModifier());
    assertEquals(
        Lists.newArrayList(
            SecurableObjects.ofMetalake(
                METALAKE_NAME, Lists.newArrayList(Privileges.CreateTable.allow()))),
        revokeRole.securableObjects());

    // grant and revoke privileges for the role
    Function<RoleEntity, RoleEntity> grantRevokeUpdater =
        role -> {
          AuditInfo updateAuditInfo =
              AuditInfo.builder()
                  .withCreator(role.auditInfo().creator())
                  .withCreateTime(role.auditInfo().createTime())
                  .withLastModifier("grantRevokeRole")
                  .withLastModifiedTime(Instant.now())
                  .build();

          List<SecurableObject> securableObjects = Lists.newArrayList(role.securableObjects());
          securableObjects.remove(0);
          securableObjects.add(
              SecurableObjects.ofCatalog(
                  "catalog", Lists.newArrayList(Privileges.CreateTable.allow())));

          return RoleEntity.builder()
              .withId(role.id())
              .withName(role.name())
              .withNamespace(role.namespace())
              .withAuditInfo(updateAuditInfo)
              .withProperties(role.properties())
              .withSecurableObjects(securableObjects)
              .withAuditInfo(updateAuditInfo)
              .build();
        };
    roleMetaService.updateRole(roleEntity.nameIdentifier(), grantRevokeUpdater);

    RoleEntity grantRevokeRole = roleMetaService.getRoleByIdentifier(roleEntity.nameIdentifier());
    assertEquals(grantRevokeRole.id(), roleEntity.id());
    assertEquals(grantRevokeRole.name(), roleEntity.name());
    assertEquals("creator", grantRevokeRole.auditInfo().creator());
    assertEquals("grantRevokeRole", grantRevokeRole.auditInfo().lastModifier());
    assertEquals(
        Lists.newArrayList(
            SecurableObjects.ofCatalog(
                "catalog", Lists.newArrayList(Privileges.CreateTable.allow()))),
        grantRevokeRole.securableObjects());

    // revoke multiple securable objects
    roleMetaService.updateRole(roleEntity.nameIdentifier(), grantUpdater);
    Function<RoleEntity, RoleEntity> revokeMultipleUpdater =
        role -> {
          AuditInfo updateAuditInfo =
              AuditInfo.builder()
                  .withCreator(role.auditInfo().creator())
                  .withCreateTime(role.auditInfo().createTime())
                  .withLastModifier("revokeMultiple")
                  .withLastModifiedTime(Instant.now())
                  .build();

          return RoleEntity.builder()
              .withId(role.id())
              .withName(role.name())
              .withNamespace(role.namespace())
              .withAuditInfo(updateAuditInfo)
              .withProperties(role.properties())
              .withSecurableObjects(Collections.emptyList())
              .withAuditInfo(updateAuditInfo)
              .build();
        };

    roleMetaService.updateRole(roleEntity.nameIdentifier(), revokeMultipleUpdater);
    RoleEntity revokeMultipleRole =
        roleMetaService.getRoleByIdentifier(roleEntity.nameIdentifier());
    assertEquals(revokeMultipleRole.id(), roleEntity.id());
    assertEquals(revokeMultipleRole.name(), roleEntity.name());
    assertEquals("creator", revokeMultipleRole.auditInfo().creator());
    assertEquals("revokeMultiple", revokeMultipleRole.auditInfo().lastModifier());
    Assertions.assertTrue(revokeMultipleRole.securableObjects().isEmpty());
  }

  @TestTemplate
  void testDeleteMetalakeCascade() throws IOException {
    BaseMetalake metalake = createAndInsertMakeLake(METALAKE_NAME);
    createAndInsertCatalog(METALAKE_NAME, "catalog");

    UserMetaService userMetaService = UserMetaService.getInstance();
    GroupMetaService groupMetaService = GroupMetaService.getInstance();
    RoleMetaService roleMetaService = RoleMetaService.getInstance();

    RoleEntity role1 =
        createRoleEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            AuthorizationUtils.ofRoleNamespace(METALAKE_NAME),
            "role1",
            AUDIT_INFO,
            "catalog");
    RoleEntity role2 =
        createRoleEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            AuthorizationUtils.ofRoleNamespace(METALAKE_NAME),
            "role2",
            AUDIT_INFO,
            "catalog");
    GroupEntity group1 =
        createGroupEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            AuthorizationUtils.ofGroupNamespace(METALAKE_NAME),
            "group1",
            AUDIT_INFO,
            Lists.newArrayList(role1.name(), role2.name()),
            Lists.newArrayList(role1.id(), role2.id()));
    UserEntity user1 =
        createUserEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            AuthorizationUtils.ofUserNamespace(METALAKE_NAME),
            "user1",
            AUDIT_INFO,
            Lists.newArrayList(role1.name(), role2.name()),
            Lists.newArrayList(role1.id(), role2.id()));
    roleMetaService.insertRole(role1, false);
    roleMetaService.insertRole(role2, false);
    groupMetaService.insertGroup(group1, false);
    userMetaService.insertUser(user1, false);

    assertEquals(role1, roleMetaService.getRoleByIdentifier(role1.nameIdentifier()));
    assertEquals(role2, roleMetaService.getRoleByIdentifier(role2.nameIdentifier()));
    assertEquals(
        group1.name(), groupMetaService.getGroupByIdentifier(group1.nameIdentifier()).name());
    assertEquals(user1.name(), userMetaService.getUserByIdentifier(user1.nameIdentifier()).name());

    List<UserPO> userRole1Rels =
        SessionUtils.doWithCommitAndFetchResult(
            UserMetaMapper.class, mapper -> mapper.listUsersByRoleId(role1.id()));
    List<UserPO> userRole2Rels =
        SessionUtils.doWithCommitAndFetchResult(
            UserMetaMapper.class, mapper -> mapper.listUsersByRoleId(role2.id()));
    List<GroupPO> groupRole1Rels =
        SessionUtils.doWithCommitAndFetchResult(
            GroupMetaMapper.class, mapper -> mapper.listGroupsByRoleId(role1.id()));
    List<GroupPO> groupRole2Rels =
        SessionUtils.doWithCommitAndFetchResult(
            GroupMetaMapper.class, mapper -> mapper.listGroupsByRoleId(role2.id()));
    assertEquals(1, userRole1Rels.size());
    assertEquals(1, userRole2Rels.size());
    assertEquals(1, groupRole1Rels.size());
    assertEquals(1, groupRole2Rels.size());

    Assertions.assertTrue(
        MetalakeMetaService.getInstance().deleteMetalake(metalake.nameIdentifier(), true));

    Assertions.assertThrows(
        NoSuchEntityException.class,
        () -> roleMetaService.getRoleByIdentifier(role1.nameIdentifier()));
    Assertions.assertThrows(
        NoSuchEntityException.class,
        () -> roleMetaService.getRoleByIdentifier(role2.nameIdentifier()));
    Assertions.assertThrows(
        NoSuchEntityException.class,
        () -> groupMetaService.getGroupByIdentifier(group1.nameIdentifier()));
    Assertions.assertThrows(
        NoSuchEntityException.class,
        () -> userMetaService.getUserByIdentifier(user1.nameIdentifier()));

    Assertions.assertTrue(
        SessionUtils.doWithCommitAndFetchResult(
                UserMetaMapper.class, mapper -> mapper.listUsersByRoleId(role1.id()))
            .isEmpty());
    Assertions.assertTrue(
        SessionUtils.doWithCommitAndFetchResult(
                UserMetaMapper.class, mapper -> mapper.listUsersByRoleId(role2.id()))
            .isEmpty());
    Assertions.assertTrue(
        SessionUtils.doWithCommitAndFetchResult(
                GroupMetaMapper.class, mapper -> mapper.listGroupsByRoleId(role1.id()))
            .isEmpty());
    Assertions.assertTrue(
        SessionUtils.doWithCommitAndFetchResult(
                GroupMetaMapper.class, mapper -> mapper.listGroupsByRoleId(role2.id()))
            .isEmpty());
  }

  @TestTemplate
  void testDeleteRoleMetasByLegacyTimeline() throws IOException {
    BaseMetalake metalake = createAndInsertMakeLake(METALAKE_NAME);
    createAndInsertCatalog(METALAKE_NAME, "catalog");

    UserMetaService userMetaService = UserMetaService.getInstance();
    GroupMetaService groupMetaService = GroupMetaService.getInstance();
    RoleMetaService roleMetaService = RoleMetaService.getInstance();

    RoleEntity role1 =
        createRoleEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            AuthorizationUtils.ofRoleNamespace(METALAKE_NAME),
            "role1",
            AUDIT_INFO,
            "catalog");
    RoleEntity role2 =
        createRoleEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            AuthorizationUtils.ofRoleNamespace(METALAKE_NAME),
            "role2",
            AUDIT_INFO,
            "catalog");
    roleMetaService.insertRole(role1, false);
    roleMetaService.insertRole(role2, false);

    UserEntity user1 =
        createUserEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            AuthorizationUtils.ofUserNamespace(METALAKE_NAME),
            "user1",
            AUDIT_INFO,
            Lists.newArrayList(role1.name(), role2.name()),
            Lists.newArrayList(role1.id(), role2.id()));
    userMetaService.insertUser(user1, false);

    GroupEntity group1 =
        createGroupEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            AuthorizationUtils.ofUserNamespace(METALAKE_NAME),
            "group1",
            AUDIT_INFO,
            Lists.newArrayList(role1.name(), role2.name()),
            Lists.newArrayList(role1.id(), role2.id()));
    groupMetaService.insertGroup(group1, false);

    // hard delete before soft delete
    int deletedCount =
        roleMetaService.deleteRoleMetasByLegacyTimeline(Instant.now().toEpochMilli() + 1000, 4);
    assertEquals(0, deletedCount);

    assertEquals(role1, roleMetaService.getRoleByIdentifier(role1.nameIdentifier()));
    assertEquals(role2, roleMetaService.getRoleByIdentifier(role2.nameIdentifier()));
    assertEquals(
        group1.name(), groupMetaService.getGroupByIdentifier(group1.nameIdentifier()).name());
    assertEquals(user1.name(), userMetaService.getUserByIdentifier(user1.nameIdentifier()).name());
    assertEquals(2, roleMetaService.listRolesByUserId(user1.id()).size());
    assertEquals(2, roleMetaService.listRolesByGroupId(group1.id()).size());
    assertEquals(2, countRoles(metalake.id()));
    assertEquals(2, countUserRoleRels());
    assertEquals(2, countGroupRoleRels());

    // delete metalake
    Assertions.assertTrue(
        MetalakeMetaService.getInstance().deleteMetalake(metalake.nameIdentifier(), true));

    Assertions.assertThrows(
        NoSuchEntityException.class,
        () -> roleMetaService.getRoleByIdentifier(role1.nameIdentifier()));
    Assertions.assertThrows(
        NoSuchEntityException.class,
        () -> roleMetaService.getRoleByIdentifier(role2.nameIdentifier()));
    Assertions.assertThrows(
        NoSuchEntityException.class,
        () -> groupMetaService.getGroupByIdentifier(group1.nameIdentifier()));
    Assertions.assertThrows(
        NoSuchEntityException.class,
        () -> userMetaService.getUserByIdentifier(user1.nameIdentifier()));
    assertEquals(0, roleMetaService.listRolesByGroupId(user1.id()).size());
    assertEquals(0, roleMetaService.listRolesByGroupId(group1.id()).size());
    assertEquals(2, countRoles(metalake.id()));
    assertEquals(2, countUserRoleRels());
    assertEquals(2, countGroupRoleRels());

    // hard delete after soft delete
    deletedCount =
        roleMetaService.deleteRoleMetasByLegacyTimeline(Instant.now().toEpochMilli() + 1000, 1);
    assertEquals(3, deletedCount); // delete 1 role + 1 userRoleRel + 1 groupRoleRel
    assertEquals(1, countRoles(metalake.id())); // 2 - 1
    assertEquals(1, countUserRoleRels()); // 2 - 1
    assertEquals(1, countGroupRoleRels()); // 2 - 1

    deletedCount =
        roleMetaService.deleteRoleMetasByLegacyTimeline(Instant.now().toEpochMilli() + 1000, 1);
    assertEquals(3, deletedCount);
    assertEquals(0, countRoles(metalake.id()));
    assertEquals(0, countUserRoleRels());
    assertEquals(0, countGroupRoleRels());

    deletedCount =
        roleMetaService.deleteRoleMetasByLegacyTimeline(Instant.now().toEpochMilli() + 1000, 1);
    assertEquals(0, deletedCount); // no more to delete
  }

  private Integer countRoles(Long metalakeId) {
    int count = 0;
    try (SqlSession sqlSession =
            SqlSessionFactoryHelper.getInstance().getSqlSessionFactory().openSession(true);
        Connection connection = sqlSession.getConnection();
        Statement statement = connection.createStatement();
        ResultSet rs =
            statement.executeQuery(
                String.format(
                    "SELECT count(*) FROM role_meta WHERE metalake_id = %d", metalakeId))) {
      while (rs.next()) {
        count = rs.getInt(1);
      }
    } catch (SQLException e) {
      throw new RuntimeException("SQL execution failed", e);
    }
    return count;
  }

  private Integer countUserRoleRels() {
    int count = 0;
    try (SqlSession sqlSession =
            SqlSessionFactoryHelper.getInstance().getSqlSessionFactory().openSession(true);
        Connection connection = sqlSession.getConnection();
        Statement statement = connection.createStatement();
        ResultSet rs = statement.executeQuery("SELECT count(*) FROM user_role_rel")) {
      while (rs.next()) {
        count = rs.getInt(1);
      }
    } catch (SQLException e) {
      throw new RuntimeException("SQL execution failed", e);
    }
    return count;
  }

  private Integer countGroupRoleRels() {
    int count = 0;
    try (SqlSession sqlSession =
            SqlSessionFactoryHelper.getInstance().getSqlSessionFactory().openSession(true);
        Connection connection = sqlSession.getConnection();
        Statement statement = connection.createStatement();
        ResultSet rs = statement.executeQuery("SELECT count(*) FROM group_role_rel")) {
      while (rs.next()) {
        count = rs.getInt(1);
      }
    } catch (SQLException e) {
      throw new RuntimeException("SQL execution failed", e);
    }
    return count;
  }

  private RoleEntity createRoleEntity(
      Long id,
      Namespace namespace,
      String name,
      AuditInfo auditInfo,
      SecurableObject securableObject,
      Map<String, String> properties) {

    return createRoleEntity(
        id, namespace, name, auditInfo, Lists.newArrayList(securableObject), properties);
  }

  private Integer countRoleRels(Long roleId) {
    int count = 0;
    try (SqlSession sqlSession =
            SqlSessionFactoryHelper.getInstance().getSqlSessionFactory().openSession(true);
        Connection connection = sqlSession.getConnection();
        Statement statement1 = connection.createStatement();
        ResultSet rs1 =
            statement1.executeQuery(
                String.format("SELECT count(*) FROM user_role_rel WHERE role_id = %d", roleId));
        Statement statement2 = connection.createStatement();
        ResultSet rs2 =
            statement2.executeQuery(
                String.format("SELECT count(*) FROM group_role_rel WHERE role_id = %d", roleId))) {
      while (rs1.next()) {
        count += rs1.getInt(1);
      }
      while (rs2.next()) {
        count += rs2.getInt(1);
      }
    } catch (SQLException e) {
      throw new RuntimeException("SQL execution failed", e);
    }
    return count;
  }
}
