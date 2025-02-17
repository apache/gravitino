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
import java.util.function.Function;
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
import org.apache.gravitino.meta.SchemaEntity;
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
import org.apache.ibatis.session.SqlSession;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class TestRoleMetaService extends TestJDBCBackend {

  String metalakeName = "metalake";

  @Test
  void getRoleByIdentifier() throws IOException {
    AuditInfo auditInfo =
        AuditInfo.builder().withCreator("creator").withCreateTime(Instant.now()).build();

    BaseMetalake metalake =
        createBaseMakeLake(RandomIdGenerator.INSTANCE.nextId(), metalakeName, auditInfo);
    backend.insert(metalake, false);
    CatalogEntity catalog =
        createCatalog(
            RandomIdGenerator.INSTANCE.nextId(), Namespace.of("metalake"), "catalog", auditInfo);
    backend.insert(catalog, false);

    CatalogEntity anotherCatalog =
        createCatalog(
            RandomIdGenerator.INSTANCE.nextId(),
            Namespace.of("metalake"),
            "anotherCatalog",
            auditInfo);
    backend.insert(anotherCatalog, false);

    RoleMetaService roleMetaService = RoleMetaService.getInstance();

    // get not exist role
    Assertions.assertThrows(
        NoSuchEntityException.class,
        () ->
            roleMetaService.getRoleByIdentifier(AuthorizationUtils.ofRole(metalakeName, "role1")));

    // get role
    RoleEntity role1 =
        createRoleEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            AuthorizationUtils.ofRoleNamespace(metalakeName),
            "role1",
            auditInfo,
            SecurableObjects.ofCatalog(
                "catalog", Lists.newArrayList(Privileges.UseCatalog.allow())),
            ImmutableMap.of("k1", "v1"));

    roleMetaService.insertRole(role1, false);
    Assertions.assertEquals(role1, roleMetaService.getRoleByIdentifier(role1.nameIdentifier()));
  }

  @Test
  void testListRoles() throws IOException {
    AuditInfo auditInfo =
        AuditInfo.builder().withCreator("creator").withCreateTime(Instant.now()).build();
    BaseMetalake metalake =
        createBaseMakeLake(RandomIdGenerator.INSTANCE.nextId(), metalakeName, auditInfo);
    backend.insert(metalake, false);

    CatalogEntity catalog =
        createCatalog(
            RandomIdGenerator.INSTANCE.nextId(), Namespace.of("metalake"), "catalog", auditInfo);
    backend.insert(catalog, false);

    RoleEntity role1 =
        createRoleEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            AuthorizationUtils.ofRoleNamespace(metalakeName),
            "role1",
            auditInfo,
            SecurableObjects.ofCatalog(
                "catalog", Lists.newArrayList(Privileges.UseCatalog.allow())),
            ImmutableMap.of("k1", "v1"));

    RoleEntity role2 =
        createRoleEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            AuthorizationUtils.ofRoleNamespace(metalakeName),
            "role2",
            auditInfo,
            SecurableObjects.ofCatalog(
                "catalog", Lists.newArrayList(Privileges.UseCatalog.allow())),
            ImmutableMap.of("k1", "v1"));

    backend.insert(role1, false);
    backend.insert(role2, false);

    RoleMetaService roleMetaService = RoleMetaService.getInstance();
    List<RoleEntity> actualRoles =
        roleMetaService.listRolesByNamespace(AuthorizationUtils.ofRoleNamespace(metalakeName));
    actualRoles.sort(Comparator.comparing(RoleEntity::name));
    List<RoleEntity> expectRoles = Lists.newArrayList(role1, role2);
    Assertions.assertEquals(expectRoles.size(), actualRoles.size());
    for (int index = 0; index < expectRoles.size(); index++) {
      RoleEntity expectRole = expectRoles.get(index);
      RoleEntity actualRole = actualRoles.get(index);
      Assertions.assertEquals(expectRole.name(), actualRole.name());
    }
  }

  @Test
  void insertRole() throws IOException {
    AuditInfo auditInfo =
        AuditInfo.builder().withCreator("creator").withCreateTime(Instant.now()).build();
    BaseMetalake metalake =
        createBaseMakeLake(RandomIdGenerator.INSTANCE.nextId(), metalakeName, auditInfo);
    backend.insert(metalake, false);
    CatalogEntity catalog =
        createCatalog(
            RandomIdGenerator.INSTANCE.nextId(), Namespace.of("metalake"), "catalog", auditInfo);
    backend.insert(catalog, false);
    CatalogEntity anotherCatalog =
        createCatalog(
            RandomIdGenerator.INSTANCE.nextId(),
            Namespace.of("metalake"),
            "anotherCatalog",
            auditInfo);
    backend.insert(anotherCatalog, false);
    CatalogEntity overwriteCatalog =
        createCatalog(
            RandomIdGenerator.INSTANCE.nextId(),
            Namespace.of("metalake"),
            "catalogOverwrite",
            auditInfo);
    backend.insert(overwriteCatalog, false);

    SchemaEntity schema =
        createSchemaEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            Namespace.of("metalake", "catalog"),
            "schema",
            auditInfo);
    backend.insert(schema, false);

    TopicEntity topic =
        createTopicEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            Namespace.of("metalake", "catalog", "schema"),
            "topic",
            auditInfo);
    backend.insert(topic, false);

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

    RoleMetaService roleMetaService = RoleMetaService.getInstance();

    // Test with different securable objects
    SecurableObject metalakeObject =
        SecurableObjects.ofMetalake(
            "metalake", Lists.newArrayList(Privileges.CreateCatalog.allow()));
    SecurableObject catalogObject =
        SecurableObjects.ofCatalog(
            "catalog",
            Lists.newArrayList(Privileges.UseCatalog.allow(), Privileges.CreateSchema.deny()));
    SecurableObject schemaObject =
        SecurableObjects.ofSchema(
            catalogObject,
            "schema",
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
                "anotherCatalog", Lists.newArrayList(Privileges.UseCatalog.allow())));

    securableObjects.sort(Comparator.comparing(SecurableObject::fullName));

    // insert role
    RoleEntity role1 =
        createRoleEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            AuthorizationUtils.ofRoleNamespace(metalakeName),
            "role1",
            auditInfo,
            securableObjects,
            ImmutableMap.of("k1", "v1"));
    Assertions.assertThrows(
        NoSuchEntityException.class,
        () -> roleMetaService.getRoleByIdentifier(role1.nameIdentifier()));
    Assertions.assertDoesNotThrow(() -> roleMetaService.insertRole(role1, false));
    Assertions.assertEquals(role1, roleMetaService.getRoleByIdentifier(role1.nameIdentifier()));

    // insert duplicate role
    RoleEntity role1Exist =
        createRoleEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            AuthorizationUtils.ofRoleNamespace(metalakeName),
            "role1",
            auditInfo,
            SecurableObjects.ofCatalog(
                "catalog", Lists.newArrayList(Privileges.UseCatalog.allow())),
            ImmutableMap.of("k1", "v1"));
    Assertions.assertThrows(
        EntityAlreadyExistsException.class, () -> roleMetaService.insertRole(role1Exist, false));

    // insert overwrite
    RoleEntity role1Overwrite =
        createRoleEntity(
            role1.id(),
            AuthorizationUtils.ofRoleNamespace(metalakeName),
            "role1Overwrite",
            auditInfo,
            SecurableObjects.ofCatalog(
                "catalogOverwrite", Lists.newArrayList(Privileges.UseCatalog.allow())),
            ImmutableMap.of("k2", "v2"));
    Assertions.assertDoesNotThrow(() -> roleMetaService.insertRole(role1Overwrite, true));
    Assertions.assertEquals(
        "role1Overwrite",
        roleMetaService.getRoleByIdentifier(role1Overwrite.nameIdentifier()).name());
    Assertions.assertEquals(
        role1Overwrite, roleMetaService.getRoleByIdentifier(role1Overwrite.nameIdentifier()));
  }

  @Test
  void deleteRole() throws IOException {
    AuditInfo auditInfo =
        AuditInfo.builder().withCreator("creator").withCreateTime(Instant.now()).build();
    BaseMetalake metalake =
        createBaseMakeLake(RandomIdGenerator.INSTANCE.nextId(), metalakeName, auditInfo);
    backend.insert(metalake, false);
    CatalogEntity catalog =
        createCatalog(
            RandomIdGenerator.INSTANCE.nextId(), Namespace.of("metalake"), "catalog", auditInfo);
    backend.insert(catalog, false);

    UserMetaService userMetaService = UserMetaService.getInstance();
    GroupMetaService groupMetaService = GroupMetaService.getInstance();
    RoleMetaService roleMetaService = RoleMetaService.getInstance();

    // delete not exist role
    Assertions.assertThrows(
        NoSuchEntityException.class,
        () -> roleMetaService.deleteRole(AuthorizationUtils.ofRole(metalakeName, "role1")));

    // delete role
    RoleEntity role1 =
        createRoleEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            AuthorizationUtils.ofRoleNamespace(metalakeName),
            "role1",
            auditInfo,
            SecurableObjects.ofCatalog(
                "catalog", Lists.newArrayList(Privileges.UseCatalog.allow())),
            ImmutableMap.of("k1", "v1"));
    Assertions.assertThrows(
        NoSuchEntityException.class,
        () -> roleMetaService.getRoleByIdentifier(role1.nameIdentifier()));
    Assertions.assertDoesNotThrow(() -> roleMetaService.insertRole(role1, false));
    Assertions.assertEquals(role1, roleMetaService.getRoleByIdentifier(role1.nameIdentifier()));
    Assertions.assertTrue(roleMetaService.deleteRole(role1.nameIdentifier()));
    Assertions.assertThrows(
        NoSuchEntityException.class,
        () -> roleMetaService.getRoleByIdentifier(role1.nameIdentifier()));

    // delete user & groups when delete role
    RoleEntity role2 =
        createRoleEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            AuthorizationUtils.ofRoleNamespace(metalakeName),
            "role2",
            auditInfo,
            SecurableObjects.ofCatalog(
                "catalog", Lists.newArrayList(Privileges.UseCatalog.allow())),
            ImmutableMap.of("k1", "v1"));
    roleMetaService.insertRole(role2, false);
    GroupEntity group1 =
        createGroupEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            AuthorizationUtils.ofGroupNamespace(metalakeName),
            "group1",
            auditInfo,
            Lists.newArrayList(role2.name()),
            Lists.newArrayList(role2.id()));
    GroupEntity group2 =
        createGroupEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            AuthorizationUtils.ofGroupNamespace(metalakeName),
            "group2",
            auditInfo,
            Lists.newArrayList(role2.name()),
            Lists.newArrayList(role2.id()));
    groupMetaService.insertGroup(group1, false);
    groupMetaService.insertGroup(group2, false);
    Assertions.assertEquals(
        group1.name(), groupMetaService.getGroupByIdentifier(group1.nameIdentifier()).name());
    Assertions.assertEquals(
        group1.roleNames(),
        groupMetaService.getGroupByIdentifier(group1.nameIdentifier()).roleNames());
    Assertions.assertEquals(
        group2.name(), groupMetaService.getGroupByIdentifier(group2.nameIdentifier()).name());
    Assertions.assertEquals(
        group2.roleNames(),
        groupMetaService.getGroupByIdentifier(group2.nameIdentifier()).roleNames());
    UserEntity user1 =
        createUserEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            AuthorizationUtils.ofUserNamespace(metalakeName),
            "user1",
            auditInfo,
            Lists.newArrayList(role2.name()),
            Lists.newArrayList(role2.id()));
    userMetaService.insertUser(user1, false);
    Assertions.assertEquals(
        user1.name(), userMetaService.getUserByIdentifier(user1.nameIdentifier()).name());
    Assertions.assertEquals(
        user1.roleNames(), userMetaService.getUserByIdentifier(user1.nameIdentifier()).roleNames());

    Assertions.assertTrue(roleMetaService.deleteRole(role2.nameIdentifier()));
    Assertions.assertThrows(
        NoSuchEntityException.class,
        () -> roleMetaService.getRoleByIdentifier(role2.nameIdentifier()));

    List<UserPO> userRoleRels =
        SessionUtils.doWithCommitAndFetchResult(
            UserMetaMapper.class, mapper -> mapper.listUsersByRoleId(role2.id()));
    Assertions.assertEquals(
        user1.name(), userMetaService.getUserByIdentifier(user1.nameIdentifier()).name());
    Assertions.assertTrue(userRoleRels.isEmpty());

    List<GroupPO> groupRoleRels =
        SessionUtils.doWithCommitAndFetchResult(
            GroupMetaMapper.class, mapper -> mapper.listGroupsByRoleId(role2.id()));
    Assertions.assertEquals(
        group1.name(), groupMetaService.getGroupByIdentifier(group1.nameIdentifier()).name());
    Assertions.assertTrue(groupRoleRels.isEmpty());
  }

  @Test
  void listRolesBySecurableObject() throws IOException {
    AuditInfo auditInfo =
        AuditInfo.builder().withCreator("creator").withCreateTime(Instant.now()).build();
    BaseMetalake metalake =
        createBaseMakeLake(RandomIdGenerator.INSTANCE.nextId(), metalakeName, auditInfo);
    backend.insert(metalake, false);

    CatalogEntity catalog =
        createCatalog(
            RandomIdGenerator.INSTANCE.nextId(), Namespace.of("metalake"), "catalog", auditInfo);
    backend.insert(catalog, false);

    RoleEntity role1 =
        createRoleEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            AuthorizationUtils.ofRoleNamespace(metalakeName),
            "role1",
            auditInfo,
            "catalog");

    RoleEntity role2 =
        createRoleEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            AuthorizationUtils.ofRoleNamespace(metalakeName),
            "role2",
            auditInfo,
            "catalog");

    RoleMetaService roleMetaService = RoleMetaService.getInstance();
    roleMetaService.insertRole(role1, false);
    roleMetaService.insertRole(role2, false);

    List<RoleEntity> roleEntities =
        roleMetaService.listRolesByMetadataObject(catalog.nameIdentifier(), catalog.type(), true);
    roleEntities.sort(Comparator.comparing(RoleEntity::name));
    Assertions.assertEquals(Lists.newArrayList(role1, role2), roleEntities);
  }

  @Test
  void deleteMetalake() throws IOException {
    AuditInfo auditInfo =
        AuditInfo.builder().withCreator("creator").withCreateTime(Instant.now()).build();
    BaseMetalake metalake =
        createBaseMakeLake(RandomIdGenerator.INSTANCE.nextId(), metalakeName, auditInfo);
    backend.insert(metalake, false);

    CatalogEntity catalog =
        createCatalog(
            RandomIdGenerator.INSTANCE.nextId(), Namespace.of("metalake"), "catalog", auditInfo);
    backend.insert(catalog, false);

    UserMetaService userMetaService = UserMetaService.getInstance();
    GroupMetaService groupMetaService = GroupMetaService.getInstance();
    RoleMetaService roleMetaService = RoleMetaService.getInstance();

    RoleEntity role1 =
        createRoleEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            AuthorizationUtils.ofRoleNamespace(metalakeName),
            "role1",
            auditInfo,
            "catalog");
    RoleEntity role2 =
        createRoleEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            AuthorizationUtils.ofRoleNamespace(metalakeName),
            "role2",
            auditInfo,
            "catalog");
    GroupEntity group1 =
        createGroupEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            AuthorizationUtils.ofGroupNamespace(metalakeName),
            "group1",
            auditInfo,
            Lists.newArrayList(role1.name(), role2.name()),
            Lists.newArrayList(role1.id(), role2.id()));
    UserEntity user1 =
        createUserEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            AuthorizationUtils.ofUserNamespace(metalakeName),
            "user1",
            auditInfo,
            Lists.newArrayList(role1.name(), role2.name()),
            Lists.newArrayList(role1.id(), role2.id()));
    roleMetaService.insertRole(role1, false);
    roleMetaService.insertRole(role2, false);
    groupMetaService.insertGroup(group1, false);
    userMetaService.insertUser(user1, false);

    Assertions.assertEquals(role1, roleMetaService.getRoleByIdentifier(role1.nameIdentifier()));
    Assertions.assertEquals(role2, roleMetaService.getRoleByIdentifier(role2.nameIdentifier()));
    Assertions.assertEquals(
        group1.name(), groupMetaService.getGroupByIdentifier(group1.nameIdentifier()).name());
    Assertions.assertEquals(
        user1.name(), userMetaService.getUserByIdentifier(user1.nameIdentifier()).name());

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
    Assertions.assertEquals(1, userRole1Rels.size());
    Assertions.assertEquals(1, userRole2Rels.size());
    Assertions.assertEquals(1, groupRole1Rels.size());
    Assertions.assertEquals(1, groupRole2Rels.size());

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

  @Test
  void testUpdateRole() throws IOException {
    AuditInfo auditInfo =
        AuditInfo.builder().withCreator("creator").withCreateTime(Instant.now()).build();
    BaseMetalake metalake =
        createBaseMakeLake(RandomIdGenerator.INSTANCE.nextId(), metalakeName, auditInfo);
    backend.insert(metalake, false);

    CatalogEntity catalog =
        createCatalog(
            RandomIdGenerator.INSTANCE.nextId(), Namespace.of("metalake"), "catalog", auditInfo);
    backend.insert(catalog, false);

    RoleMetaService roleMetaService = RoleMetaService.getInstance();
    RoleEntity roleEntity =
        createRoleEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            AuthorizationUtils.ofRoleNamespace(metalakeName),
            "role1",
            auditInfo,
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
                  metalakeName, Lists.newArrayList(Privileges.CreateTable.allow())));

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

    Assertions.assertEquals(grantRole.id(), roleEntity.id());
    Assertions.assertEquals(grantRole.name(), roleEntity.name());
    Assertions.assertEquals("creator", grantRole.auditInfo().creator());
    Assertions.assertEquals("grantRole", grantRole.auditInfo().lastModifier());
    Assertions.assertEquals(
        Lists.newArrayList(
            SecurableObjects.ofCatalog(
                "catalog", Lists.newArrayList(Privileges.UseCatalog.allow())),
            SecurableObjects.ofMetalake(
                metalakeName, Lists.newArrayList(Privileges.CreateTable.allow()))),
        grantRole.securableObjects());

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
          securableObjects.remove(0);

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
    Assertions.assertEquals(revokeRole.id(), roleEntity.id());
    Assertions.assertEquals(revokeRole.name(), roleEntity.name());
    Assertions.assertEquals("creator", revokeRole.auditInfo().creator());
    Assertions.assertEquals("revokeRole", revokeRole.auditInfo().lastModifier());
    Assertions.assertEquals(
        Lists.newArrayList(
            SecurableObjects.ofMetalake(
                metalakeName, Lists.newArrayList(Privileges.CreateTable.allow()))),
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
    Assertions.assertEquals(grantRevokeRole.id(), roleEntity.id());
    Assertions.assertEquals(grantRevokeRole.name(), roleEntity.name());
    Assertions.assertEquals("creator", grantRevokeRole.auditInfo().creator());
    Assertions.assertEquals("grantRevokeRole", grantRevokeRole.auditInfo().lastModifier());
    Assertions.assertEquals(
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
    Assertions.assertEquals(revokeMultipleRole.id(), roleEntity.id());
    Assertions.assertEquals(revokeMultipleRole.name(), roleEntity.name());
    Assertions.assertEquals("creator", revokeMultipleRole.auditInfo().creator());
    Assertions.assertEquals("revokeMultiple", revokeMultipleRole.auditInfo().lastModifier());
    Assertions.assertTrue(revokeMultipleRole.securableObjects().isEmpty());
  }

  @Test
  void deleteMetalakeCascade() throws IOException {
    AuditInfo auditInfo =
        AuditInfo.builder().withCreator("creator").withCreateTime(Instant.now()).build();
    BaseMetalake metalake =
        createBaseMakeLake(RandomIdGenerator.INSTANCE.nextId(), metalakeName, auditInfo);
    backend.insert(metalake, false);
    CatalogEntity catalog =
        createCatalog(
            RandomIdGenerator.INSTANCE.nextId(), Namespace.of("metalake"), "catalog", auditInfo);
    backend.insert(catalog, false);

    UserMetaService userMetaService = UserMetaService.getInstance();
    GroupMetaService groupMetaService = GroupMetaService.getInstance();
    RoleMetaService roleMetaService = RoleMetaService.getInstance();

    RoleEntity role1 =
        createRoleEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            AuthorizationUtils.ofRoleNamespace(metalakeName),
            "role1",
            auditInfo,
            "catalog");
    RoleEntity role2 =
        createRoleEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            AuthorizationUtils.ofRoleNamespace(metalakeName),
            "role2",
            auditInfo,
            "catalog");
    GroupEntity group1 =
        createGroupEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            AuthorizationUtils.ofGroupNamespace(metalakeName),
            "group1",
            auditInfo,
            Lists.newArrayList(role1.name(), role2.name()),
            Lists.newArrayList(role1.id(), role2.id()));
    UserEntity user1 =
        createUserEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            AuthorizationUtils.ofUserNamespace(metalakeName),
            "user1",
            auditInfo,
            Lists.newArrayList(role1.name(), role2.name()),
            Lists.newArrayList(role1.id(), role2.id()));
    roleMetaService.insertRole(role1, false);
    roleMetaService.insertRole(role2, false);
    groupMetaService.insertGroup(group1, false);
    userMetaService.insertUser(user1, false);

    Assertions.assertEquals(role1, roleMetaService.getRoleByIdentifier(role1.nameIdentifier()));
    Assertions.assertEquals(role2, roleMetaService.getRoleByIdentifier(role2.nameIdentifier()));
    Assertions.assertEquals(
        group1.name(), groupMetaService.getGroupByIdentifier(group1.nameIdentifier()).name());
    Assertions.assertEquals(
        user1.name(), userMetaService.getUserByIdentifier(user1.nameIdentifier()).name());

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
    Assertions.assertEquals(1, userRole1Rels.size());
    Assertions.assertEquals(1, userRole2Rels.size());
    Assertions.assertEquals(1, groupRole1Rels.size());
    Assertions.assertEquals(1, groupRole2Rels.size());

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

  @Test
  void deleteRoleMetasByLegacyTimeline() throws IOException {
    AuditInfo auditInfo =
        AuditInfo.builder().withCreator("creator").withCreateTime(Instant.now()).build();
    BaseMetalake metalake =
        createBaseMakeLake(RandomIdGenerator.INSTANCE.nextId(), metalakeName, auditInfo);
    backend.insert(metalake, false);
    CatalogEntity catalog =
        createCatalog(
            RandomIdGenerator.INSTANCE.nextId(), Namespace.of("metalake"), "catalog", auditInfo);
    backend.insert(catalog, false);

    UserMetaService userMetaService = UserMetaService.getInstance();
    GroupMetaService groupMetaService = GroupMetaService.getInstance();
    RoleMetaService roleMetaService = RoleMetaService.getInstance();

    RoleEntity role1 =
        createRoleEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            AuthorizationUtils.ofRoleNamespace(metalakeName),
            "role1",
            auditInfo,
            "catalog");
    RoleEntity role2 =
        createRoleEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            AuthorizationUtils.ofRoleNamespace(metalakeName),
            "role2",
            auditInfo,
            "catalog");
    roleMetaService.insertRole(role1, false);
    roleMetaService.insertRole(role2, false);

    UserEntity user1 =
        createUserEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            AuthorizationUtils.ofUserNamespace(metalakeName),
            "user1",
            auditInfo,
            Lists.newArrayList(role1.name(), role2.name()),
            Lists.newArrayList(role1.id(), role2.id()));
    userMetaService.insertUser(user1, false);

    GroupEntity group1 =
        createGroupEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            AuthorizationUtils.ofUserNamespace(metalakeName),
            "group1",
            auditInfo,
            Lists.newArrayList(role1.name(), role2.name()),
            Lists.newArrayList(role1.id(), role2.id()));
    groupMetaService.insertGroup(group1, false);

    // hard delete before soft delete
    int deletedCount =
        roleMetaService.deleteRoleMetasByLegacyTimeline(Instant.now().toEpochMilli() + 1000, 4);
    Assertions.assertEquals(0, deletedCount);

    Assertions.assertEquals(role1, roleMetaService.getRoleByIdentifier(role1.nameIdentifier()));
    Assertions.assertEquals(role2, roleMetaService.getRoleByIdentifier(role2.nameIdentifier()));
    Assertions.assertEquals(
        group1.name(), groupMetaService.getGroupByIdentifier(group1.nameIdentifier()).name());
    Assertions.assertEquals(
        user1.name(), userMetaService.getUserByIdentifier(user1.nameIdentifier()).name());
    Assertions.assertEquals(2, roleMetaService.listRolesByUserId(user1.id()).size());
    Assertions.assertEquals(2, roleMetaService.listRolesByGroupId(group1.id()).size());
    Assertions.assertEquals(2, countRoles(metalake.id()));
    Assertions.assertEquals(2, countUserRoleRels());
    Assertions.assertEquals(2, countGroupRoleRels());

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
    Assertions.assertEquals(0, roleMetaService.listRolesByGroupId(user1.id()).size());
    Assertions.assertEquals(0, roleMetaService.listRolesByGroupId(group1.id()).size());
    Assertions.assertEquals(2, countRoles(metalake.id()));
    Assertions.assertEquals(2, countUserRoleRels());
    Assertions.assertEquals(2, countGroupRoleRels());

    // hard delete after soft delete
    deletedCount =
        roleMetaService.deleteRoleMetasByLegacyTimeline(Instant.now().toEpochMilli() + 1000, 1);
    Assertions.assertEquals(3, deletedCount); // delete 1 role + 1 userRoleRel + 1 groupRoleRel
    Assertions.assertEquals(1, countRoles(metalake.id())); // 2 - 1
    Assertions.assertEquals(1, countUserRoleRels()); // 2 - 1
    Assertions.assertEquals(1, countGroupRoleRels()); // 2 - 1

    deletedCount =
        roleMetaService.deleteRoleMetasByLegacyTimeline(Instant.now().toEpochMilli() + 1000, 1);
    Assertions.assertEquals(3, deletedCount);
    Assertions.assertEquals(0, countRoles(metalake.id()));
    Assertions.assertEquals(0, countUserRoleRels());
    Assertions.assertEquals(0, countGroupRoleRels());

    deletedCount =
        roleMetaService.deleteRoleMetasByLegacyTimeline(Instant.now().toEpochMilli() + 1000, 1);
    Assertions.assertEquals(0, deletedCount); // no more to delete
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
}
