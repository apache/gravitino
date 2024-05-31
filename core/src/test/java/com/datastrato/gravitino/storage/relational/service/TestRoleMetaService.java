/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.storage.relational.service;

import com.datastrato.gravitino.Namespace;
import com.datastrato.gravitino.authorization.AuthorizationUtils;
import com.datastrato.gravitino.authorization.Privileges;
import com.datastrato.gravitino.authorization.SecurableObjects;
import com.datastrato.gravitino.exceptions.AlreadyExistsException;
import com.datastrato.gravitino.exceptions.NoSuchEntityException;
import com.datastrato.gravitino.meta.AuditInfo;
import com.datastrato.gravitino.meta.BaseMetalake;
import com.datastrato.gravitino.meta.CatalogEntity;
import com.datastrato.gravitino.meta.GroupEntity;
import com.datastrato.gravitino.meta.RoleEntity;
import com.datastrato.gravitino.meta.UserEntity;
import com.datastrato.gravitino.storage.RandomIdGenerator;
import com.datastrato.gravitino.storage.relational.TestJDBCBackend;
import com.datastrato.gravitino.storage.relational.mapper.GroupMetaMapper;
import com.datastrato.gravitino.storage.relational.mapper.UserMetaMapper;
import com.datastrato.gravitino.storage.relational.po.GroupPO;
import com.datastrato.gravitino.storage.relational.po.UserPO;
import com.datastrato.gravitino.storage.relational.session.SqlSessionFactoryHelper;
import com.datastrato.gravitino.storage.relational.utils.SessionUtils;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Instant;
import java.util.List;
import org.apache.ibatis.session.SqlSession;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class TestRoleMetaService extends TestJDBCBackend {

  String metalakeName = "metalake";

  @Test
  void getRoleByIdentifier() {
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
  void insertRole() {
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

    RoleMetaService roleMetaService = RoleMetaService.getInstance();

    // insert role
    RoleEntity role1 =
        createRoleEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            AuthorizationUtils.ofRoleNamespace(metalakeName),
            "role1",
            auditInfo,
            Lists.newArrayList(
                SecurableObjects.ofCatalog(
                    "catalog",
                    Lists.newArrayList(
                        Privileges.UseCatalog.allow(), Privileges.DropCatalog.deny())),
                SecurableObjects.ofCatalog(
                    "anotherCatalog", Lists.newArrayList(Privileges.UseCatalog.allow()))),
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
        AlreadyExistsException.class, () -> roleMetaService.insertRole(role1Exist, false));

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
  void deleteRole() {
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
  void deleteMetalake() {
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
  void deleteMetalakeCascade() {
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
  void deleteRoleMetasByLegacyTimeLine() {
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
        roleMetaService.deleteRoleMetasByLegacyTimeLine(Instant.now().toEpochMilli() + 1000, 4);
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
        roleMetaService.deleteRoleMetasByLegacyTimeLine(Instant.now().toEpochMilli() + 1000, 1);
    Assertions.assertEquals(3, deletedCount); // delete 1 role + 1 userRoleRel + 1 groupRoleRel
    Assertions.assertEquals(1, countRoles(metalake.id())); // 2 - 1
    Assertions.assertEquals(1, countUserRoleRels()); // 2 - 1
    Assertions.assertEquals(1, countGroupRoleRels()); // 2 - 1

    deletedCount =
        roleMetaService.deleteRoleMetasByLegacyTimeLine(Instant.now().toEpochMilli() + 1000, 1);
    Assertions.assertEquals(3, deletedCount);
    Assertions.assertEquals(0, countRoles(metalake.id()));
    Assertions.assertEquals(0, countUserRoleRels());
    Assertions.assertEquals(0, countGroupRoleRels());

    deletedCount =
        roleMetaService.deleteRoleMetasByLegacyTimeLine(Instant.now().toEpochMilli() + 1000, 1);
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
