/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.storage.relational.service;

import com.datastrato.gravitino.authorization.AuthorizationUtils;
import com.datastrato.gravitino.exceptions.AlreadyExistsException;
import com.datastrato.gravitino.exceptions.NoSuchEntityException;
import com.datastrato.gravitino.meta.AuditInfo;
import com.datastrato.gravitino.meta.BaseMetalake;
import com.datastrato.gravitino.meta.RoleEntity;
import com.datastrato.gravitino.meta.UserEntity;
import com.datastrato.gravitino.storage.RandomIdGenerator;
import com.datastrato.gravitino.storage.relational.TestJDBCBackend;
import com.datastrato.gravitino.storage.relational.mapper.RoleMetaMapper;
import com.datastrato.gravitino.storage.relational.po.RolePO;
import com.datastrato.gravitino.storage.relational.session.SqlSessionFactoryHelper;
import com.datastrato.gravitino.storage.relational.utils.SessionUtils;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Instant;
import java.util.List;
import java.util.function.Function;
import org.apache.ibatis.session.SqlSession;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class TestUserMetaService extends TestJDBCBackend {

  String metalakeName = "metalake";

  @Test
  void getUserByIdentifier() {
    AuditInfo auditInfo =
        AuditInfo.builder().withCreator("creator").withCreateTime(Instant.now()).build();

    BaseMetalake metalake =
        createBaseMakeLake(RandomIdGenerator.INSTANCE.nextId(), metalakeName, auditInfo);
    backend.insert(metalake, false);

    UserMetaService userMetaService = UserMetaService.getInstance();
    RoleMetaService roleMetaService = RoleMetaService.getInstance();

    // get not exist user
    Assertions.assertThrows(
        NoSuchEntityException.class,
        () ->
            userMetaService.getUserByIdentifier(AuthorizationUtils.ofUser(metalakeName, "user1")));

    // get user
    UserEntity user1 =
        createUserEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            AuthorizationUtils.ofUserNamespace(metalakeName),
            "user1",
            auditInfo);
    userMetaService.insertUser(user1, false);
    Assertions.assertEquals(user1, userMetaService.getUserByIdentifier(user1.nameIdentifier()));

    // get user with roles
    RoleEntity role1 =
        createRoleEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            AuthorizationUtils.ofRoleNamespace(metalakeName),
            "role1",
            auditInfo);
    RoleEntity role2 =
        createRoleEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            AuthorizationUtils.ofRoleNamespace(metalakeName),
            "role2",
            auditInfo);
    roleMetaService.insertRole(role1, false);
    roleMetaService.insertRole(role2, false);
    UserEntity user2 =
        createUserEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            AuthorizationUtils.ofUserNamespace(metalakeName),
            "user2",
            auditInfo,
            Lists.newArrayList(role1.name(), role2.name()),
            Lists.newArrayList(role1.id(), role2.id()));
    userMetaService.insertUser(user2, false);
    UserEntity actualUser = userMetaService.getUserByIdentifier(user2.nameIdentifier());
    Assertions.assertEquals(user2.name(), actualUser.name());
    Assertions.assertEquals(
        Sets.newHashSet(user2.roleNames()), Sets.newHashSet(actualUser.roleNames()));
  }

  @Test
  void insertUser() {
    AuditInfo auditInfo =
        AuditInfo.builder().withCreator("creator").withCreateTime(Instant.now()).build();
    BaseMetalake metalake =
        createBaseMakeLake(RandomIdGenerator.INSTANCE.nextId(), metalakeName, auditInfo);
    backend.insert(metalake, false);

    UserMetaService userMetaService = UserMetaService.getInstance();
    RoleMetaService roleMetaService = RoleMetaService.getInstance();

    // insert user
    UserEntity user1 =
        createUserEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            AuthorizationUtils.ofUserNamespace(metalakeName),
            "user1",
            auditInfo);
    Assertions.assertThrows(
        NoSuchEntityException.class,
        () -> userMetaService.getUserByIdentifier(user1.nameIdentifier()));
    Assertions.assertDoesNotThrow(() -> userMetaService.insertUser(user1, false));
    Assertions.assertEquals(user1, userMetaService.getUserByIdentifier(user1.nameIdentifier()));

    // insert duplicate user
    UserEntity user1Exist =
        createUserEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            AuthorizationUtils.ofUserNamespace(metalakeName),
            "user1",
            auditInfo);
    Assertions.assertThrows(
        AlreadyExistsException.class, () -> userMetaService.insertUser(user1Exist, false));

    // insert overwrite
    UserEntity user1Overwrite =
        createUserEntity(
            user1.id(),
            AuthorizationUtils.ofUserNamespace(metalakeName),
            "user1Overwrite",
            auditInfo);
    Assertions.assertDoesNotThrow(() -> userMetaService.insertUser(user1Overwrite, true));
    Assertions.assertEquals(
        "user1Overwrite",
        userMetaService.getUserByIdentifier(user1Overwrite.nameIdentifier()).name());
    Assertions.assertEquals(
        user1Overwrite, userMetaService.getUserByIdentifier(user1Overwrite.nameIdentifier()));

    // insert user with roles
    RoleEntity role1 =
        createRoleEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            AuthorizationUtils.ofRoleNamespace(metalakeName),
            "role1",
            auditInfo);
    RoleEntity role2 =
        createRoleEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            AuthorizationUtils.ofRoleNamespace(metalakeName),
            "role2",
            auditInfo);
    roleMetaService.insertRole(role1, false);
    roleMetaService.insertRole(role2, false);
    UserEntity user2 =
        createUserEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            AuthorizationUtils.ofUserNamespace(metalakeName),
            "user2",
            auditInfo,
            Lists.newArrayList(role1.name(), role2.name()),
            Lists.newArrayList(role1.id(), role2.id()));
    Assertions.assertDoesNotThrow(() -> userMetaService.insertUser(user2, false));
    UserEntity actualUser = userMetaService.getUserByIdentifier(user2.nameIdentifier());
    Assertions.assertEquals(user2.name(), actualUser.name());
    Assertions.assertEquals(
        Sets.newHashSet(user2.roleNames()), Sets.newHashSet(actualUser.roleNames()));

    // insert duplicate user with roles
    UserEntity user2Exist =
        createUserEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            AuthorizationUtils.ofUserNamespace(metalakeName),
            "user2",
            auditInfo);
    Assertions.assertThrows(
        AlreadyExistsException.class, () -> userMetaService.insertUser(user2Exist, false));

    // insert overwrite user with 2 roles
    UserEntity user2Overwrite =
        createUserEntity(
            user1.id(),
            AuthorizationUtils.ofUserNamespace(metalakeName),
            "user2Overwrite",
            auditInfo,
            Lists.newArrayList(role1.name(), role2.name()),
            Lists.newArrayList(role1.id(), role2.id()));
    Assertions.assertDoesNotThrow(() -> userMetaService.insertUser(user2Overwrite, true));

    UserEntity actualOverwriteUser2 =
        userMetaService.getUserByIdentifier(user2Overwrite.nameIdentifier());
    Assertions.assertEquals("user2Overwrite", actualOverwriteUser2.name());
    Assertions.assertEquals(2, actualOverwriteUser2.roleNames().size());
    Assertions.assertEquals(
        Sets.newHashSet(role1.name(), role2.name()),
        Sets.newHashSet(actualOverwriteUser2.roleNames()));

    // insert overwrite user with 1 roles
    RoleEntity role3 =
        createRoleEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            AuthorizationUtils.ofRoleNamespace(metalakeName),
            "role3",
            auditInfo);
    roleMetaService.insertRole(role3, false);
    UserEntity user3Overwrite =
        createUserEntity(
            user1.id(),
            AuthorizationUtils.ofUserNamespace(metalakeName),
            "user3Overwrite",
            auditInfo,
            Lists.newArrayList(role3.name()),
            Lists.newArrayList(role3.id()));
    Assertions.assertDoesNotThrow(() -> userMetaService.insertUser(user3Overwrite, true));

    UserEntity actualOverwriteUser3 =
        userMetaService.getUserByIdentifier(user3Overwrite.nameIdentifier());
    Assertions.assertEquals("user3Overwrite", actualOverwriteUser3.name());
    Assertions.assertEquals(1, actualOverwriteUser3.roleNames().size());
    Assertions.assertEquals("role3", actualOverwriteUser3.roleNames().get(0));

    // insert overwrite user with 0 roles
    UserEntity user4Overwrite =
        createUserEntity(
            user1.id(),
            AuthorizationUtils.ofUserNamespace(metalakeName),
            "user4Overwrite",
            auditInfo);
    Assertions.assertDoesNotThrow(() -> userMetaService.insertUser(user4Overwrite, true));

    UserEntity actualOverwriteUser4 =
        userMetaService.getUserByIdentifier(user4Overwrite.nameIdentifier());
    Assertions.assertEquals("user4Overwrite", actualOverwriteUser4.name());
    Assertions.assertNull(actualOverwriteUser4.roleNames());
  }

  @Test
  void deleteUser() {
    AuditInfo auditInfo =
        AuditInfo.builder().withCreator("creator").withCreateTime(Instant.now()).build();
    BaseMetalake metalake =
        createBaseMakeLake(RandomIdGenerator.INSTANCE.nextId(), metalakeName, auditInfo);
    backend.insert(metalake, false);

    UserMetaService userMetaService = UserMetaService.getInstance();
    RoleMetaService roleMetaService = RoleMetaService.getInstance();

    // delete user
    UserEntity user1 =
        createUserEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            AuthorizationUtils.ofUserNamespace(metalakeName),
            "user1",
            auditInfo);
    Assertions.assertThrows(
        NoSuchEntityException.class,
        () -> userMetaService.getUserByIdentifier(user1.nameIdentifier()));
    Assertions.assertDoesNotThrow(() -> userMetaService.insertUser(user1, false));
    Assertions.assertEquals(user1, userMetaService.getUserByIdentifier(user1.nameIdentifier()));
    Assertions.assertTrue(userMetaService.deleteUser(user1.nameIdentifier()));
    Assertions.assertThrows(
        NoSuchEntityException.class,
        () -> userMetaService.getUserByIdentifier(user1.nameIdentifier()));

    // delete user with roles
    RoleEntity role1 =
        createRoleEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            AuthorizationUtils.ofRoleNamespace(metalakeName),
            "role1",
            auditInfo);
    RoleEntity role2 =
        createRoleEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            AuthorizationUtils.ofRoleNamespace(metalakeName),
            "role2",
            auditInfo);
    roleMetaService.insertRole(role1, false);
    roleMetaService.insertRole(role2, false);
    UserEntity user2 =
        createUserEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            AuthorizationUtils.ofUserNamespace(metalakeName),
            "user2",
            auditInfo,
            Lists.newArrayList(role1.name(), role2.name()),
            Lists.newArrayList(role1.id(), role2.id()));
    userMetaService.insertUser(user2, false);
    List<RolePO> rolePOs =
        SessionUtils.doWithCommitAndFetchResult(
            RoleMetaMapper.class, mapper -> mapper.listRolesByUserId(user2.id()));
    Assertions.assertEquals(2, rolePOs.size());
    UserEntity actualUser = userMetaService.getUserByIdentifier(user2.nameIdentifier());
    Assertions.assertEquals(user2.name(), actualUser.name());
    Assertions.assertEquals(
        Sets.newHashSet(user2.roleNames()), Sets.newHashSet(actualUser.roleNames()));

    Assertions.assertTrue(userMetaService.deleteUser(user2.nameIdentifier()));
    Assertions.assertThrows(
        NoSuchEntityException.class,
        () -> userMetaService.getUserByIdentifier(user2.nameIdentifier()));
    rolePOs =
        SessionUtils.doWithCommitAndFetchResult(
            RoleMetaMapper.class, mapper -> mapper.listRolesByUserId(user2.id()));
    Assertions.assertEquals(0, rolePOs.size());
  }

  @Test
  void updateUser() {
    AuditInfo auditInfo =
        AuditInfo.builder().withCreator("creator").withCreateTime(Instant.now()).build();
    BaseMetalake metalake =
        createBaseMakeLake(RandomIdGenerator.INSTANCE.nextId(), metalakeName, auditInfo);
    backend.insert(metalake, false);

    UserMetaService userMetaService = UserMetaService.getInstance();
    RoleMetaService roleMetaService = RoleMetaService.getInstance();

    RoleEntity role1 =
        createRoleEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            AuthorizationUtils.ofRoleNamespace(metalakeName),
            "role1",
            auditInfo);
    RoleEntity role2 =
        createRoleEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            AuthorizationUtils.ofRoleNamespace(metalakeName),
            "role2",
            auditInfo);
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
    UserEntity actualUser = userMetaService.getUserByIdentifier(user1.nameIdentifier());
    Assertions.assertEquals(user1.name(), actualUser.name());
    Assertions.assertEquals(
        Sets.newHashSet(user1.roleNames()), Sets.newHashSet(actualUser.roleNames()));

    RoleEntity role3 =
        createRoleEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            AuthorizationUtils.ofRoleNamespace(metalakeName),
            "role3",
            auditInfo);
    roleMetaService.insertRole(role3, false);

    // update user (grant)
    Function<UserEntity, UserEntity> grantUpdater =
        user -> {
          AuditInfo updateAuditInfo =
              AuditInfo.builder()
                  .withCreator(user.auditInfo().creator())
                  .withCreateTime(user.auditInfo().createTime())
                  .withLastModifier("grantUser")
                  .withLastModifiedTime(Instant.now())
                  .build();

          List<String> roleNames = Lists.newArrayList(user.roleNames());
          List<Long> roleIds = Lists.newArrayList(user.roleIds());
          roleNames.add(role3.name());
          roleIds.add(role3.id());

          return UserEntity.builder()
              .withNamespace(user.namespace())
              .withId(user.id())
              .withName(user.name())
              .withRoleNames(roleNames)
              .withRoleIds(roleIds)
              .withAuditInfo(updateAuditInfo)
              .build();
        };

    Assertions.assertNotNull(userMetaService.updateUser(user1.nameIdentifier(), grantUpdater));
    UserEntity grantUser =
        UserMetaService.getInstance().getUserByIdentifier(user1.nameIdentifier());
    Assertions.assertEquals(user1.id(), grantUser.id());
    Assertions.assertEquals(user1.name(), grantUser.name());
    Assertions.assertEquals(
        Sets.newHashSet("role1", "role2", "role3"), Sets.newHashSet(grantUser.roleNames()));
    Assertions.assertEquals(
        Sets.newHashSet(role1.id(), role2.id(), role3.id()), Sets.newHashSet(grantUser.roleIds()));
    Assertions.assertEquals("creator", grantUser.auditInfo().creator());
    Assertions.assertEquals("grantUser", grantUser.auditInfo().lastModifier());

    // update user (revoke)
    Function<UserEntity, UserEntity> revokeUpdater =
        user -> {
          AuditInfo updateAuditInfo =
              AuditInfo.builder()
                  .withCreator(user.auditInfo().creator())
                  .withCreateTime(user.auditInfo().createTime())
                  .withLastModifier("revokeUser")
                  .withLastModifiedTime(Instant.now())
                  .build();

          List<String> roleNames = Lists.newArrayList(user.roleNames());
          List<Long> roleIds = Lists.newArrayList(user.roleIds());
          roleIds.remove(roleNames.indexOf("role2"));
          roleNames.remove("role2");

          return UserEntity.builder()
              .withNamespace(user.namespace())
              .withId(user.id())
              .withName(user.name())
              .withRoleNames(roleNames)
              .withRoleIds(roleIds)
              .withAuditInfo(updateAuditInfo)
              .build();
        };

    Assertions.assertNotNull(userMetaService.updateUser(user1.nameIdentifier(), revokeUpdater));
    UserEntity revokeUser =
        UserMetaService.getInstance().getUserByIdentifier(user1.nameIdentifier());
    Assertions.assertEquals(user1.id(), revokeUser.id());
    Assertions.assertEquals(user1.name(), revokeUser.name());
    Assertions.assertEquals(
        Sets.newHashSet("role1", "role3"), Sets.newHashSet(revokeUser.roleNames()));
    Assertions.assertEquals(
        Sets.newHashSet(role1.id(), role3.id()), Sets.newHashSet(revokeUser.roleIds()));
    Assertions.assertEquals("creator", revokeUser.auditInfo().creator());
    Assertions.assertEquals("revokeUser", revokeUser.auditInfo().lastModifier());

    RoleEntity role4 =
        createRoleEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            AuthorizationUtils.ofRoleNamespace(metalakeName),
            "role4",
            auditInfo);
    roleMetaService.insertRole(role4, false);

    // update user (grant & revoke)
    Function<UserEntity, UserEntity> grantRevokeUpdater =
        user -> {
          AuditInfo updateAuditInfo =
              AuditInfo.builder()
                  .withCreator(user.auditInfo().creator())
                  .withCreateTime(user.auditInfo().createTime())
                  .withLastModifier("grantRevokeUser")
                  .withLastModifiedTime(Instant.now())
                  .build();

          List<String> roleNames = Lists.newArrayList(user.roleNames());
          List<Long> roleIds = Lists.newArrayList(user.roleIds());
          roleIds.remove(roleNames.indexOf("role3"));
          roleNames.remove("role3");
          roleIds.add(role4.id());
          roleNames.add(role4.name());

          return UserEntity.builder()
              .withNamespace(user.namespace())
              .withId(user.id())
              .withName(user.name())
              .withRoleNames(roleNames)
              .withRoleIds(roleIds)
              .withAuditInfo(updateAuditInfo)
              .build();
        };
    Assertions.assertNotNull(
        userMetaService.updateUser(user1.nameIdentifier(), grantRevokeUpdater));
    UserEntity grantRevokeUser =
        UserMetaService.getInstance().getUserByIdentifier(user1.nameIdentifier());
    Assertions.assertEquals(user1.id(), grantRevokeUser.id());
    Assertions.assertEquals(user1.name(), grantRevokeUser.name());
    Assertions.assertEquals(
        Sets.newHashSet("role1", "role4"), Sets.newHashSet(grantRevokeUser.roleNames()));
    Assertions.assertEquals(
        Sets.newHashSet(role1.id(), role4.id()), Sets.newHashSet(grantRevokeUser.roleIds()));
    Assertions.assertEquals("creator", grantRevokeUser.auditInfo().creator());
    Assertions.assertEquals("grantRevokeUser", grantRevokeUser.auditInfo().lastModifier());

    Function<UserEntity, UserEntity> noUpdater =
        user -> {
          AuditInfo updateAuditInfo =
              AuditInfo.builder()
                  .withCreator(user.auditInfo().creator())
                  .withCreateTime(user.auditInfo().createTime())
                  .withLastModifier("noUpdateUser")
                  .withLastModifiedTime(Instant.now())
                  .build();

          List<String> roleNames = Lists.newArrayList(user.roleNames());
          List<Long> roleIds = Lists.newArrayList(user.roleIds());

          return UserEntity.builder()
              .withNamespace(user.namespace())
              .withId(user.id())
              .withName(user.name())
              .withRoleNames(roleNames)
              .withRoleIds(roleIds)
              .withAuditInfo(updateAuditInfo)
              .build();
        };
    Assertions.assertNotNull(userMetaService.updateUser(user1.nameIdentifier(), noUpdater));
    UserEntity noUpdaterUser =
        UserMetaService.getInstance().getUserByIdentifier(user1.nameIdentifier());
    Assertions.assertEquals(user1.id(), noUpdaterUser.id());
    Assertions.assertEquals(user1.name(), noUpdaterUser.name());
    Assertions.assertEquals(
        Sets.newHashSet("role1", "role4"), Sets.newHashSet(noUpdaterUser.roleNames()));
    Assertions.assertEquals(
        Sets.newHashSet(role1.id(), role4.id()), Sets.newHashSet(noUpdaterUser.roleIds()));
    Assertions.assertEquals("creator", noUpdaterUser.auditInfo().creator());
    Assertions.assertEquals("grantRevokeUser", noUpdaterUser.auditInfo().lastModifier());
  }

  @Test
  void deleteMetalake() {
    AuditInfo auditInfo =
        AuditInfo.builder().withCreator("creator").withCreateTime(Instant.now()).build();
    BaseMetalake metalake =
        createBaseMakeLake(RandomIdGenerator.INSTANCE.nextId(), metalakeName, auditInfo);
    backend.insert(metalake, false);

    UserMetaService userMetaService = UserMetaService.getInstance();
    RoleMetaService roleMetaService = RoleMetaService.getInstance();

    RoleEntity role1 =
        createRoleEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            AuthorizationUtils.ofRoleNamespace(metalakeName),
            "role1",
            auditInfo);
    RoleEntity role2 =
        createRoleEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            AuthorizationUtils.ofRoleNamespace(metalakeName),
            "role2",
            auditInfo);
    RoleEntity role3 =
        createRoleEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            AuthorizationUtils.ofRoleNamespace(metalakeName),
            "role3",
            auditInfo);
    roleMetaService.insertRole(role1, false);
    roleMetaService.insertRole(role2, false);
    roleMetaService.insertRole(role3, false);
    UserEntity user1 =
        createUserEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            AuthorizationUtils.ofUserNamespace(metalakeName),
            "user1",
            auditInfo,
            Lists.newArrayList(role1.name(), role2.name()),
            Lists.newArrayList(role1.id(), role2.id()));
    UserEntity user2 =
        createUserEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            AuthorizationUtils.ofUserNamespace(metalakeName),
            "user2",
            auditInfo,
            Lists.newArrayList(role3.name()),
            Lists.newArrayList(role3.id()));
    userMetaService.insertUser(user1, false);
    userMetaService.insertUser(user2, false);

    Assertions.assertEquals(
        user1.name(), userMetaService.getUserByIdentifier(user1.nameIdentifier()).name());
    Assertions.assertEquals(2, roleMetaService.listRolesByUserId(user1.id()).size());
    Assertions.assertEquals(
        user2.name(), userMetaService.getUserByIdentifier(user2.nameIdentifier()).name());
    Assertions.assertEquals(1, roleMetaService.listRolesByUserId(user2.id()).size());

    // delete metalake without cascade
    Assertions.assertTrue(
        MetalakeMetaService.getInstance().deleteMetalake(metalake.nameIdentifier(), false));

    Assertions.assertThrows(
        NoSuchEntityException.class,
        () -> userMetaService.getUserByIdentifier(user1.nameIdentifier()));
    Assertions.assertThrows(
        NoSuchEntityException.class,
        () -> userMetaService.getUserByIdentifier(user2.nameIdentifier()));
    Assertions.assertEquals(0, roleMetaService.listRolesByUserId(user1.id()).size());
    Assertions.assertEquals(0, roleMetaService.listRolesByUserId(user2.id()).size());
  }

  @Test
  void deleteMetalakeCascade() {
    AuditInfo auditInfo =
        AuditInfo.builder().withCreator("creator").withCreateTime(Instant.now()).build();
    BaseMetalake metalake =
        createBaseMakeLake(RandomIdGenerator.INSTANCE.nextId(), metalakeName, auditInfo);
    backend.insert(metalake, false);

    UserMetaService userMetaService = UserMetaService.getInstance();
    RoleMetaService roleMetaService = RoleMetaService.getInstance();

    RoleEntity role1 =
        createRoleEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            AuthorizationUtils.ofRoleNamespace(metalakeName),
            "role1",
            auditInfo);
    RoleEntity role2 =
        createRoleEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            AuthorizationUtils.ofRoleNamespace(metalakeName),
            "role2",
            auditInfo);
    RoleEntity role3 =
        createRoleEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            AuthorizationUtils.ofRoleNamespace(metalakeName),
            "role3",
            auditInfo);
    roleMetaService.insertRole(role1, false);
    roleMetaService.insertRole(role2, false);
    roleMetaService.insertRole(role3, false);
    UserEntity user1 =
        createUserEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            AuthorizationUtils.ofUserNamespace(metalakeName),
            "user1",
            auditInfo,
            Lists.newArrayList(role1.name(), role2.name()),
            Lists.newArrayList(role1.id(), role2.id()));
    UserEntity user2 =
        createUserEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            AuthorizationUtils.ofUserNamespace(metalakeName),
            "user2",
            auditInfo,
            Lists.newArrayList(role3.name()),
            Lists.newArrayList(role3.id()));
    userMetaService.insertUser(user1, false);
    userMetaService.insertUser(user2, false);

    Assertions.assertEquals(
        user1.name(), userMetaService.getUserByIdentifier(user1.nameIdentifier()).name());
    Assertions.assertEquals(2, roleMetaService.listRolesByUserId(user1.id()).size());
    Assertions.assertEquals(
        user2.name(), userMetaService.getUserByIdentifier(user2.nameIdentifier()).name());
    Assertions.assertEquals(1, roleMetaService.listRolesByUserId(user2.id()).size());

    // delete metalake with cascade
    Assertions.assertTrue(
        MetalakeMetaService.getInstance().deleteMetalake(metalake.nameIdentifier(), true));

    Assertions.assertThrows(
        NoSuchEntityException.class,
        () -> userMetaService.getUserByIdentifier(user1.nameIdentifier()));
    Assertions.assertThrows(
        NoSuchEntityException.class,
        () -> userMetaService.getUserByIdentifier(user2.nameIdentifier()));
    Assertions.assertEquals(0, roleMetaService.listRolesByUserId(user1.id()).size());
    Assertions.assertEquals(0, roleMetaService.listRolesByUserId(user2.id()).size());
  }

  @Test
  void deleteUserMetasByLegacyTimeLine() {
    AuditInfo auditInfo =
        AuditInfo.builder().withCreator("creator").withCreateTime(Instant.now()).build();
    BaseMetalake metalake =
        createBaseMakeLake(RandomIdGenerator.INSTANCE.nextId(), metalakeName, auditInfo);
    backend.insert(metalake, false);

    UserMetaService userMetaService = UserMetaService.getInstance();
    RoleMetaService roleMetaService = RoleMetaService.getInstance();

    RoleEntity role1 =
        createRoleEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            AuthorizationUtils.ofRoleNamespace(metalakeName),
            "role1",
            auditInfo);
    RoleEntity role2 =
        createRoleEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            AuthorizationUtils.ofRoleNamespace(metalakeName),
            "role2",
            auditInfo);
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
    UserEntity user2 =
        createUserEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            AuthorizationUtils.ofUserNamespace(metalakeName),
            "user2",
            auditInfo,
            Lists.newArrayList(role1.name(), role2.name()),
            Lists.newArrayList(role1.id(), role2.id()));
    UserEntity user3 =
        createUserEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            AuthorizationUtils.ofUserNamespace(metalakeName),
            "user3",
            auditInfo,
            Lists.newArrayList(role1.name(), role2.name()),
            Lists.newArrayList(role1.id(), role2.id()));
    UserEntity user4 =
        createUserEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            AuthorizationUtils.ofUserNamespace(metalakeName),
            "user4",
            auditInfo,
            Lists.newArrayList(role1.name(), role2.name()),
            Lists.newArrayList(role1.id(), role2.id()));
    userMetaService.insertUser(user1, false);
    userMetaService.insertUser(user2, false);
    userMetaService.insertUser(user3, false);
    userMetaService.insertUser(user4, false);

    // hard delete before soft delete
    int deletedCount =
        userMetaService.deleteUserMetasByLegacyTimeLine(Instant.now().toEpochMilli() + 1000, 4);
    Assertions.assertEquals(0, deletedCount);
    Assertions.assertEquals(
        user1.name(), userMetaService.getUserByIdentifier(user1.nameIdentifier()).name());
    Assertions.assertEquals(
        user2.name(), userMetaService.getUserByIdentifier(user2.nameIdentifier()).name());
    Assertions.assertEquals(
        user3.name(), userMetaService.getUserByIdentifier(user3.nameIdentifier()).name());
    Assertions.assertEquals(
        user4.name(), userMetaService.getUserByIdentifier(user4.nameIdentifier()).name());
    Assertions.assertEquals(2, roleMetaService.listRolesByUserId(user1.id()).size());
    Assertions.assertEquals(2, roleMetaService.listRolesByUserId(user2.id()).size());
    Assertions.assertEquals(2, roleMetaService.listRolesByUserId(user3.id()).size());
    Assertions.assertEquals(2, roleMetaService.listRolesByUserId(user4.id()).size());
    Assertions.assertEquals(4, countUsers(metalake.id()));
    Assertions.assertEquals(8, countUserRoleRels());

    // delete metalake
    Assertions.assertTrue(
        MetalakeMetaService.getInstance().deleteMetalake(metalake.nameIdentifier(), true));
    Assertions.assertThrows(
        NoSuchEntityException.class,
        () -> userMetaService.getUserByIdentifier(user1.nameIdentifier()));
    Assertions.assertThrows(
        NoSuchEntityException.class,
        () -> userMetaService.getUserByIdentifier(user2.nameIdentifier()));
    Assertions.assertThrows(
        NoSuchEntityException.class,
        () -> userMetaService.getUserByIdentifier(user3.nameIdentifier()));
    Assertions.assertThrows(
        NoSuchEntityException.class,
        () -> userMetaService.getUserByIdentifier(user4.nameIdentifier()));
    Assertions.assertEquals(0, roleMetaService.listRolesByUserId(user1.id()).size());
    Assertions.assertEquals(0, roleMetaService.listRolesByUserId(user2.id()).size());
    Assertions.assertEquals(0, roleMetaService.listRolesByUserId(user3.id()).size());
    Assertions.assertEquals(0, roleMetaService.listRolesByUserId(user4.id()).size());
    Assertions.assertEquals(4, countUsers(metalake.id()));
    Assertions.assertEquals(8, countUserRoleRels());

    // hard delete after soft delete
    deletedCount =
        userMetaService.deleteUserMetasByLegacyTimeLine(Instant.now().toEpochMilli() + 1000, 3);
    Assertions.assertEquals(6, deletedCount); // delete 3 user + 3 userRoleRel
    Assertions.assertEquals(1, countUsers(metalake.id())); // 4 - 3
    Assertions.assertEquals(5, countUserRoleRels()); // 8 - 3

    deletedCount =
        userMetaService.deleteUserMetasByLegacyTimeLine(Instant.now().toEpochMilli() + 1000, 3);
    Assertions.assertEquals(4, deletedCount); // delete 1 user + 3 userRoleRel
    Assertions.assertEquals(0, countUsers(metalake.id()));
    Assertions.assertEquals(2, countUserRoleRels()); // 5 - 3

    deletedCount =
        userMetaService.deleteUserMetasByLegacyTimeLine(Instant.now().toEpochMilli() + 1000, 3);
    Assertions.assertEquals(2, deletedCount);
    Assertions.assertEquals(0, countUsers(metalake.id()));
    Assertions.assertEquals(0, countUserRoleRels());

    deletedCount =
        userMetaService.deleteUserMetasByLegacyTimeLine(Instant.now().toEpochMilli() + 1000, 3);
    Assertions.assertEquals(0, deletedCount); // no more to delete
  }

  private Integer countUsers(Long metalakeId) {
    int count = 0;
    try (SqlSession sqlSession =
            SqlSessionFactoryHelper.getInstance().getSqlSessionFactory().openSession(true);
        Connection connection = sqlSession.getConnection();
        Statement statement = connection.createStatement();
        ResultSet rs =
            statement.executeQuery(
                String.format(
                    "SELECT count(*) FROM user_meta WHERE metalake_id = %d", metalakeId))) {
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
}
