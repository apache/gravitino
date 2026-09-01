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

import static org.apache.gravitino.metrics.source.MetricsSource.GRAVITINO_RELATIONAL_STORE_METRIC_NAME;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.gravitino.Entity;
import org.apache.gravitino.HasIdentifier;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.authorization.AuthorizationUtils;
import org.apache.gravitino.authorization.PagedResult;
import org.apache.gravitino.exceptions.NoSuchEntityException;
import org.apache.gravitino.meta.RoleEntity;
import org.apache.gravitino.meta.UserEntity;
import org.apache.gravitino.metrics.Monitored;
import org.apache.gravitino.storage.relational.mapper.MetalakeMetaMapper;
import org.apache.gravitino.storage.relational.mapper.OwnerMetaMapper;
import org.apache.gravitino.storage.relational.mapper.UserMetaMapper;
import org.apache.gravitino.storage.relational.mapper.UserRoleRelMapper;
import org.apache.gravitino.storage.relational.po.ExtendedUserPO;
import org.apache.gravitino.storage.relational.po.MetalakePO;
import org.apache.gravitino.storage.relational.po.RolePO;
import org.apache.gravitino.storage.relational.po.UserPO;
import org.apache.gravitino.storage.relational.po.UserRoleRelPO;
import org.apache.gravitino.storage.relational.utils.ExceptionUtils;
import org.apache.gravitino.storage.relational.utils.POConverters;
import org.apache.gravitino.storage.relational.utils.SessionUtils;

/** The service class for user metadata. It provides the basic database operations for user. */
public class UserMetaService {
  private static final UserMetaService INSTANCE = new UserMetaService();

  public static UserMetaService getInstance() {
    return INSTANCE;
  }

  private UserMetaService() {}

  private UserPO getUserPOByMetalakeIdAndName(Long metalakeId, String userName) {
    UserPO userPO =
        SessionUtils.getWithoutCommit(
            UserMetaMapper.class,
            mapper -> mapper.selectUserMetaByMetalakeIdAndName(metalakeId, userName));

    if (userPO == null) {
      throw new NoSuchEntityException(
          NoSuchEntityException.NO_SUCH_ENTITY_MESSAGE,
          Entity.EntityType.USER.name().toLowerCase(),
          userName);
    }
    return userPO;
  }

  @Monitored(
      metricsSource = GRAVITINO_RELATIONAL_STORE_METRIC_NAME,
      baseMetricName = "getUserIdByMetalakeIdAndName")
  public Long getUserIdByMetalakeIdAndName(Long metalakeId, String userName) {
    Long userId =
        SessionUtils.getWithoutCommit(
            UserMetaMapper.class,
            mapper -> mapper.selectUserIdByMetalakeIdAndName(metalakeId, userName));

    if (userId == null) {
      throw new NoSuchEntityException(
          NoSuchEntityException.NO_SUCH_ENTITY_MESSAGE,
          Entity.EntityType.USER.name().toLowerCase(),
          userName);
    }
    return userId;
  }

  @Monitored(
      metricsSource = GRAVITINO_RELATIONAL_STORE_METRIC_NAME,
      baseMetricName = "getUserByIdentifier")
  public UserEntity getUserByIdentifier(NameIdentifier identifier) {
    AuthorizationUtils.checkUser(identifier);

    Long metalakeId =
        MetalakeMetaService.getInstance().getMetalakeIdByName(identifier.namespace().level(0));
    UserPO userPO = getUserPOByMetalakeIdAndName(metalakeId, identifier.name());
    List<RolePO> rolePOs = RoleMetaService.getInstance().listRolesByUserId(userPO.getUserId());

    return POConverters.fromUserPO(userPO, rolePOs, identifier.namespace());
  }

  @Monitored(
      metricsSource = GRAVITINO_RELATIONAL_STORE_METRIC_NAME,
      baseMetricName = "listUsersByRoleIdent")
  public List<UserEntity> listUsersByRoleIdent(NameIdentifier roleIdent) {
    RoleEntity roleEntity = RoleMetaService.getInstance().getRoleByIdentifier(roleIdent);
    List<UserPO> userPOs =
        SessionUtils.getWithoutCommit(
            UserMetaMapper.class, mapper -> mapper.listUsersByRoleId(roleEntity.id()));
    return userPOs.stream()
        .map(
            po ->
                POConverters.fromUserPO(
                    po,
                    Collections.emptyList(),
                    AuthorizationUtils.ofUserNamespace(roleIdent.namespace().level(0))))
        .collect(Collectors.toList());
  }

  @Monitored(metricsSource = GRAVITINO_RELATIONAL_STORE_METRIC_NAME, baseMetricName = "insertUser")
  public void insertUser(UserEntity userEntity, boolean overwritten) throws IOException {
    try {
      AuthorizationUtils.checkUser(userEntity.nameIdentifier());

      String metalakeName = userEntity.namespace().level(0);
      MetalakePO metalakePO =
          SessionUtils.getWithoutCommit(
              MetalakeMetaMapper.class, mapper -> mapper.selectMetalakeMetaByName(metalakeName));
      if (metalakePO == null) {
        throw new NoSuchEntityException(
            NoSuchEntityException.NO_SUCH_ENTITY_MESSAGE,
            Entity.EntityType.METALAKE.name().toLowerCase(),
            metalakeName);
      }

      UserPO.Builder builder = UserPO.builder().withMetalakeId(metalakePO.getMetalakeId());
      UserPO userPO = POConverters.initializeUserPOWithVersion(userEntity, builder);

      List<Long> roleIds = Optional.ofNullable(userEntity.roleIds()).orElse(Lists.newArrayList());
      List<UserRoleRelPO> userRoleRelPOs =
          POConverters.initializeUserRoleRelsPOWithVersion(userEntity, roleIds);

      SessionUtils.doMultipleWithCommit(
          () -> lockMetalakeForUserCreate(metalakePO),
          () ->
              SessionUtils.doWithoutCommit(
                  UserMetaMapper.class,
                  mapper -> {
                    if (overwritten) {
                      mapper.insertUserMetaOnDuplicateKeyUpdate(userPO);
                    } else {
                      mapper.insertUserMeta(userPO);
                    }
                  }),
          () -> {
            SessionUtils.doWithoutCommit(
                UserRoleRelMapper.class,
                mapper -> {
                  if (overwritten) {
                    mapper.softDeleteUserRoleRelByUserId(userEntity.id());
                  }
                  if (!userRoleRelPOs.isEmpty()) {
                    mapper.batchInsertUserRoleRel(userRoleRelPOs);
                  }
                });
          });
    } catch (RuntimeException re) {
      ExceptionUtils.checkSQLException(
          re, Entity.EntityType.USER, userEntity.nameIdentifier().toString());
      throw re;
    }
  }

  @Monitored(metricsSource = GRAVITINO_RELATIONAL_STORE_METRIC_NAME, baseMetricName = "deleteUser")
  public boolean deleteUser(NameIdentifier identifier) {
    AuthorizationUtils.checkUser(identifier);

    Long metalakeId =
        MetalakeMetaService.getInstance().getMetalakeIdByName(identifier.namespace().level(0));
    UserPO userPO = getUserPOByMetalakeIdAndName(metalakeId, identifier.name());

    deleteUserWithVersion(identifier, userPO);
    return true;
  }

  /**
   * Deletes the user whose version matches {@code observedUserPO}, together with its role and owner
   * relations. Package-private so tests can hand in a deliberately stale PO; callers outside this
   * class go through {@link #deleteUser(NameIdentifier)}, which reads the row first.
   *
   * @param identifier the user being deleted, used only to build the error
   * @param observedUserPO the user row the caller observed, carrying the version to match
   */
  void deleteUserWithVersion(NameIdentifier identifier, UserPO observedUserPO) {
    Long userId = observedUserPO.getUserId();
    SessionUtils.doMultipleWithCommit(
        () -> {
          int deleted =
              SessionUtils.getWithoutCommit(
                  UserMetaMapper.class,
                  mapper ->
                      mapper.softDeleteUserMetaByUserId(
                          userId, observedUserPO.getCurrentVersion()));
          if (deleted == 0) {
            throw userWriteFailure(identifier, observedUserPO, UserLookup.NAME);
          }
        },
        () ->
            SessionUtils.doWithoutCommit(
                UserRoleRelMapper.class, mapper -> mapper.softDeleteUserRoleRelByUserId(userId)),
        () ->
            SessionUtils.doWithoutCommit(
                OwnerMetaMapper.class,
                mapper ->
                    mapper.softDeleteOwnerRelByOwnerIdAndType(
                        userId, Entity.EntityType.USER.name())));
  }

  @Monitored(metricsSource = GRAVITINO_RELATIONAL_STORE_METRIC_NAME, baseMetricName = "updateUser")
  public <E extends Entity & HasIdentifier> UserEntity updateUser(
      NameIdentifier identifier, Function<E, E> updater) throws IOException {
    AuthorizationUtils.checkUser(identifier);

    Long metalakeId =
        MetalakeMetaService.getInstance().getMetalakeIdByName(identifier.namespace().level(0));
    UserPO oldUserPO = getUserPOByMetalakeIdAndName(metalakeId, identifier.name());
    List<RolePO> rolePOs = RoleMetaService.getInstance().listRolesByUserId(oldUserPO.getUserId());
    UserEntity oldUserEntity = POConverters.fromUserPO(oldUserPO, rolePOs, identifier.namespace());

    UserEntity newEntity = (UserEntity) updater.apply((E) oldUserEntity);
    Preconditions.checkArgument(
        Objects.equals(oldUserEntity.id(), newEntity.id()),
        "The updated user entity id: %s should be same with the user entity id before: %s",
        newEntity.id(),
        oldUserEntity.id());

    Set<Long> oldRoleIds =
        oldUserEntity.roleIds() == null
            ? Sets.newHashSet()
            : Sets.newHashSet(oldUserEntity.roleIds());
    Set<Long> newRoleIds =
        newEntity.roleIds() == null ? Sets.newHashSet() : Sets.newHashSet(newEntity.roleIds());

    Set<Long> insertRoleIds = Sets.difference(newRoleIds, oldRoleIds);
    Set<Long> deleteRoleIds = Sets.difference(oldRoleIds, newRoleIds);

    // Every update runs the compare-and-set, including one that leaves the roles untouched. The
    // short-circuit that used to return early here would skip the version check, so a caller whose
    // snapshot was already stale would be told the update succeeded. It also has to run because a
    // metadata-only change, such as the audit info, still has to be written.
    try {
      SessionUtils.doMultipleWithCommit(
          () -> {
            int updated =
                SessionUtils.getWithoutCommit(
                    UserMetaMapper.class,
                    mapper ->
                        mapper.updateUserMeta(
                            POConverters.updateUserPOWithVersion(oldUserPO, newEntity), oldUserPO));
            if (updated == 0) {
              throw userWriteFailure(identifier, oldUserPO, UserLookup.NAME);
            }
          },
          () -> {
            if (insertRoleIds.isEmpty()) {
              return;
            }
            SessionUtils.doWithoutCommit(
                UserRoleRelMapper.class,
                mapper ->
                    mapper.batchInsertUserRoleRel(
                        POConverters.initializeUserRoleRelsPOWithVersion(
                            newEntity, Lists.newArrayList(insertRoleIds))));
          },
          () -> {
            if (deleteRoleIds.isEmpty()) {
              return;
            }
            SessionUtils.doWithoutCommit(
                UserRoleRelMapper.class,
                mapper ->
                    mapper.softDeleteUserRoleRelByUserAndRoles(
                        newEntity.id(), Lists.newArrayList(deleteRoleIds)));
          },
          () ->
              SessionUtils.doWithoutCommit(
                  UserMetaMapper.class,
                  mapper -> mapper.touchUserUpdatedAt(oldUserPO.getUserId())));
    } catch (RuntimeException re) {
      ExceptionUtils.checkSQLException(
          re, Entity.EntityType.USER, newEntity.nameIdentifier().toString());
      throw re;
    }
    return newEntity;
  }

  @Monitored(
      metricsSource = GRAVITINO_RELATIONAL_STORE_METRIC_NAME,
      baseMetricName = "listUsersByNamespace")
  public List<UserEntity> listUsersByNamespace(Namespace namespace, boolean allFields) {
    AuthorizationUtils.checkUserNamespace(namespace);
    String metalakeName = namespace.level(0);

    if (allFields) {
      Long metalakeId = MetalakeMetaService.getInstance().getMetalakeIdByName(metalakeName);
      List<ExtendedUserPO> userPOs =
          SessionUtils.getWithoutCommit(
              UserMetaMapper.class, mapper -> mapper.listExtendedUserPOsByMetalakeId(metalakeId));
      return userPOs.stream()
          .map(
              po ->
                  POConverters.fromExtendedUserPO(
                      po, AuthorizationUtils.ofUserNamespace(metalakeName)))
          .collect(Collectors.toList());
    } else {
      List<UserPO> userPOs =
          SessionUtils.getWithoutCommit(
              UserMetaMapper.class, mapper -> mapper.listUserPOsByMetalake(metalakeName));
      return userPOs.stream()
          .map(
              po ->
                  POConverters.fromUserPO(
                      po,
                      Collections.emptyList(),
                      AuthorizationUtils.ofUserNamespace(metalakeName)))
          .collect(Collectors.toList());
    }
  }

  @Monitored(
      metricsSource = GRAVITINO_RELATIONAL_STORE_METRIC_NAME,
      baseMetricName = "deleteUserMetasByLegacyTimeline")
  public int deleteUserMetasByLegacyTimeline(long legacyTimeline, int limit) {
    int[] userDeletedCount = new int[] {0};
    int[] userRoleRelDeletedCount = new int[] {0};

    SessionUtils.doMultipleWithCommit(
        () ->
            userDeletedCount[0] =
                SessionUtils.getWithoutCommit(
                    UserMetaMapper.class,
                    mapper -> mapper.deleteUserMetasByLegacyTimeline(legacyTimeline, limit)),
        () ->
            userRoleRelDeletedCount[0] =
                SessionUtils.getWithoutCommit(
                    UserRoleRelMapper.class,
                    mapper ->
                        mapper.deleteUserRoleRelMetasByLegacyTimeline(legacyTimeline, limit)));

    return userDeletedCount[0] + userRoleRelDeletedCount[0];
  }

  private UserPO getUserPOByMetalakeNameAndExternalId(String metalakeName, String externalId) {
    UserPO userPO =
        SessionUtils.getWithoutCommit(
            UserMetaMapper.class,
            mapper -> mapper.selectUserMetaByMetalakeNameAndExternalId(metalakeName, externalId));

    if (userPO == null) {
      throw new NoSuchEntityException(
          NoSuchEntityException.NO_SUCH_ENTITY_MESSAGE,
          Entity.EntityType.USER.name().toLowerCase(),
          externalId);
    }
    return userPO;
  }

  @Monitored(
      metricsSource = GRAVITINO_RELATIONAL_STORE_METRIC_NAME,
      baseMetricName = "getUserByExternalId")
  public UserEntity getUserByExternalId(NameIdentifier ident) {
    AuthorizationUtils.checkUserExternalId(ident);
    String metalake = ident.namespace().level(0);
    String externalId = ident.name();
    Namespace userNamespace = AuthorizationUtils.ofUserNamespace(metalake);
    UserPO userPO = getUserPOByMetalakeNameAndExternalId(metalake, externalId);
    List<RolePO> rolePOs = RoleMetaService.getInstance().listRolesByUserId(userPO.getUserId());
    return POConverters.fromUserPO(userPO, rolePOs, userNamespace);
  }

  @Monitored(
      metricsSource = GRAVITINO_RELATIONAL_STORE_METRIC_NAME,
      baseMetricName = "updateUserByExternalId")
  public <E extends Entity & HasIdentifier> UserEntity updateUserByExternalId(
      NameIdentifier ident, Function<E, E> updater) throws IOException {
    AuthorizationUtils.checkUserExternalId(ident);
    String metalake = ident.namespace().level(0);
    String externalId = ident.name();
    Namespace userNamespace = AuthorizationUtils.ofUserNamespace(metalake);
    UserPO oldUserPO = getUserPOByMetalakeNameAndExternalId(metalake, externalId);
    List<RolePO> rolePOs = RoleMetaService.getInstance().listRolesByUserId(oldUserPO.getUserId());
    UserEntity oldEntity = POConverters.fromUserPO(oldUserPO, rolePOs, userNamespace);
    UserEntity newEntity = (UserEntity) updater.apply((E) oldEntity);
    Preconditions.checkArgument(
        Objects.equals(oldEntity.id(), newEntity.id()),
        "The updated user entity id: %s should be same with the user entity id before: %s",
        newEntity.id(),
        oldEntity.id());

    try {
      SessionUtils.doMultipleWithCommit(
          () -> {
            int updated =
                SessionUtils.getWithoutCommit(
                    UserMetaMapper.class,
                    mapper ->
                        mapper.updateUserMetaByExternalId(
                            POConverters.updateUserPOWithVersion(oldUserPO, newEntity), oldUserPO));
            if (updated == 0) {
              throw userWriteFailure(ident, oldUserPO, UserLookup.EXTERNAL_ID);
            }
          },
          () ->
              SessionUtils.doWithoutCommit(
                  UserMetaMapper.class,
                  mapper -> mapper.touchUserUpdatedAt(oldUserPO.getUserId())));
    } catch (RuntimeException re) {
      ExceptionUtils.checkSQLException(
          re, Entity.EntityType.USER, newEntity.nameIdentifier().toString());
      throw re;
    }
    return newEntity;
  }

  private UserPO getUserPOByMetalakeNameAndId(String metalakeName, Long userId) {
    UserPO userPO =
        SessionUtils.getWithoutCommit(
            UserMetaMapper.class,
            mapper -> mapper.selectUserMetaByMetalakeNameAndId(metalakeName, userId));

    if (userPO == null) {
      throw new NoSuchEntityException(
          NoSuchEntityException.NO_SUCH_ENTITY_MESSAGE,
          Entity.EntityType.USER.name().toLowerCase(),
          String.valueOf(userId));
    }
    return userPO;
  }

  @Monitored(metricsSource = GRAVITINO_RELATIONAL_STORE_METRIC_NAME, baseMetricName = "getUserById")
  public UserEntity getUserById(String metalake, long userId) {
    Namespace userNamespace = AuthorizationUtils.ofUserNamespace(metalake);
    UserPO userPO = getUserPOByMetalakeNameAndId(metalake, userId);
    List<RolePO> rolePOs = RoleMetaService.getInstance().listRolesByUserId(userPO.getUserId());
    return POConverters.fromUserPO(userPO, rolePOs, userNamespace);
  }

  @Monitored(
      metricsSource = GRAVITINO_RELATIONAL_STORE_METRIC_NAME,
      baseMetricName = "updateUserById")
  public <E extends Entity & HasIdentifier> UserEntity updateUserById(
      String metalake, long userId, Function<E, E> updater) throws IOException {
    Namespace userNamespace = AuthorizationUtils.ofUserNamespace(metalake);
    UserPO oldUserPO = getUserPOByMetalakeNameAndId(metalake, userId);
    List<RolePO> rolePOs = RoleMetaService.getInstance().listRolesByUserId(oldUserPO.getUserId());
    UserEntity oldEntity = POConverters.fromUserPO(oldUserPO, rolePOs, userNamespace);
    UserEntity newEntity = (UserEntity) updater.apply((E) oldEntity);
    Preconditions.checkArgument(
        Objects.equals(oldEntity.id(), newEntity.id()),
        "The updated user entity id: %s should be same with the user entity id before: %s",
        newEntity.id(),
        oldEntity.id());

    try {
      SessionUtils.doMultipleWithCommit(
          () -> {
            int updated =
                SessionUtils.getWithoutCommit(
                    UserMetaMapper.class,
                    mapper ->
                        mapper.updateUserMeta(
                            POConverters.updateUserPOWithVersion(oldUserPO, newEntity), oldUserPO));
            if (updated == 0) {
              NameIdentifier userIdIdentifier =
                  AuthorizationUtils.ofUser(metalake, String.valueOf(userId));
              throw userWriteFailure(userIdIdentifier, oldUserPO, UserLookup.ID);
            }
          },
          () ->
              SessionUtils.doWithoutCommit(
                  UserMetaMapper.class,
                  mapper -> mapper.touchUserUpdatedAt(oldUserPO.getUserId())));
    } catch (RuntimeException re) {
      ExceptionUtils.checkSQLException(
          re, Entity.EntityType.USER, newEntity.nameIdentifier().toString());
      throw re;
    }
    return newEntity;
  }

  @Monitored(
      metricsSource = GRAVITINO_RELATIONAL_STORE_METRIC_NAME,
      baseMetricName = "deleteUserById")
  public boolean deleteUserById(String metalake, long userId) {
    UserPO userPO;
    try {
      userPO = getUserPOByMetalakeNameAndId(metalake, userId);
    } catch (NoSuchEntityException e) {
      return false;
    }
    NameIdentifier identifier = AuthorizationUtils.ofUser(metalake, userPO.getUserName());

    // Starts false so that any path that does not reach the child cleanup reports "nothing was
    // deleted here" rather than claiming a delete it did not perform.
    AtomicBoolean deletedUser = new AtomicBoolean(false);
    SessionUtils.doMultipleWithCommit(
        () -> {
          int deleted =
              SessionUtils.getWithoutCommit(
                  UserMetaMapper.class,
                  mapper -> mapper.softDeleteUserMetaByUserId(userId, userPO.getCurrentVersion()));
          if (deleted == 0) {
            // The compare-and-set matched no row for one of two reasons. Either the row is already
            // gone, and a delete that has nothing left to delete is a no-op rather than an error,
            // or the row is still there under a newer version, which is a genuine conflict.
            if (getUserPOByIdForUpdate(userId) == null) {
              return;
            }
            throw ExceptionUtils.concurrentModification(Entity.EntityType.USER, identifier);
          }

          deletedUser.set(true);
          SessionUtils.doWithoutCommit(
              UserRoleRelMapper.class, mapper -> mapper.softDeleteUserRoleRelByUserId(userId));
          SessionUtils.doWithoutCommit(
              OwnerMetaMapper.class,
              mapper ->
                  mapper.softDeleteOwnerRelByOwnerIdAndType(userId, Entity.EntityType.USER.name()));
        });
    return deletedUser.get();
  }

  @Monitored(
      metricsSource = GRAVITINO_RELATIONAL_STORE_METRIC_NAME,
      baseMetricName = "countUsersByMetalake")
  public long countUsersByMetalake(String metalakeName) {
    Long count =
        SessionUtils.getWithoutCommit(
            UserMetaMapper.class, mapper -> mapper.countUserMetasByMetalakeName(metalakeName));
    return count == null ? 0L : count;
  }

  @Monitored(
      metricsSource = GRAVITINO_RELATIONAL_STORE_METRIC_NAME,
      baseMetricName = "listUsersByMetalakePaginated")
  public PagedResult<UserEntity> listUsersByMetalakePaginated(
      String metalakeName, int offset, int limit) {
    Preconditions.checkArgument(offset >= 0, "offset must be >= 0");
    Preconditions.checkArgument(limit >= 0, "limit must be >= 0");

    long totalCount = countUsersByMetalake(metalakeName);
    if (limit == 0 || offset >= totalCount) {
      return new PagedResult<>(totalCount, Collections.emptyList());
    }

    List<ExtendedUserPO> userPOs =
        SessionUtils.getWithoutCommit(
            UserMetaMapper.class,
            mapper ->
                mapper.listExtendedUserPOsByMetalakeNamePaginated(metalakeName, offset, limit));
    List<UserEntity> users =
        userPOs.stream()
            .map(
                po ->
                    POConverters.fromExtendedUserPO(
                        po, AuthorizationUtils.ofUserNamespace(metalakeName)))
            .collect(Collectors.toList());
    return new PagedResult<>(totalCount, users);
  }

  /**
   * Holds the parent metalake row for the rest of the transaction, so the user cannot be created
   * under a metalake that is going away.
   *
   * <p>The lock is shared, not exclusive: many users can be created under the same metalake at the
   * same time. Dropping a metalake takes an exclusive lock on this row, so a drop and a create
   * cannot overlap. Whoever gets the row first wins, and the loser either sees the metalake gone or
   * inserts under a metalake that is still there.
   *
   * <p>The name is compared again because the ID alone cannot tell a rename apart: the caller
   * looked the metalake up by name, so a renamed row means the name in the request no longer
   * exists.
   *
   * <p>The metalake's version is deliberately not compared, matching {@code CatalogMetaService}.
   * Holding the row is what makes the create safe. An unrelated metalake edit that commits in
   * between bumps the version without making this create wrong, so comparing it would reject the
   * create for no reason.
   */
  private void lockMetalakeForUserCreate(MetalakePO observedMetalakePO) {
    MetalakePO currentMetalakePO =
        SessionUtils.getWithoutCommit(
            MetalakeMetaMapper.class,
            mapper -> mapper.selectMetalakeMetaByIdForShare(observedMetalakePO.getMetalakeId()));
    if (currentMetalakePO == null
        || !Objects.equals(
            currentMetalakePO.getMetalakeName(), observedMetalakePO.getMetalakeName())) {
      throw new NoSuchEntityException(
          NoSuchEntityException.NO_SUCH_ENTITY_MESSAGE,
          Entity.EntityType.METALAKE.name().toLowerCase(),
          observedMetalakePO.getMetalakeName());
    }
  }

  private RuntimeException userWriteFailure(
      NameIdentifier identifier, UserPO observedUserPO, UserLookup lookup) {
    // Sessions run at READ_COMMITTED, so a plain read would already see the latest committed row.
    // The locking read additionally waits for a writer that is still in flight, so a rename or
    // delete that has not committed yet is classified as not-found instead of as a stale-version
    // conflict. The lock is taken on the error path of a transaction that is about to roll back.
    UserPO currentUserPO = getUserPOByIdForUpdate(observedUserPO.getUserId());
    boolean missing =
        currentUserPO == null
            || !Objects.equals(currentUserPO.getMetalakeId(), observedUserPO.getMetalakeId());
    if (!missing && lookup == UserLookup.NAME) {
      missing = !Objects.equals(currentUserPO.getUserName(), observedUserPO.getUserName());
    } else if (!missing && lookup == UserLookup.EXTERNAL_ID) {
      missing = !Objects.equals(currentUserPO.getExternalId(), observedUserPO.getExternalId());
    }
    if (missing) {
      return new NoSuchEntityException(
          NoSuchEntityException.NO_SUCH_ENTITY_MESSAGE,
          Entity.EntityType.USER.name().toLowerCase(),
          identifier.name());
    }
    return ExceptionUtils.concurrentModification(Entity.EntityType.USER, identifier);
  }

  private UserPO getUserPOByIdForUpdate(long userId) {
    return SessionUtils.getWithoutCommit(
        UserMetaMapper.class, mapper -> mapper.selectUserMetaByIdForUpdate(userId));
  }

  /**
   * How the caller addressed the user, which decides what counts as "the same user" when a failed
   * compare-and-set is classified. A caller that used the name is looking for that name, so a
   * rename means the user it asked for is gone; the same holds for the external ID. A caller that
   * used the ID addressed the row itself, so a rename leaves it addressing the same user and only
   * the metalake has to still match.
   */
  private enum UserLookup {
    NAME,
    EXTERNAL_ID,
    ID
  }
}
