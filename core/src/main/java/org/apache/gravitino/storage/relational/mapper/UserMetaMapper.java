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

package org.apache.gravitino.storage.relational.mapper;

import java.util.List;
import org.apache.gravitino.storage.relational.po.ExtendedUserPO;
import org.apache.gravitino.storage.relational.po.UserPO;
import org.apache.gravitino.storage.relational.po.auth.AuthSubjectVersion;
import org.apache.gravitino.storage.relational.po.auth.UserUpdatedAt;
import org.apache.ibatis.annotations.DeleteProvider;
import org.apache.ibatis.annotations.InsertProvider;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.SelectProvider;
import org.apache.ibatis.annotations.UpdateProvider;

/**
 * A MyBatis Mapper for table meta operation SQLs.
 *
 * <p>This interface class is a specification defined by MyBatis. It requires this interface class
 * to identify the corresponding SQLs for execution. We can write SQLs in an additional XML file, or
 * write SQLs with annotations in this interface Mapper. See: <a
 * href="https://mybatis.org/mybatis-3/getting-started.html"></a>
 */
public interface UserMetaMapper {
  String USER_TABLE_NAME = "user_meta";
  String USER_ROLE_RELATION_TABLE_NAME = "user_role_rel";

  @SelectProvider(
      type = UserMetaSQLProviderFactory.class,
      method = "selectUserIdByMetalakeIdAndName")
  Long selectUserIdByMetalakeIdAndName(
      @Param("metalakeId") Long metalakeId, @Param("userName") String name);

  @SelectProvider(
      type = UserMetaSQLProviderFactory.class,
      method = "selectUserMetaByMetalakeIdAndName")
  UserPO selectUserMetaByMetalakeIdAndName(
      @Param("metalakeId") Long metalakeId, @Param("userName") String name);

  @InsertProvider(type = UserMetaSQLProviderFactory.class, method = "insertUserMeta")
  void insertUserMeta(@Param("userMeta") UserPO userPO);

  @SelectProvider(type = UserMetaSQLProviderFactory.class, method = "listUserPOsByMetalake")
  List<UserPO> listUserPOsByMetalake(@Param("metalakeName") String metalakeName);

  @SelectProvider(
      type = UserMetaSQLProviderFactory.class,
      method = "listExtendedUserPOsByMetalakeId")
  List<ExtendedUserPO> listExtendedUserPOsByMetalakeId(@Param("metalakeId") Long metalakeId);

  @InsertProvider(
      type = UserMetaSQLProviderFactory.class,
      method = "insertUserMetaOnDuplicateKeyUpdate")
  void insertUserMetaOnDuplicateKeyUpdate(@Param("userMeta") UserPO userPO);

  @UpdateProvider(type = UserMetaSQLProviderFactory.class, method = "softDeleteUserMetaByUserId")
  void softDeleteUserMetaByUserId(@Param("userId") Long userId);

  @UpdateProvider(
      type = UserMetaSQLProviderFactory.class,
      method = "softDeleteUserMetasByMetalakeId")
  void softDeleteUserMetasByMetalakeId(@Param("metalakeId") Long metalakeId);

  @UpdateProvider(type = UserMetaSQLProviderFactory.class, method = "updateUserMeta")
  Integer updateUserMeta(
      @Param("newUserMeta") UserPO newUserPO, @Param("oldUserMeta") UserPO oldUserPO);

  @SelectProvider(type = UserMetaSQLProviderFactory.class, method = "listUsersByRoleId")
  List<UserPO> listUsersByRoleId(@Param("roleId") Long roleId);

  @DeleteProvider(
      type = UserMetaSQLProviderFactory.class,
      method = "deleteUserMetasByLegacyTimeline")
  Integer deleteUserMetasByLegacyTimeline(
      @Param("legacyTimeline") Long legacyTimeline, @Param("limit") int limit);

  @UpdateProvider(type = UserMetaSQLProviderFactory.class, method = "touchUserUpdatedAt")
  void touchUserUpdatedAt(@Param("userId") long userId);

  @SelectProvider(type = UserMetaSQLProviderFactory.class, method = "getUserUpdatedAt")
  UserUpdatedAt getUserUpdatedAt(
      @Param("metalakeName") String metalakeName, @Param("userName") String userName);

  /**
   * One-shot UNION probe that returns {@code (id, name, updated_at)} for one user and (optionally)
   * any number of groups in the same metalake. Used by the authorization hot path to collapse the
   * per-user + per-group version probes into a single round trip.
   *
   * <p>When {@code groupNames} is empty the GROUP side of the UNION is omitted entirely (the SQL
   * provider returns a user-only SELECT) so callers do not need to special-case that branch.
   *
   * @param metalakeName the metalake the user and groups belong to
   * @param userName the user to probe
   * @param groupNames the group names to probe; may be empty (never null)
   * @return one row per matching user/group; missing entities are simply absent
   */
  @SelectProvider(type = UserMetaSQLProviderFactory.class, method = "batchGetUserAndGroupUpdatedAt")
  List<AuthSubjectVersion> batchGetUserAndGroupUpdatedAt(
      @Param("metalakeName") String metalakeName,
      @Param("userName") String userName,
      @Param("groupNames") List<String> groupNames);

  /**
   * Fat UNION ALL probe that returns every version sentinel the JCasbin authorize hot path needs:
   * the user, the requested groups, the user's direct {@code user_role_rel} role ids (joined to
   * {@code role_meta} for their {@code updated_at}), and the group-inherited {@code group_role_rel}
   * role ids (also joined for versions).
   *
   * <p>Each result row's {@code subjectType} discriminator + {@code parentId} let the caller pivot
   * a flat list into a per-subject {@link AuthSubjectVersion} bucket — see that POJO's javadoc.
   *
   * <p>Empty {@code groupNames} skips the GROUP and GROUP_ROLE branches so the query degrades to a
   * 2-branch UNION (user + user_role) for users without group membership.
   *
   * <p>Replaces the sequence of {@link #batchGetUserAndGroupUpdatedAt} + {@code
   * RoleMetaMapper.batchGetRoleUpdatedAt} on the cache-warm path (1 round trip instead of 2; mean /
   * p99 measured ~37% / ~67% lower against a 3-role / 0-group fixture).
   *
   * @param metalakeName the metalake the user and groups belong to
   * @param userName the user to probe
   * @param groupNames the group names to probe; may be empty (never null)
   * @return rows for the user, each present group, the user's direct roles, and each group's
   *     inherited roles
   */
  @SelectProvider(type = UserMetaSQLProviderFactory.class, method = "batchGetAuthSubjectsForUser")
  List<AuthSubjectVersion> batchGetAuthSubjectsForUser(
      @Param("metalakeName") String metalakeName,
      @Param("userName") String userName,
      @Param("groupNames") List<String> groupNames);
}
