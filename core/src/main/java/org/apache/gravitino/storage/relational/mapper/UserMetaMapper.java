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
import org.apache.gravitino.storage.relational.po.auth.AuthPrefetchRow;
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

  @SelectProvider(
      type = UserMetaSQLProviderFactory.class,
      method = "selectUserMetaByMetalakeNameAndExternalId")
  UserPO selectUserMetaByMetalakeNameAndExternalId(
      @Param("metalakeName") String metalakeName, @Param("externalId") String externalId);

  @UpdateProvider(type = UserMetaSQLProviderFactory.class, method = "updateUserMetaByExternalId")
  Integer updateUserMetaByExternalId(
      @Param("newUserMeta") UserPO newUserPO, @Param("oldUserMeta") UserPO oldUserPO);

  /**
   * Single-round-trip auth prefetch for the JCasbin authorize hot path. Returns every version
   * sentinel the request needs:
   *
   * <ul>
   *   <li>the request user's {@code user_meta} row,
   *   <li>each requested group's {@code group_meta} row,
   *   <li>the user's direct {@code user_role_rel} role ids (joined to {@code role_meta} for their
   *       {@code updated_at}),
   *   <li>the group-inherited {@code group_role_rel} role ids (also joined for versions).
   * </ul>
   *
   * <p>Implementation is up to four {@code UNION ALL} branches that map 1:1 to {@link
   * AuthPrefetchRow.Kind} values — see {@link AuthPrefetchRow}'s class-level Javadoc for the row
   * shape and how its fields are interpreted per Kind. Consumers switch on {@code subjectType} and
   * bucket each row into the matching collection.
   *
   * <p>Empty {@code groupNames} skips the {@code GROUP} and {@code GROUP_ROLE} branches so the
   * query degrades to a 2-branch UNION ({@code USER} + {@code USER_ROLE}) for users without group
   * membership.
   *
   * @param metalakeName the metalake the user and groups belong to
   * @param userName the user to probe
   * @param groupNames the group names to probe; may be empty (never null)
   * @return polymorphic rows for the user, each present group, the user's direct roles, and each
   *     group's inherited roles
   */
  @SelectProvider(type = UserMetaSQLProviderFactory.class, method = "batchGetAuthSubjectsForUser")
  List<AuthPrefetchRow> batchGetAuthSubjectsForUser(
      @Param("metalakeName") String metalakeName,
      @Param("userName") String userName,
      @Param("groupNames") List<String> groupNames);
}
