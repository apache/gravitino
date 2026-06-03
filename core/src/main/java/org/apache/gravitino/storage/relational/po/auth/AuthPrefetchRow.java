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
package org.apache.gravitino.storage.relational.po.auth;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

/**
 * Polymorphic result row returned by {@code UserMetaMapper#batchGetAuthSubjectsForUser}. That
 * mapper folds four logically distinct fetches (the request user, the user's groups, the user's
 * direct roles, the user's group-inherited roles) into a single round trip via {@code UNION ALL},
 * so its result set is a flat list of rows whose meaning depends on a discriminator. This class is
 * that row.
 *
 * <p>Each row carries the same physical columns ({@code subjectType}, {@code entityId}, {@code
 * entityName}, {@code updatedAt}, {@code bindingOwnerId}), but the semantics of those columns shift
 * by {@link Kind}:
 *
 * <table>
 *   <caption>Per-Kind field semantics</caption>
 *   <tr>
 *     <th>{@link Kind}</th>
 *     <th>{@code entityId}</th>
 *     <th>{@code entityName}</th>
 *     <th>{@code updatedAt}</th>
 *     <th>{@code bindingOwnerId}</th>
 *   </tr>
 *   <tr>
 *     <td>{@link Kind#USER}</td>
 *     <td>{@code user_meta.user_id}</td>
 *     <td>{@code user_meta.user_name}</td>
 *     <td>{@code user_meta.updated_at}</td>
 *     <td>{@code null}</td>
 *   </tr>
 *   <tr>
 *     <td>{@link Kind#GROUP}</td>
 *     <td>{@code group_meta.group_id}</td>
 *     <td>{@code group_meta.group_name}</td>
 *     <td>{@code group_meta.updated_at}</td>
 *     <td>{@code null}</td>
 *   </tr>
 *   <tr>
 *     <td>{@link Kind#USER_ROLE}</td>
 *     <td>{@code role_meta.role_id}</td>
 *     <td>{@code role_meta.role_name}</td>
 *     <td>{@code role_meta.updated_at}</td>
 *     <td>owning {@code user_id}</td>
 *   </tr>
 *   <tr>
 *     <td>{@link Kind#GROUP_ROLE}</td>
 *     <td>{@code role_meta.role_id}</td>
 *     <td>{@code role_meta.role_name}</td>
 *     <td>{@code role_meta.updated_at}</td>
 *     <td>owning {@code group_id}</td>
 *   </tr>
 * </table>
 *
 * <p>The four kinds mirror the four {@code UNION ALL} branches one-for-one; the discriminator is
 * not an arbitrary taxonomy. The consumer ({@code JcasbinAuthorizer#prefetchUserAndGroupInfo})
 * switches on {@link Kind} and steers each row into the matching collection (single user, group
 * map, user role-id set, group role-ids-by-group-id map).
 *
 * <p>Why a polymorphic flat row instead of separate POs:
 *
 * <ul>
 *   <li>{@code UNION ALL} requires all branches to share the same column count and types, so the
 *       physical row shape is necessarily uniform.
 *   <li>Splitting into separate per-kind POs would mean separate SQL queries — losing the single
 *       round trip that is the whole point of this method.
 * </ul>
 *
 * <p>Prefer the {@code forXxx} factory methods over the all-args constructor when building rows in
 * Java code (tests, ad-hoc fixtures); they document which {@link Kind} each shape corresponds to
 * and avoid mistakes such as passing {@code null} as {@code bindingOwnerId} for a {@link
 * Kind#USER_ROLE} row. The all-args constructor is kept public for MyBatis reflective row mapping.
 */
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
public class AuthPrefetchRow {

  /**
   * Discriminator that determines how the other fields are interpreted. Each value maps 1:1 to a
   * branch of the {@code UNION ALL} in {@code UserMetaBaseSQLProvider#batchGetAuthSubjectsForUser}.
   * See class-level Javadoc for the full per-Kind field table.
   */
  public enum Kind {
    /** A {@code user_meta} row (the request user). {@code bindingOwnerId} is {@code null}. */
    USER,
    /**
     * A {@code group_meta} row (one of the request user's groups). {@code bindingOwnerId} is {@code
     * null}.
     */
    GROUP,
    /**
     * A {@code user_role_rel JOIN role_meta} row: a role bound directly to the request user. {@code
     * entityId} is the {@code role_id}; {@code bindingOwnerId} is the owning {@code user_id}.
     */
    USER_ROLE,
    /**
     * A {@code group_role_rel JOIN role_meta} row: a role inherited via group membership. {@code
     * entityId} is the {@code role_id}; {@code bindingOwnerId} is the owning {@code group_id}.
     */
    GROUP_ROLE
  }

  /**
   * Discriminator field. Populated from the SQL string literal in each {@code UNION ALL} branch
   * ({@code 'USER'}, {@code 'GROUP'}, {@code 'USER_ROLE'}, {@code 'GROUP_ROLE'}). Drives how the
   * other fields are interpreted by consumers.
   */
  private Kind subjectType;

  /**
   * Primary entity id for this row. Interpretation depends on {@link #subjectType}:
   *
   * <ul>
   *   <li>{@link Kind#USER} → {@code user_meta.user_id}
   *   <li>{@link Kind#GROUP} → {@code group_meta.group_id}
   *   <li>{@link Kind#USER_ROLE}, {@link Kind#GROUP_ROLE} → {@code role_meta.role_id}
   * </ul>
   */
  private long entityId;

  /**
   * Primary entity name. Interpretation depends on {@link #subjectType}:
   *
   * <ul>
   *   <li>{@link Kind#USER} → {@code user_name}
   *   <li>{@link Kind#GROUP} → {@code group_name}
   *   <li>{@link Kind#USER_ROLE}, {@link Kind#GROUP_ROLE} → {@code role_name}
   * </ul>
   */
  private String entityName;

  /**
   * {@code updated_at} of the row's primary entity ({@code user_meta} / {@code group_meta} / {@code
   * role_meta}). Used as the cache staleness sentinel by the version-validated caches in {@code
   * JcasbinAuthorizer}.
   */
  private long updatedAt;

  /**
   * Owning subject id for role-binding rows; {@code null} for identity rows:
   *
   * <ul>
   *   <li>{@link Kind#USER_ROLE} → the {@code user_id} the role is directly bound to
   *   <li>{@link Kind#GROUP_ROLE} → the {@code group_id} the role is inherited from
   *   <li>{@link Kind#USER}, {@link Kind#GROUP} → {@code null}
   * </ul>
   *
   * Consumers use this to bucket role rows back onto the owning user/group when populating the
   * downstream per-subject role caches.
   */
  private Long bindingOwnerId;

  /**
   * Builds a {@link Kind#USER} row.
   *
   * @param userId {@code user_meta.user_id}
   * @param userName {@code user_meta.user_name}
   * @param updatedAt {@code user_meta.updated_at}
   * @return a populated row
   */
  public static AuthPrefetchRow forUser(long userId, String userName, long updatedAt) {
    return new AuthPrefetchRow(Kind.USER, userId, userName, updatedAt, null);
  }

  /**
   * Builds a {@link Kind#GROUP} row.
   *
   * @param groupId {@code group_meta.group_id}
   * @param groupName {@code group_meta.group_name}
   * @param updatedAt {@code group_meta.updated_at}
   * @return a populated row
   */
  public static AuthPrefetchRow forGroup(long groupId, String groupName, long updatedAt) {
    return new AuthPrefetchRow(Kind.GROUP, groupId, groupName, updatedAt, null);
  }

  /**
   * Builds a {@link Kind#USER_ROLE} row bound to {@code ownerUserId}.
   *
   * @param roleId {@code role_meta.role_id}
   * @param roleName {@code role_meta.role_name}
   * @param updatedAt {@code role_meta.updated_at}
   * @param ownerUserId the {@code user_id} that owns this direct role binding
   * @return a populated row
   */
  public static AuthPrefetchRow forUserRole(
      long roleId, String roleName, long updatedAt, long ownerUserId) {
    return new AuthPrefetchRow(Kind.USER_ROLE, roleId, roleName, updatedAt, ownerUserId);
  }

  /**
   * Builds a {@link Kind#GROUP_ROLE} row inherited from {@code ownerGroupId}.
   *
   * @param roleId {@code role_meta.role_id}
   * @param roleName {@code role_meta.role_name}
   * @param updatedAt {@code role_meta.updated_at}
   * @param ownerGroupId the {@code group_id} that owns this inherited role binding
   * @return a populated row
   */
  public static AuthPrefetchRow forGroupRole(
      long roleId, String roleName, long updatedAt, long ownerGroupId) {
    return new AuthPrefetchRow(Kind.GROUP_ROLE, roleId, roleName, updatedAt, ownerGroupId);
  }
}
