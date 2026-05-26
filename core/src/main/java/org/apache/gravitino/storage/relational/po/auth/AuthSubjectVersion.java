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
 * One row of the batched auth-subject probe. The {@link Kind} discriminator distinguishes the row's
 * kind; the {@code id}, {@code name}, {@code updatedAt} and {@code parentId} fields are interpreted
 * accordingly:
 *
 * <ul>
 *   <li>{@link Kind#USER}: {@code id} = {@code user_id}, {@code name} = {@code user_name}, {@code
 *       updatedAt} = {@code user_meta.updated_at}, {@code parentId} = {@code null}.
 *   <li>{@link Kind#GROUP}: {@code id} = {@code group_id}, {@code name} = {@code group_name},
 *       {@code updatedAt} = {@code group_meta.updated_at}, {@code parentId} = {@code null}.
 *   <li>{@link Kind#USER_ROLE}: {@code id} = {@code role_id}, {@code name} = {@code role_name},
 *       {@code updatedAt} = {@code role_meta.updated_at}, {@code parentId} = the owning {@code
 *       user_id}.
 *   <li>{@link Kind#GROUP_ROLE}: {@code id} = {@code role_id}, {@code name} = {@code role_name},
 *       {@code updatedAt} = {@code role_meta.updated_at}, {@code parentId} = the owning {@code
 *       group_id}.
 * </ul>
 */
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
public class AuthSubjectVersion {

  /** Discriminates the row's underlying subject kind. */
  public enum Kind {
    /** {@code user_meta} row. */
    USER,
    /** {@code group_meta} row. */
    GROUP,
    /** {@code user_role_rel} JOIN {@code role_meta} row; {@code parentId} is the owning user. */
    USER_ROLE,
    /** {@code group_role_rel} JOIN {@code role_meta} row; {@code parentId} is the owning group. */
    GROUP_ROLE
  }

  /** Row kind discriminator; populated from the SQL literal in the UNION branches. */
  private Kind subjectType;

  /** {@code user_id}, {@code group_id}, or {@code role_id}. */
  private long id;

  /** {@code user_name}, {@code group_name}, or {@code role_name}. */
  private String name;

  /** {@code updated_at} value used as the cache staleness sentinel. */
  private long updatedAt;

  /**
   * Owning subject id for role rows: {@code user_id} for {@link Kind#USER_ROLE} and {@code
   * group_id} for {@link Kind#GROUP_ROLE}; {@code null} for {@link Kind#USER} / {@link Kind#GROUP}
   * rows.
   */
  private Long parentId;
}
