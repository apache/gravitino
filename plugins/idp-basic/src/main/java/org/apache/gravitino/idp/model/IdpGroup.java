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
package org.apache.gravitino.idp.model;

import java.util.List;
import java.util.Objects;
import org.apache.gravitino.Audit;
import org.apache.gravitino.Auditable;
import org.apache.gravitino.dto.util.DTOConverters;
import org.apache.gravitino.idp.dto.IdpGroupDTO;
import org.apache.gravitino.meta.AuditInfo;

/** Built-in IdP group. */
public class IdpGroup implements Auditable {

  private final String name;
  private final String comment;
  private final List<String> usernames;
  private final AuditInfo auditInfo;

  /**
   * Creates a built-in IdP group.
   *
   * @param name The group name.
   * @param usernames The usernames in the group.
   */
  public IdpGroup(String name, List<String> usernames) {
    this(name, usernames, "", AuditInfo.EMPTY);
  }

  /**
   * Creates a built-in IdP group.
   *
   * @param name The group name.
   * @param usernames The usernames in the group.
   * @param comment The group comment, or empty if none.
   */
  public IdpGroup(String name, List<String> usernames, String comment) {
    this(name, usernames, comment, AuditInfo.EMPTY);
  }

  /**
   * Creates a built-in IdP group.
   *
   * @param name The group name.
   * @param usernames The usernames in the group.
   * @param comment The group comment, or empty if none.
   * @param auditInfo Audit information.
   */
  public IdpGroup(String name, List<String> usernames, String comment, AuditInfo auditInfo) {
    this.name = name;
    this.usernames = usernames;
    this.comment = comment == null ? "" : comment;
    this.auditInfo = auditInfo == null ? AuditInfo.EMPTY : auditInfo;
  }

  /** Returns the group name. */
  public String name() {
    return name;
  }

  /** Returns the group comment, or an empty string if none. */
  public String comment() {
    return comment;
  }

  /** Returns the usernames in the group. */
  public List<String> usernames() {
    return usernames;
  }

  @Override
  public Audit auditInfo() {
    return auditInfo;
  }

  /**
   * Converts this group to a REST DTO.
   *
   * @return the group DTO
   */
  public IdpGroupDTO toDTO() {
    return IdpGroupDTO.builder()
        .withName(name)
        .withComment(comment)
        .withUsers(usernames)
        .withAudit(DTOConverters.toDTO(auditInfo))
        .build();
  }

  @Override
  public boolean equals(Object other) {
    if (this == other) {
      return true;
    }
    if (!(other instanceof IdpGroup)) {
      return false;
    }
    IdpGroup that = (IdpGroup) other;
    return Objects.equals(name, that.name)
        && Objects.equals(comment, that.comment)
        && Objects.equals(usernames, that.usernames)
        && Objects.equals(auditInfo, that.auditInfo);
  }

  @Override
  public int hashCode() {
    return Objects.hash(name, comment, usernames, auditInfo);
  }

  @Override
  public String toString() {
    return "IdpGroup{name='"
        + name
        + "', comment='"
        + comment
        + "', usernames="
        + usernames
        + ", auditInfo="
        + auditInfo
        + '}';
  }
}
