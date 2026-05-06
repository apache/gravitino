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

package org.apache.gravitino.dto;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import java.util.Collections;
import java.util.List;
import lombok.EqualsAndHashCode;
import org.apache.commons.lang3.StringUtils;

/** Represents a built-in IdP user Data Transfer Object (DTO). */
@EqualsAndHashCode
public class IdpUserDTO {

  @JsonProperty("name")
  private String name;

  @JsonProperty("groups")
  private List<String> groups;

  @JsonProperty("audit")
  private AuditDTO audit;

  /** Default constructor for Jackson deserialization. */
  protected IdpUserDTO() {
    this.groups = Collections.emptyList();
  }

  /**
   * Creates a new instance of IdpUserDTO.
   *
   * @param name The name of the built-in IdP user DTO.
   * @param groups The groups of the built-in IdP user DTO.
   * @param audit The audit of the built-in IdP user DTO.
   */
  protected IdpUserDTO(String name, List<String> groups, AuditDTO audit) {
    this.name = name;
    this.groups = groups;
    this.audit = audit;
  }

  /**
   * @return The name of the built-in IdP user DTO.
   */
  public String name() {
    return name;
  }

  /**
   * The groups of the built-in IdP user. A user can belong to multiple groups.
   *
   * @return The groups of the built-in IdP user.
   */
  public List<String> groups() {
    return groups;
  }

  /**
   * @return The audit of the built-in IdP user DTO.
   */
  public AuditDTO auditInfo() {
    return audit;
  }

  /**
   * Creates a new Builder for constructing a built-in IdP user DTO.
   *
   * @return A new Builder instance.
   */
  public static Builder builder() {
    return new Builder();
  }

  /**
   * Builder class for constructing an IdpUserDTO instance.
   *
   * @param <S> The type of the builder instance.
   */
  public static class Builder<S extends Builder> {

    /** The name of the built-in IdP user. */
    protected String name;

    /** The groups of the built-in IdP user. */
    protected List<String> groups = Collections.emptyList();

    /** The audit information of the built-in IdP user. */
    protected AuditDTO audit;

    /**
     * Sets the name of the built-in IdP user.
     *
     * @param name The name of the built-in IdP user.
     * @return The builder instance.
     */
    public S withName(String name) {
      this.name = name;
      return (S) this;
    }

    /**
     * Sets the groups of the built-in IdP user.
     *
     * @param groups The groups of the built-in IdP user.
     * @return The builder instance.
     */
    public S withGroups(List<String> groups) {
      if (groups != null) {
        this.groups = groups;
      }

      return (S) this;
    }

    /**
     * Sets the audit of the built-in IdP user.
     *
     * @param audit The audit of the built-in IdP user.
     * @return The builder instance.
     */
    public S withAudit(AuditDTO audit) {
      this.audit = audit;
      return (S) this;
    }

    /**
     * Builds an instance of IdpUserDTO using the builder's properties.
     *
     * @return An instance of IdpUserDTO.
     * @throws IllegalArgumentException If the name or audit is not set.
     */
    public IdpUserDTO build() {
      Preconditions.checkArgument(StringUtils.isNotBlank(name), "name cannot be null or empty");
      Preconditions.checkArgument(audit != null, "audit cannot be null");
      return new IdpUserDTO(name, groups, audit);
    }
  }
}
