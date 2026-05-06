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

/** Represents a built-in IdP group Data Transfer Object (DTO). */
@EqualsAndHashCode
public class IdpGroupDTO {

  @JsonProperty("name")
  private String name;

  @JsonProperty("users")
  private List<String> users;

  @JsonProperty("audit")
  private AuditDTO audit;

  /** Default constructor for Jackson deserialization. */
  protected IdpGroupDTO() {
    this.users = Collections.emptyList();
  }

  /**
   * Creates a new instance of IdpGroupDTO.
   *
   * @param name The name of the built-in IdP group DTO.
   * @param users The users of the built-in IdP group DTO.
   * @param audit The audit of the built-in IdP group DTO.
   */
  protected IdpGroupDTO(String name, List<String> users, AuditDTO audit) {
    this.name = name;
    this.users = users;
    this.audit = audit;
  }

  /**
   * @return The name of the built-in IdP group DTO.
   */
  public String name() {
    return name;
  }

  /**
   * The users of the built-in IdP group. A group can contain multiple users.
   *
   * @return The users of the built-in IdP group.
   */
  public List<String> users() {
    return users;
  }

  /**
   * @return The audit of the built-in IdP group DTO.
   */
  public AuditDTO auditInfo() {
    return audit;
  }

  /**
   * Creates a new Builder for constructing a built-in IdP group DTO.
   *
   * @return A new Builder instance.
   */
  public static Builder builder() {
    return new Builder();
  }

  /**
   * Builder class for constructing an IdpGroupDTO instance.
   *
   * @param <S> The type of the builder instance.
   */
  public static class Builder<S extends Builder> {

    /** The name of the built-in IdP group. */
    protected String name;

    /** The users of the built-in IdP group. */
    protected List<String> users = Collections.emptyList();

    /** The audit of the built-in IdP group. */
    protected AuditDTO audit;

    /**
     * Sets the name of the built-in IdP group.
     *
     * @param name The name of the built-in IdP group.
     * @return The builder instance.
     */
    public S withName(String name) {
      this.name = name;
      return (S) this;
    }

    /**
     * Sets the users of the built-in IdP group.
     *
     * @param users The users of the built-in IdP group.
     * @return The builder instance.
     */
    public S withUsers(List<String> users) {
      if (users != null) {
        this.users = users;
      }

      return (S) this;
    }

    /**
     * Sets the audit of the built-in IdP group.
     *
     * @param audit The audit of the built-in IdP group.
     * @return The builder instance.
     */
    public S withAudit(AuditDTO audit) {
      this.audit = audit;
      return (S) this;
    }

    /**
     * Builds an instance of IdpGroupDTO using the builder's properties.
     *
     * @return An instance of IdpGroupDTO.
     * @throws IllegalArgumentException If the name or audit is not set.
     */
    public IdpGroupDTO build() {
      Preconditions.checkArgument(StringUtils.isNotBlank(name), "name cannot be null or empty");
      Preconditions.checkArgument(audit != null, "audit cannot be null");
      return new IdpGroupDTO(name, users, audit);
    }
  }
}
