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
package org.apache.gravitino.dto.authorization;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.authorization.Privilege;
import org.apache.gravitino.authorization.Privileges;

/** Data transfer object representing a privilege. */
public class PrivilegeDTO implements Privilege {

  @JsonProperty("name")
  private Name name;

  @JsonProperty("condition")
  private Condition condition;

  /** Default constructor for Jackson deserialization. */
  protected PrivilegeDTO() {}

  /**
   * Creates a new instance of PrivilegeDTO.
   *
   * @param name The name of the Privilege DTO.
   * @param condition The condition of the Privilege DTO.
   */
  protected PrivilegeDTO(Name name, Condition condition) {
    this.name = name;
    this.condition = condition;
  }

  @Override
  public Name name() {
    return name;
  }

  @Override
  public String simpleString() {
    if (Condition.ALLOW.equals(condition)) {
      return Privileges.allow(name).simpleString();
    } else {
      return Privileges.deny(name).simpleString();
    }
  }

  @Override
  public Condition condition() {
    return condition;
  }

  @Override
  public boolean canBindTo(MetadataObject.Type type) {
    if (Condition.ALLOW.equals(condition)) {
      return Privileges.allow(name).canBindTo(type);
    } else {
      return Privileges.deny(name).canBindTo(type);
    }
  }

  /**
   * @return the builder for creating a new instance of PrivilegeDTO.
   */
  public static Builder builder() {
    return new Builder();
  }

  /** Builder for {@link PrivilegeDTO}. */
  public static class Builder {

    private Name name;
    private Condition condition;

    /**
     * Sets the name of the privilege.
     *
     * @param name The name of the privilege.
     * @return The builder instance.
     */
    public Builder withName(Name name) {
      this.name = name;
      return this;
    }

    /**
     * Sets the condition of the privilege.
     *
     * @param condition The condition of the privilege.
     * @return The builder instance.
     */
    public Builder withCondition(Condition condition) {
      this.condition = condition;
      return this;
    }

    /**
     * Builds an instance of PrivilegeDTO using the builder's properties.
     *
     * @return An instance of PrivilegeDTO.
     * @throws IllegalArgumentException If the name or condition are not set.
     */
    public PrivilegeDTO build() {
      Preconditions.checkArgument(name != null, "name cannot be null");
      Preconditions.checkArgument(condition != null, "condition cannot be null");
      return new PrivilegeDTO(name, condition);
    }
  }
}
