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
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import javax.annotation.Nullable;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.authorization.Privilege;
import org.apache.gravitino.authorization.SecurableObject;

/** Data transfer object representing a securable object. */
public class SecurableObjectDTO implements SecurableObject {

  @JsonProperty("type")
  private Type type;

  @JsonProperty("privileges")
  private PrivilegeDTO[] privileges;

  private String parent;
  private String name;

  /** Default constructor for Jackson deserialization. */
  protected SecurableObjectDTO() {}

  /**
   * @return The full name of the securable object.
   */
  @JsonProperty("fullName")
  public String getFullName() {
    return fullName();
  }

  /**
   * Sets the full name of the securable object. Only used by Jackson deserializer.
   *
   * @param fullName The full name of the metadata object.
   */
  @JsonProperty("fullName")
  public void setFullName(String fullName) {
    int index = fullName.lastIndexOf(".");
    if (index == -1) {
      parent = null;
      name = fullName;
    } else {
      parent = fullName.substring(0, index);
      name = fullName.substring(index + 1);
    }
  }

  @Nullable
  @Override
  public String parent() {
    return parent;
  }

  @Override
  public String name() {
    return name;
  }

  @Override
  public Type type() {
    return type;
  }

  @Override
  public List<Privilege> privileges() {
    if (privileges == null) {
      return Collections.emptyList();
    }

    return Collections.unmodifiableList(Arrays.asList(privileges));
  }

  /**
   * @return the builder for creating a new instance of SecurableObjectDTO.
   */
  public static Builder builder() {
    return new Builder();
  }

  /** Builder for {@link SecurableObjectDTO}. */
  public static class Builder {
    private final SecurableObjectDTO securableObjectDTO = new SecurableObjectDTO();
    private String fullName;
    private Type type;
    private PrivilegeDTO[] privileges;

    private Builder() {}

    /**
     * Sets the full name of the securable object.
     *
     * @param fullName The full name of the securable object.
     * @return The builder instance.
     */
    public Builder withFullName(String fullName) {
      this.fullName = fullName;
      return this;
    }

    /**
     * Sets the type of the securable object.
     *
     * @param type The type of the securable object.
     * @return The builder instance.
     */
    public Builder withType(Type type) {
      this.type = type;
      return this;
    }

    /**
     * Sets the privileges of the securable object.
     *
     * @param privileges The privileges of the securable object.
     * @return The builder instance.
     */
    public Builder withPrivileges(PrivilegeDTO[] privileges) {
      this.privileges = privileges;
      return this;
    }

    /**
     * Builds an instance of SecurableObjectDTO using the builder's properties.
     *
     * @return An instance of SecurableObjectDTO.
     * @throws IllegalArgumentException If the full name or type are not set.
     */
    public SecurableObjectDTO build() {
      Preconditions.checkArgument(
          StringUtils.isNotBlank(fullName), "full name cannot be null or empty");

      Preconditions.checkArgument(type != null, "type cannot be null");

      Preconditions.checkArgument(
          privileges != null && privileges.length != 0, "privileges can't be null or empty");

      securableObjectDTO.type = type;
      securableObjectDTO.privileges = privileges;
      securableObjectDTO.setFullName(fullName);

      return securableObjectDTO;
    }
  }
}
