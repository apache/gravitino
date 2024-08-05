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
import org.apache.gravitino.authorization.Owner;

/** Represents an Owner Data Transfer Object (DTO). */
public class OwnerDTO implements Owner {
  @JsonProperty("name")
  private String name;

  @JsonProperty("type")
  private Type type;

  private OwnerDTO() {}

  @Override
  public String name() {
    return name;
  }

  @Override
  public Type type() {
    return type;
  }

  /**
   * Creates a new Builder for constructing an Owner DTO.
   *
   * @return A new Builder instance.
   */
  public static Builder builder() {
    return new Builder();
  }

  /** Builder class for constructing OwnerDTO instances. */
  public static class Builder {

    private final OwnerDTO ownerDTO;

    private Builder() {
      ownerDTO = new OwnerDTO();
    }

    /**
     * Sets the name for the owner.
     *
     * @param name The name of the owner.
     * @return The builder instance.
     */
    public Builder withName(String name) {
      ownerDTO.name = name;
      return this;
    }

    /**
     * Sets the type for the owner.
     *
     * @param type The type of the owner.
     * @return The builder instance.
     */
    public Builder withType(Type type) {
      ownerDTO.type = type;
      return this;
    }

    /**
     * Builds an instance of OwnerDTO using the builder's properties.
     *
     * @return An instance of OwnerDTO.
     */
    public OwnerDTO build() {
      return ownerDTO;
    }
  }
}
