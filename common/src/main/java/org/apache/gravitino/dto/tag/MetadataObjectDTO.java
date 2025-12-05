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
package org.apache.gravitino.dto.tag;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.EqualsAndHashCode;
import org.apache.gravitino.MetadataObject;

/** Represents a Metadata Object DTO (Data Transfer Object). */
@EqualsAndHashCode
public class MetadataObjectDTO implements MetadataObject {

  private String parent;

  private String name;

  @JsonProperty("type")
  private Type type;

  private MetadataObjectDTO() {}

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

  /**
   * @return The full name of the metadata object.
   */
  @JsonProperty("fullName")
  public String getFullName() {
    return fullName();
  }

  /**
   * Sets the full name of the metadata object. Only used by Jackson deserializer.
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

  /**
   * @return a new builder for constructing a Metadata Object DTO.
   */
  public static Builder builder() {
    return new Builder();
  }

  /** Builder for constructing a Metadata Object DTO. */
  public static class Builder {

    private final MetadataObjectDTO metadataObjectDTO = new MetadataObjectDTO();

    /**
     * Sets the parent of the metadata object.
     *
     * @param parent The parent of the metadata object.
     * @return The builder.
     */
    public Builder withParent(String parent) {
      metadataObjectDTO.parent = parent;
      return this;
    }

    /**
     * Sets the name of the metadata object.
     *
     * @param name The name of the metadata object.
     * @return The builder.
     */
    public Builder withName(String name) {
      metadataObjectDTO.name = name;
      return this;
    }

    /**
     * Sets the type of the metadata object.
     *
     * @param type The type of the metadata object.
     * @return The builder.
     */
    public Builder withType(Type type) {
      metadataObjectDTO.type = type;
      return this;
    }

    /**
     * @return The constructed Metadata Object DTO.
     */
    public MetadataObjectDTO build() {
      return metadataObjectDTO;
    }
  }
}
