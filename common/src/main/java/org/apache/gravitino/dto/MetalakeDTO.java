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
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import java.util.Map;
import javax.annotation.Nullable;
import lombok.ToString;
import org.apache.gravitino.Audit;
import org.apache.gravitino.Metalake;

/** Represents a Metalake Data Transfer Object (DTO) that implements the Metalake interface. */
@ToString
public class MetalakeDTO implements Metalake {

  @JsonProperty("name")
  private String name;

  @Nullable
  @JsonProperty("comment")
  private String comment;

  @Nullable
  @JsonProperty("properties")
  private Map<String, String> properties;

  @JsonProperty("audit")
  private AuditDTO audit;

  /** Default constructor for Jackson deserialization. */
  protected MetalakeDTO() {}

  /**
   * Creates a new instance of MetalakeDTO.
   *
   * @param name The name of the Metalake DTO.
   * @param comment The comment of the Metalake DTO.
   * @param properties The properties of the Metalake DTO.
   * @param audit The audit information of the Metalake DTO.
   */
  protected MetalakeDTO(
      String name, String comment, Map<String, String> properties, AuditDTO audit) {
    this.name = name;
    this.comment = comment;
    this.properties = properties;
    this.audit = audit;
  }

  /** @return The name of the Metalake DTO. */
  @Override
  public String name() {
    return name;
  }

  /** @return The comment of the Metalake DTO. */
  @Override
  public String comment() {
    return comment;
  }

  /** @return The properties of the Metalake DTO. */
  @Override
  public Map<String, String> properties() {
    return properties;
  }

  /** @return The audit information of the Metalake DTO. */
  @Override
  public Audit auditInfo() {
    return audit;
  }

  /**
   * A builder class for constructing instances of MetalakeDTO.
   *
   * @param <S> The type of the builder subclass.
   */
  public static class Builder<S extends Builder> {

    /** The name of the Metalake DTO. */
    protected String name;

    /** The comment of the Metalake DTO. */
    protected String comment;

    /** The properties of the Metalake DTO. */
    protected Map<String, String> properties;

    /** The audit information of the Metalake DTO. */
    protected AuditDTO audit;

    /** Default constructor. */
    protected Builder() {}

    /**
     * Sets the name of the Metalake DTO.
     *
     * @param name The name of the Metalake DTO.
     * @return The builder instance.
     */
    public S withName(String name) {
      this.name = name;
      return (S) this;
    }

    /**
     * Sets the comment of the Metalake DTO.
     *
     * @param comment The comment of the Metalake DTO.
     * @return The builder instance.
     */
    public S withComment(String comment) {
      this.comment = comment;
      return (S) this;
    }

    /**
     * Sets the properties of the Metalake DTO.
     *
     * @param properties The properties of the Metalake DTO.
     * @return The builder instance.
     */
    public S withProperties(Map<String, String> properties) {
      this.properties = properties;
      return (S) this;
    }

    /**
     * Sets the audit information of the Metalake DTO.
     *
     * @param audit The audit information of the Metalake DTO.
     * @return The builder instance.
     */
    public S withAudit(AuditDTO audit) {
      this.audit = audit;
      return (S) this;
    }

    /**
     * Builds an instance of MetalakeDTO using the builder's properties.
     *
     * @return An instance of MetalakeDTO.
     * @throws IllegalArgumentException If the name or audit are not set.
     */
    public MetalakeDTO build() {
      Preconditions.checkArgument(name != null && !name.isEmpty(), "name cannot be null or empty");
      Preconditions.checkArgument(audit != null, "audit cannot be null");
      return new MetalakeDTO(name, comment, properties, audit);
    }
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof MetalakeDTO)) {
      return false;
    }
    MetalakeDTO that = (MetalakeDTO) o;
    return Objects.equal(name, that.name)
        && Objects.equal(comment, that.comment)
        && propertyEqual(properties, that.properties)
        && Objects.equal(audit, that.audit);
  }

  private boolean propertyEqual(Map<String, String> p1, Map<String, String> p2) {
    if (p1 == null && p2 == null) {
      return true;
    }

    if (p1 != null && p1.isEmpty() && p2 == null) {
      return true;
    }

    if (p2 != null && p2.isEmpty() && p1 == null) {
      return true;
    }

    return java.util.Objects.equals(p1, p2);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(name, comment, audit, properties);
  }

  /** @return the builder for creating a new instance of MetalakeDTO. */
  public static Builder builder() {
    return new Builder();
  }
}
