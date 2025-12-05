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
import java.time.Instant;
import lombok.EqualsAndHashCode;
import lombok.ToString;
import org.apache.gravitino.Audit;

/** Data transfer object representing audit information. */
@EqualsAndHashCode
@ToString
public class AuditDTO implements Audit {

  @JsonProperty("creator")
  private String creator;

  @JsonProperty("createTime")
  private Instant createTime;

  @JsonProperty("lastModifier")
  private String lastModifier;

  @JsonProperty("lastModifiedTime")
  private Instant lastModifiedTime;

  private AuditDTO() {}

  private AuditDTO(
      String creator, Instant createTime, String lastModifier, Instant lastModifiedTime) {
    this.creator = creator;
    this.createTime = createTime;
    this.lastModifier = lastModifier;
    this.lastModifiedTime = lastModifiedTime;
  }

  /**
   * @return The creator of the audit.
   */
  @Override
  public String creator() {
    return creator;
  }

  /**
   * @return The create time of the audit.
   */
  @Override
  public Instant createTime() {
    return createTime;
  }

  /**
   * @return The last modifier of the audit.
   */
  @Override
  public String lastModifier() {
    return lastModifier;
  }

  /**
   * @return The last modified time of the audit.
   */
  @Override
  public Instant lastModifiedTime() {
    return lastModifiedTime;
  }

  /**
   * Creates a new Builder for constructing an Audit DTO.
   *
   * @return A new Builder instance.
   */
  public static Builder builder() {
    return new Builder();
  }

  /**
   * Builder class for constructing an AuditDTO instance.
   *
   * @param <S> The type of the builder instance.
   */
  public static class Builder<S extends Builder> {

    /** The creator of the audit. */
    protected String creator;

    /** The create time for the audit. */
    protected Instant createTime;

    /** The last modifier of the audit. */
    protected String lastModifier;

    /** The last modified time for the audit. */
    protected Instant lastModifiedTime;

    /** * Default constructor for the builder. */
    private Builder() {}

    /**
     * Sets the creator for the audit.
     *
     * @param creator The creator of the audit.
     * @return The builder instance.
     */
    public S withCreator(String creator) {
      this.creator = creator;
      return (S) this;
    }

    /**
     * Sets the create time for the audit.
     *
     * @param createTime The create time of the audit.
     * @return The builder instance.
     */
    public S withCreateTime(Instant createTime) {
      this.createTime = createTime;
      return (S) this;
    }

    /**
     * Sets who last modified the audit.
     *
     * @param lastModifier The last modifier of the audit.
     * @return The builder instance.
     */
    public S withLastModifier(String lastModifier) {
      this.lastModifier = lastModifier;
      return (S) this;
    }

    /**
     * Sets the last modified time for the audit.
     *
     * @param lastModifiedTime The last modified time of the audit.
     * @return The builder instance.
     */
    public S withLastModifiedTime(Instant lastModifiedTime) {
      this.lastModifiedTime = lastModifiedTime;
      return (S) this;
    }

    /**
     * Builds an instance of AuditDTO using the builder's properties.
     *
     * @return An instance of AuditDTO.
     */
    public AuditDTO build() {
      return new AuditDTO(creator, createTime, lastModifier, lastModifiedTime);
    }
  }
}
