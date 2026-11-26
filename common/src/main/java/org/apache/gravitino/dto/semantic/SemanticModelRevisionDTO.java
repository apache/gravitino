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
package org.apache.gravitino.dto.semantic;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.fasterxml.jackson.databind.ser.std.ToStringSerializer;
import java.time.Instant;
import javax.annotation.Nullable;
import lombok.EqualsAndHashCode;
import lombok.ToString;
import org.apache.gravitino.dto.AuditDTO;
import org.apache.gravitino.dto.util.DTOConverters;
import org.apache.gravitino.semantic.SemanticModel.Status;
import org.apache.gravitino.semantic.SemanticModelRevision;

/** The wire representation of a retained revision summary. */
@EqualsAndHashCode
@ToString
@JsonInclude(JsonInclude.Include.NON_NULL)
public final class SemanticModelRevisionDTO implements SemanticModelRevision {
  @JsonProperty("revision")
  private final int revision;

  @JsonProperty("status")
  @JsonSerialize(using = ToStringSerializer.class)
  private final Status status;

  @Nullable
  @JsonProperty("publishedBy")
  private final String publishedBy;

  @Nullable
  @JsonProperty("publishedAt")
  private final Instant publishedAt;

  @JsonProperty("audit")
  private final AuditDTO audit;

  /**
   * Creates revision metadata with the writer audit and optional publisher details.
   *
   * @param revision The revision number.
   * @param status The publication status.
   * @param publishedBy The publishing principal, or null for a draft.
   * @param publishedAt The publication time, or null for a draft.
   * @param audit The revision writer's audit information.
   */
  @JsonCreator
  public SemanticModelRevisionDTO(
      @JsonProperty("revision") int revision,
      @JsonProperty("status") Status status,
      @JsonProperty("publishedBy") @Nullable String publishedBy,
      @JsonProperty("publishedAt") @Nullable Instant publishedAt,
      @JsonProperty("audit") AuditDTO audit) {
    this.revision = revision;
    this.status = status;
    this.publishedBy = publishedBy;
    this.publishedAt = publishedAt;
    this.audit = audit;
  }

  /**
   * Creates a wire summary from revision metadata.
   *
   * @param revision The retained revision to convert.
   * @return The corresponding revision summary.
   */
  public static SemanticModelRevisionDTO fromRevision(SemanticModelRevision revision) {
    return new SemanticModelRevisionDTO(
        revision.revision(),
        revision.status(),
        revision.publishedBy(),
        revision.publishedAt(),
        DTOConverters.toDTO(revision.auditInfo()));
  }

  @Override
  public int revision() {
    return revision;
  }

  @Override
  public Status status() {
    return status;
  }

  @Override
  @Nullable
  public String publishedBy() {
    return publishedBy;
  }

  @Override
  @Nullable
  public Instant publishedAt() {
    return publishedAt;
  }

  @Override
  public AuditDTO auditInfo() {
    return audit;
  }
}
