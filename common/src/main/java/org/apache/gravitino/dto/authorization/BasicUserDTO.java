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
import javax.annotation.Nullable;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.Audit;
import org.apache.gravitino.authorization.BasicUser;
import org.apache.gravitino.dto.AuditDTO;

/** Represents a BasicUser Data Transfer Object (DTO). */
public class BasicUserDTO implements BasicUser {

  @JsonProperty("id")
  private Long id;

  @JsonProperty("name")
  private String name;

  @Nullable
  @JsonProperty("externalId")
  private String externalId;

  @JsonProperty("enabled")
  private boolean enabled = true;

  @JsonProperty("audit")
  private AuditDTO audit;

  /** Default constructor for Jackson deserialization. */
  protected BasicUserDTO() {}

  protected BasicUserDTO(Long id, String name, String externalId, AuditDTO audit, boolean enabled) {
    this.id = id;
    this.name = name;
    this.externalId = externalId;
    this.enabled = enabled;
    this.audit = audit;
  }

  @Override
  public Long id() {
    return id;
  }

  @Override
  public String name() {
    return name;
  }

  @Override
  public String externalId() {
    return externalId;
  }

  @Override
  public boolean enabled() {
    return enabled;
  }

  @Override
  public Audit auditInfo() {
    return audit;
  }

  /** Creates a new Builder for constructing a BasicUserDTO instance. */
  public static Builder builder() {
    return new Builder();
  }

  /** Builder for {@link BasicUserDTO}. */
  public static class Builder {

    protected Long id;
    protected String name;
    protected String externalId;
    protected boolean enabled = true;
    protected AuditDTO audit;

    public Builder withId(Long id) {
      this.id = id;
      return this;
    }

    public Builder withName(String name) {
      this.name = name;
      return this;
    }

    public Builder withExternalId(String externalId) {
      this.externalId = externalId;
      return this;
    }

    public Builder withEnabled(boolean enabled) {
      this.enabled = enabled;
      return this;
    }

    public Builder withAudit(AuditDTO audit) {
      this.audit = audit;
      return this;
    }

    public BasicUserDTO build() {
      Preconditions.checkArgument(id != null, "id cannot be null");
      Preconditions.checkArgument(StringUtils.isNotBlank(name), "name cannot be null or empty");
      Preconditions.checkArgument(audit != null, "audit cannot be null");
      return new BasicUserDTO(id, name, externalId, audit, enabled);
    }
  }
}
