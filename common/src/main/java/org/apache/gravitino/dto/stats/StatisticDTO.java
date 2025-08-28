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
package org.apache.gravitino.dto.stats;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.google.common.base.Preconditions;
import java.util.Optional;
import javax.annotation.Nullable;
import lombok.EqualsAndHashCode;
import lombok.ToString;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.dto.AuditDTO;
import org.apache.gravitino.json.JsonUtils;
import org.apache.gravitino.stats.Statistic;
import org.apache.gravitino.stats.StatisticValue;

/** Data Transfer Object (DTO) for representing a statistic. */
@EqualsAndHashCode
@ToString
public class StatisticDTO implements Statistic {

  @JsonProperty("name")
  private String name;

  @Nullable
  @JsonInclude
  @JsonProperty("value")
  @JsonSerialize(using = JsonUtils.StatisticValueSerializer.class)
  @JsonDeserialize(using = JsonUtils.StatisticValueDeserializer.class)
  private StatisticValue<?> value;

  @JsonProperty("reserved")
  private boolean reserved;

  @JsonProperty("modifiable")
  private boolean modifiable;

  @JsonProperty("audit")
  private AuditDTO audit;

  @Override
  public String name() {
    return name;
  }

  @Override
  public Optional<StatisticValue<?>> value() {
    return Optional.ofNullable(value);
  }

  @Override
  public boolean reserved() {
    return reserved;
  }

  @Override
  public boolean modifiable() {
    return modifiable;
  }

  /**
   * Validates the StatisticDTO.
   *
   * @throws IllegalArgumentException if any of the required fields are invalid.
   */
  public void validate() {
    Preconditions.checkArgument(
        StringUtils.isNotBlank(name), "\"name\" is required and cannot be empty");

    Preconditions.checkArgument(
        audit != null, "\"audit\" information is required and cannot be null");
  }

  @Override
  public AuditDTO auditInfo() {
    return audit;
  }

  /**
   * Creates a new builder for constructing instances of {@link StatisticDTO}.
   *
   * @return a new instance of {@link Builder}
   */
  public static Builder builder() {
    return new Builder();
  }

  /** Builder class for constructing instances of {@link StatisticDTO}. */
  public static class Builder {
    private String name;
    private StatisticValue<?> value;
    private boolean reserved;
    private boolean modifiable;
    private AuditDTO audit;

    /**
     * Sets the name of the statistic.
     *
     * @param name the name of the statistic
     * @return the builder instance
     */
    public Builder withName(String name) {
      this.name = name;
      return this;
    }

    /**
     * Sets the value of the statistic.
     *
     * @param value the value of the statistic
     * @return the builder instance
     */
    public Builder withValue(Optional<StatisticValue<?>> value) {
      value.ifPresent(v -> this.value = v);
      return this;
    }

    /**
     * Sets whether the statistic is reserved.
     *
     * @param reserved true if the statistic is reserved, false otherwise
     * @return the builder instance
     */
    public Builder withReserved(boolean reserved) {
      this.reserved = reserved;
      return this;
    }

    /**
     * Sets whether the statistic is modifiable.
     *
     * @param modifiable true if the statistic is modifiable, false otherwise
     * @return the builder instance
     */
    public Builder withModifiable(boolean modifiable) {
      this.modifiable = modifiable;
      return this;
    }

    /**
     * Sets the audit information for the statistic.
     *
     * @param audit the audit information
     * @return the builder instance
     */
    public Builder withAudit(AuditDTO audit) {
      this.audit = audit;
      return this;
    }

    /**
     * Builds an instance of {@link StatisticDTO} with the provided values.
     *
     * @return a new instance of {@link StatisticDTO}
     */
    public StatisticDTO build() {
      StatisticDTO statisticDTO = new StatisticDTO();
      statisticDTO.name = this.name;
      statisticDTO.value = this.value;
      statisticDTO.reserved = this.reserved;
      statisticDTO.modifiable = this.modifiable;
      statisticDTO.audit = this.audit;
      statisticDTO.validate();
      return statisticDTO;
    }
  }
}
