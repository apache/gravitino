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

<<<<<<< HEAD
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
=======
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Objects;
import java.util.Optional;
import org.apache.gravitino.stats.Statistic;
import org.apache.gravitino.stats.StatisticValue;

/** Represents a Statistic Data Transfer Object (DTO). */
>>>>>>> dc6f26d92 ([#7274] feat(client-java): Support statistics http client config for gravitino client)
public class StatisticDTO implements Statistic {

  @JsonProperty("name")
  private String name;

<<<<<<< HEAD
  @Nullable
  @JsonInclude
  @JsonProperty("value")
  @JsonSerialize(using = JsonUtils.StatisticValueSerializer.class)
  @JsonDeserialize(using = JsonUtils.StatisticValueDeserializer.class)
  private StatisticValue<?> value;
=======
  @JsonProperty("value")
  private StatisticValueDTO<?> value;
>>>>>>> dc6f26d92 ([#7274] feat(client-java): Support statistics http client config for gravitino client)

  @JsonProperty("reserved")
  private boolean reserved;

  @JsonProperty("modifiable")
  private boolean modifiable;

<<<<<<< HEAD
  @JsonProperty("audit")
  private AuditDTO audit;
=======
  private StatisticDTO() {}
>>>>>>> dc6f26d92 ([#7274] feat(client-java): Support statistics http client config for gravitino client)

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

<<<<<<< HEAD
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
=======
  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof StatisticDTO)) {
      return false;
    }

    StatisticDTO that = (StatisticDTO) o;
    return reserved == that.reserved
        && modifiable == that.modifiable
        && Objects.equal(name, that.name)
        && Objects.equal(value, that.value);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(name, value, reserved, modifiable);
  }

  /** @return a new builder for constructing a Statistic DTO. */
>>>>>>> dc6f26d92 ([#7274] feat(client-java): Support statistics http client config for gravitino client)
  public static Builder builder() {
    return new Builder();
  }

<<<<<<< HEAD
  /** Builder class for constructing instances of {@link StatisticDTO}. */
  public static class Builder {
    private String name;
    private StatisticValue<?> value;
    private boolean reserved;
    private boolean modifiable;
    private AuditDTO audit;
=======
  /** Builder class for constructing StatisticDTO instances. */
  public static class Builder {
    private final StatisticDTO statisticDTO;

    private Builder() {
      statisticDTO = new StatisticDTO();
    }
>>>>>>> dc6f26d92 ([#7274] feat(client-java): Support statistics http client config for gravitino client)

    /**
     * Sets the name of the statistic.
     *
<<<<<<< HEAD
     * @param name the name of the statistic
     * @return the builder instance
     */
    public Builder withName(String name) {
      this.name = name;
=======
     * @param name The name of the statistic.
     * @return The builder instance.
     */
    public Builder withName(String name) {
      statisticDTO.name = name;
>>>>>>> dc6f26d92 ([#7274] feat(client-java): Support statistics http client config for gravitino client)
      return this;
    }

    /**
     * Sets the value of the statistic.
     *
<<<<<<< HEAD
     * @param value the value of the statistic
     * @return the builder instance
     */
    public Builder withValue(Optional<StatisticValue<?>> value) {
      value.ifPresent(v -> this.value = v);
=======
     * @param value The value of the statistic.
     * @return The builder instance.
     */
    public Builder withValue(StatisticValueDTO<?> value) {
      statisticDTO.value = value;
>>>>>>> dc6f26d92 ([#7274] feat(client-java): Support statistics http client config for gravitino client)
      return this;
    }

    /**
     * Sets whether the statistic is reserved.
     *
<<<<<<< HEAD
     * @param reserved true if the statistic is reserved, false otherwise
     * @return the builder instance
     */
    public Builder withReserved(boolean reserved) {
      this.reserved = reserved;
=======
     * @param reserved Whether the statistic is reserved.
     * @return The builder instance.
     */
    public Builder withReserved(boolean reserved) {
      statisticDTO.reserved = reserved;
>>>>>>> dc6f26d92 ([#7274] feat(client-java): Support statistics http client config for gravitino client)
      return this;
    }

    /**
     * Sets whether the statistic is modifiable.
     *
<<<<<<< HEAD
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
=======
     * @param modifiable Whether the statistic is modifiable.
     * @return The builder instance.
     */
    public Builder withModifiable(boolean modifiable) {
      statisticDTO.modifiable = modifiable;
      return this;
    }

    /** @return The constructed Statistic DTO. */
    public StatisticDTO build() {
>>>>>>> dc6f26d92 ([#7274] feat(client-java): Support statistics http client config for gravitino client)
      return statisticDTO;
    }
  }
}
