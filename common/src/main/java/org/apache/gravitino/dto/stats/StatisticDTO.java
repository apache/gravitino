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

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Objects;
import java.util.Optional;
import org.apache.gravitino.stats.Statistic;
import org.apache.gravitino.stats.StatisticValue;

/** Represents a Statistic Data Transfer Object (DTO). */
public class StatisticDTO implements Statistic {

  @JsonProperty("name")
  private String name;

  @JsonProperty("value")
  private StatisticValueDTO<?> value;

  @JsonProperty("reserved")
  private boolean reserved;

  @JsonProperty("modifiable")
  private boolean modifiable;

  private StatisticDTO() {}

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
  public static Builder builder() {
    return new Builder();
  }

  /** Builder class for constructing StatisticDTO instances. */
  public static class Builder {
    private final StatisticDTO statisticDTO;

    private Builder() {
      statisticDTO = new StatisticDTO();
    }

    /**
     * Sets the name of the statistic.
     *
     * @param name The name of the statistic.
     * @return The builder instance.
     */
    public Builder withName(String name) {
      statisticDTO.name = name;
      return this;
    }

    /**
     * Sets the value of the statistic.
     *
     * @param value The value of the statistic.
     * @return The builder instance.
     */
    public Builder withValue(StatisticValueDTO<?> value) {
      statisticDTO.value = value;
      return this;
    }

    /**
     * Sets whether the statistic is reserved.
     *
     * @param reserved Whether the statistic is reserved.
     * @return The builder instance.
     */
    public Builder withReserved(boolean reserved) {
      statisticDTO.reserved = reserved;
      return this;
    }

    /**
     * Sets whether the statistic is modifiable.
     *
     * @param modifiable Whether the statistic is modifiable.
     * @return The builder instance.
     */
    public Builder withModifiable(boolean modifiable) {
      statisticDTO.modifiable = modifiable;
      return this;
    }

    /** @return The constructed Statistic DTO. */
    public StatisticDTO build() {
      return statisticDTO;
    }
  }
}
