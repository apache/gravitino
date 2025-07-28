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
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import java.util.Optional;
import javax.annotation.Nullable;
import lombok.EqualsAndHashCode;
import lombok.ToString;
import org.apache.gravitino.json.JsonUtils;
import org.apache.gravitino.stats.Statistic;
import org.apache.gravitino.stats.StatisticValue;

@EqualsAndHashCode
@ToString
public class StatisticDTO implements Statistic {

  @JsonProperty("name")
  private String name;

  @Nullable
  @JsonProperty("value")
  @JsonSerialize(using = JsonUtils.StatisticValueSerializer.class)
  @JsonDeserialize(using = JsonUtils.StatisticValueDeserializer.class)
  private StatisticValue<?> value;

  @JsonProperty("reserved")
  private boolean reserved;

  @JsonProperty("modifiable")
  private boolean modifiable;

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

  public static Builder builder() {
    return new Builder();
  }

  public static class Builder {
    private String name;
    private StatisticValue<?> value;
    private boolean reserved;
    private boolean modifiable;

    public Builder withName(String name) {
      this.name = name;
      return this;
    }

    public Builder withValue(Optional<StatisticValue<?>> value) {
      value.ifPresent(v -> this.value = v);
      return this;
    }

    public Builder withReserved(boolean reserved) {
      this.reserved = reserved;
      return this;
    }

    public Builder withModifiable(boolean modifiable) {
      this.modifiable = modifiable;
      return this;
    }

    public StatisticDTO build() {
      StatisticDTO statisticDTO = new StatisticDTO();
      statisticDTO.name = this.name;
      statisticDTO.value = this.value;
      statisticDTO.reserved = this.reserved;
      statisticDTO.modifiable = this.modifiable;
      return statisticDTO;
    }
  }
}
