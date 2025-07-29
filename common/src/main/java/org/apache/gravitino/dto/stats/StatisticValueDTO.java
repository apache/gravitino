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

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.google.common.base.Objects;
import java.util.List;
import java.util.Map;
import org.apache.gravitino.rel.types.Type;
import org.apache.gravitino.rel.types.Types;
import org.apache.gravitino.stats.StatisticValue;

/** Base class for Statistic Value Data Transfer Objects. */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")
@JsonSubTypes({
  @JsonSubTypes.Type(value = StatisticValueDTO.BooleanValueDTO.class, name = "boolean"),
  @JsonSubTypes.Type(value = StatisticValueDTO.LongValueDTO.class, name = "long"),
  @JsonSubTypes.Type(value = StatisticValueDTO.DoubleValueDTO.class, name = "double"),
  @JsonSubTypes.Type(value = StatisticValueDTO.StringValueDTO.class, name = "string"),
  @JsonSubTypes.Type(value = StatisticValueDTO.ListValueDTO.class, name = "list"),
  @JsonSubTypes.Type(value = StatisticValueDTO.ObjectValueDTO.class, name = "object")
})
public abstract class StatisticValueDTO<T> implements StatisticValue<T> {

  @JsonProperty("value")
  protected T value;

  protected StatisticValueDTO() {}

  protected StatisticValueDTO(T value) {
    this.value = value;
  }

  @Override
  public T value() {
    return value;
  }

  @Override
  @JsonIgnore
  public abstract Type dataType();

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (!(o instanceof StatisticValueDTO)) return false;
    StatisticValueDTO<?> that = (StatisticValueDTO<?>) o;
    return Objects.equal(value, that.value);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(value);
  }

  /** Boolean value DTO. */
  public static class BooleanValueDTO extends StatisticValueDTO<Boolean> {

    private BooleanValueDTO() {
      super();
    }

    public BooleanValueDTO(Boolean value) {
      super(value);
    }

    @Override
    public Type dataType() {
      return Types.BooleanType.get();
    }
  }

  /** Long value DTO. */
  public static class LongValueDTO extends StatisticValueDTO<Long> {

    private LongValueDTO() {
      super();
    }

    public LongValueDTO(Long value) {
      super(value);
    }

    @Override
    public Type dataType() {
      return Types.LongType.get();
    }
  }

  /** Double value DTO. */
  public static class DoubleValueDTO extends StatisticValueDTO<Double> {

    private DoubleValueDTO() {
      super();
    }

    public DoubleValueDTO(Double value) {
      super(value);
    }

    @Override
    public Type dataType() {
      return Types.DoubleType.get();
    }
  }

  /** String value DTO. */
  public static class StringValueDTO extends StatisticValueDTO<String> {

    private StringValueDTO() {
      super();
    }

    public StringValueDTO(String value) {
      super(value);
    }

    @Override
    public Type dataType() {
      return Types.StringType.get();
    }
  }

  /** List value DTO. */
  public static class ListValueDTO<T> extends StatisticValueDTO<List<StatisticValueDTO<T>>> {

    @JsonProperty("elementType")
    private Type elementType;

    private ListValueDTO() {
      super();
    }

    public ListValueDTO(List<StatisticValueDTO<T>> value, Type elementType) {
      super(value);
      this.elementType = elementType;
    }

    @Override
    public Type dataType() {
      return Types.ListType.of(elementType, false);
    }
  }

  /** Object value DTO. */
  public static class ObjectValueDTO extends StatisticValueDTO<Map<String, StatisticValueDTO<?>>> {

    private ObjectValueDTO() {
      super();
    }

    public ObjectValueDTO(Map<String, StatisticValueDTO<?>> value) {
      super(value);
    }

    @Override
    public Type dataType() {
      // For object values, we use a struct type. The exact fields would be determined by the map
      // keys.
      // For simplicity, we'll return a generic struct type.
      return Types.StructType.of();
    }
  }
}
