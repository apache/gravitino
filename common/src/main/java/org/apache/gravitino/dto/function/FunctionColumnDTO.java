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
package org.apache.gravitino.dto.function;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import javax.annotation.Nullable;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import org.apache.gravitino.function.FunctionColumn;
import org.apache.gravitino.json.JsonUtils;
import org.apache.gravitino.rel.types.Type;

/** DTO for function column. */
@Getter
@EqualsAndHashCode
@NoArgsConstructor(access = AccessLevel.PRIVATE)
@AllArgsConstructor
@Builder(setterPrefix = "with")
public class FunctionColumnDTO {

  @JsonProperty("name")
  private String name;

  @JsonProperty("dataType")
  @JsonSerialize(using = JsonUtils.TypeSerializer.class)
  @JsonDeserialize(using = JsonUtils.TypeDeserializer.class)
  private Type dataType;

  @Nullable
  @JsonProperty("comment")
  private String comment;

  /**
   * Convert this DTO to a {@link FunctionColumn} instance.
   *
   * @return The function column.
   */
  public FunctionColumn toFunctionColumn() {
    return FunctionColumn.of(name, dataType, comment);
  }

  /**
   * Create a {@link FunctionColumnDTO} from a {@link FunctionColumn} instance.
   *
   * @param column The function column.
   * @return The function column DTO.
   */
  public static FunctionColumnDTO fromFunctionColumn(FunctionColumn column) {
    return new FunctionColumnDTO(column.name(), column.dataType(), column.comment());
  }

  @Override
  public String toString() {
    return "FunctionColumnDTO{"
        + "name='"
        + name
        + '\''
        + ", dataType="
        + dataType
        + ", comment='"
        + comment
        + '\''
        + '}';
  }
}
