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
import com.google.common.base.Preconditions;
import lombok.EqualsAndHashCode;
import lombok.ToString;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.function.FunctionColumn;
import org.apache.gravitino.json.JsonUtils;
import org.apache.gravitino.rel.types.Type;
import org.apache.gravitino.rest.RESTRequest;

/** DTO for {@link FunctionColumn}. */
@EqualsAndHashCode
@ToString
public class FunctionColumnDTO implements RESTRequest {

  @JsonProperty("columnName")
  private String name;

  @JsonProperty("dataType")
  @JsonSerialize(using = JsonUtils.TypeSerializer.class)
  @JsonDeserialize(using = JsonUtils.TypeDeserializer.class)
  private Type dataType;

  @JsonProperty("comment")
  private String comment;

  private FunctionColumnDTO() {}

  /**
   * Creates a function column DTO.
   *
   * @param name Column name.
   * @param dataType Column data type.
   * @param comment Optional column comment.
   */
  public FunctionColumnDTO(String name, Type dataType, String comment) {
    this.name = name;
    this.dataType = dataType;
    this.comment = comment;
  }

  /**
   * Converts this DTO to {@link FunctionColumn}.
   *
   * @return The converted {@link FunctionColumn}.
   */
  public FunctionColumn toFunctionColumn() {
    return FunctionColumn.of(name, dataType, comment);
  }

  /** {@inheritDoc} */
  @Override
  public void validate() throws IllegalArgumentException {
    Preconditions.checkArgument(StringUtils.isNotBlank(name), "\"columnName\" field is required");
    Preconditions.checkArgument(dataType != null, "\"dataType\" field is required");
  }
}
