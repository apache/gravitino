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
package org.apache.gravitino.dto.responses;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.dto.semantic.SemanticModelDTO;

/** Represents a response containing one Semantic Model. */
@Getter
@EqualsAndHashCode(callSuper = true)
@ToString
public class SemanticModelResponse extends BaseResponse {

  @JsonProperty("semanticModel")
  private final SemanticModelDTO semanticModel;

  /** Default constructor for Jackson deserialization. */
  public SemanticModelResponse() {
    super();
    this.semanticModel = null;
  }

  /**
   * Creates a successful Semantic Model response.
   *
   * @param semanticModel The Semantic Model DTO.
   */
  public SemanticModelResponse(SemanticModelDTO semanticModel) {
    super(0);
    this.semanticModel = semanticModel;
  }

  @Override
  public void validate() throws IllegalArgumentException {
    super.validate();
    Preconditions.checkArgument(semanticModel != null, "semanticModel must not be null");
    Preconditions.checkArgument(
        StringUtils.isNotBlank(semanticModel.name()),
        "semanticModel 'name' must not be null or empty");
    semanticModel.definition();
    Preconditions.checkArgument(
        semanticModel.auditInfo() != null, "semanticModel 'audit' must not be null");
  }
}
