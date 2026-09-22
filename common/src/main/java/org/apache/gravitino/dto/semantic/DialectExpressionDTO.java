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

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import org.apache.gravitino.semantic.DialectExpression;

/** DTO for an expression written in a specific Semantic Model dialect. */
@Getter
@EqualsAndHashCode
@NoArgsConstructor(access = AccessLevel.PRIVATE)
@AllArgsConstructor(access = AccessLevel.PRIVATE)
@Builder(setterPrefix = "with")
@JsonInclude(JsonInclude.Include.NON_NULL)
public class DialectExpressionDTO {

  @JsonProperty("dialect")
  private String dialect;

  @JsonProperty("expression")
  private String expression;

  /**
   * Creates a dialect expression DTO from an API model.
   *
   * @param dialectExpression The API dialect expression.
   * @return The dialect expression DTO.
   */
  public static DialectExpressionDTO fromDialectExpression(DialectExpression dialectExpression) {
    return builder()
        .withDialect(dialectExpression.dialect())
        .withExpression(dialectExpression.expression())
        .build();
  }

  /**
   * Converts this DTO to an API dialect expression.
   *
   * @return The API dialect expression.
   */
  public DialectExpression toDialectExpression() {
    return DialectExpression.builder().withDialect(dialect).withExpression(expression).build();
  }
}
