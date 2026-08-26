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
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import org.apache.gravitino.semantic.DialectExpression;
import org.apache.gravitino.semantic.Expression;

/** DTO for a multi-dialect Semantic Model expression. */
@Getter
@EqualsAndHashCode
@NoArgsConstructor(access = AccessLevel.PRIVATE)
@JsonInclude(JsonInclude.Include.NON_NULL)
public class ExpressionDTO {

  @JsonProperty("dialects")
  @Getter(AccessLevel.NONE)
  private DialectExpressionDTO[] dialects;

  @Builder(setterPrefix = "with")
  private ExpressionDTO(DialectExpressionDTO[] dialects) {
    this.dialects = SemanticDTOUtils.copyArray(dialects);
  }

  /**
   * Returns the ordered dialect-specific expressions.
   *
   * @return A defensive copy of the dialect expressions.
   */
  public DialectExpressionDTO[] getDialects() {
    return SemanticDTOUtils.copyArray(dialects);
  }

  /**
   * Creates an expression DTO from an API model.
   *
   * @param expression The API expression.
   * @return The expression DTO.
   */
  public static ExpressionDTO fromExpression(Expression expression) {
    return builder()
        .withDialects(
            SemanticDTOUtils.convertArray(
                expression.dialects(),
                DialectExpressionDTO::fromDialectExpression,
                DialectExpressionDTO[]::new))
        .build();
  }

  /**
   * Converts this DTO to an API expression.
   *
   * @return The API expression.
   */
  public Expression toExpression() {
    DialectExpression[] convertedDialects =
        SemanticDTOUtils.convertArray(
            dialects, DialectExpressionDTO::toDialectExpression, DialectExpression[]::new);
    return Expression.builder().withDialects(convertedDialects).build();
  }
}
