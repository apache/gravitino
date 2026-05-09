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
package org.apache.gravitino.server.authorization.annotations;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import org.apache.gravitino.MetadataObject;

/**
 * This annotation is used to implement unified authentication in AOP. Use Expressions to define the
 * required privileges for an API.
 *
 * <p>An annotated method may define up to three authorization expressions. The executor selects
 * which expression to evaluate based on the associated {@link ExpressionCondition} and then
 * performs the operation described by the associated {@link ExpressionAction}. The overall logic
 * follows this pseudo-code:
 *
 * <pre>{@code
 * if (firstExpressionCondition) {
 *   action(firstExpression)           // default: ALWAYS → EVALUATE
 * }
 * if (secondaryExpressionCondition) { // default: NONE → not evaluated
 *   action(secondaryExpression)
 * }
 * if (thirdExpressionCondition) {     // default: NONE → not evaluated
 *   action(thirdExpression)
 * }
 * }</pre>
 *
 * <p>Typical usage for a load-table endpoint:
 *
 * <pre>{@code
 * // if (NOT_CONTAIN_REQUIRED_PRIVILEGES) { EVALUATE(LOAD_TABLE_EXPRESSION) }
 * // if (CONTAIN_REQUIRED_PRIVILEGES)     { EVALUATE(MODIFY_TABLE_EXPRESSION) }
 * // if (PREVIOUS_EXPRESSION_FORBIDDEN)   { CHECK_METADATA_OBJECT_EXISTS(thirdExpression) }
 * }</pre>
 */
@Target({ElementType.METHOD})
@Retention(RetentionPolicy.RUNTIME)
public @interface AuthorizationExpression {
  /**
   * The primary expression to evaluate for authorization, which represents multiple privileges.
   *
   * @return the expression to evaluate for authorization.
   */
  String expression() default "";

  /**
   * The condition under which the primary expression is evaluated. Defaults to {@link
   * ExpressionCondition#ALWAYS} so that the primary check runs on every request unless a more
   * specific condition (e.g. {@link ExpressionCondition#NOT_CONTAIN_REQUIRED_PRIVILEGES}) is set.
   *
   * @return the condition for evaluating the primary expression.
   */
  ExpressionCondition firstExpressionCondition() default ExpressionCondition.ALWAYS;

  /**
   * The action to take when the primary expression is evaluated.
   *
   * @return the action for the primary expression evaluation.
   */
  ExpressionAction firstExpressionAction() default ExpressionAction.EVALUATE;

  /**
   * Used to identify the type of metadata that needs to be accessed.
   *
   * @return accessMetadataType
   */
  MetadataObject.Type accessMetadataType() default MetadataObject.Type.METALAKE;

  /**
   * Error message when authorization failed.
   *
   * @return error message
   */
  String errorMessage() default "";

  /**
   * The secondary expression to evaluate for authorization, which represents multiple privileges.
   * Only evaluated when {@link #secondaryExpressionCondition()} is met.
   *
   * @return the secondary expression to evaluate for authorization.
   */
  String secondaryExpression() default "";

  /**
   * The condition under which the secondary expression is evaluated instead of the primary
   * expression.
   *
   * @return the condition for evaluating the secondary expression.
   */
  ExpressionCondition secondaryExpressionCondition() default ExpressionCondition.NONE;

  /**
   * The action to take when the secondary expression is evaluated.
   *
   * @return the action for the secondary expression evaluation.
   */
  ExpressionAction secondaryExpressionAction() default ExpressionAction.EVALUATE;

  /**
   * An optional third expression to evaluate. Only evaluated when {@link
   * #thirdExpressionCondition()} is met.
   *
   * @return the third expression to evaluate for authorization.
   */
  String thirdExpression() default "";

  /**
   * The condition under which the third expression is evaluated.
   *
   * @return the condition for evaluating the third expression.
   */
  ExpressionCondition thirdExpressionCondition() default ExpressionCondition.NONE;

  /**
   * The action to take when the third expression is evaluated.
   *
   * @return the action for the third expression evaluation.
   */
  ExpressionAction thirdExpressionAction() default ExpressionAction.EVALUATE;
}
