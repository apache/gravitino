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
package org.apache.gravitino.policy.rego;

import com.google.common.base.Preconditions;
import java.util.Objects;

/** A Boolean negation operation. */
final class Not implements CanonicalExpression {
  private final CanonicalExpression operand;

  Not(CanonicalExpression operand) {
    this.operand = operand;
  }

  static CanonicalExpression negate(CanonicalExpression operand) {
    return operand instanceof Not ? ((Not) operand).operand() : new Not(operand);
  }

  CanonicalExpression operand() {
    return operand;
  }

  @Override
  public void validate() throws IllegalArgumentException {
    Preconditions.checkArgument(operand != null, "not requires an operand");
    operand.validate();
    Preconditions.checkArgument(
        ExpressionValidator.isPredicate(operand), "not operand must be a boolean predicate");
  }

  @Override
  public int depth() {
    return 1 + (operand == null ? 0 : operand.depth());
  }

  @Override
  public boolean equals(Object other) {
    return other instanceof Not && Objects.equals(operand, ((Not) other).operand);
  }

  @Override
  public int hashCode() {
    return Objects.hash(operand);
  }

  @Override
  public String toString() {
    return "Not{" + "operand=" + operand + '}';
  }
}
