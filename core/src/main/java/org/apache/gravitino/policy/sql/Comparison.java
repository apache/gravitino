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
package org.apache.gravitino.policy.sql;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/** A binary comparison with SQL three-valued null semantics. */
final class Comparison extends SqlExpression {
  private final ComparisonOperator operator;

  Comparison(ComparisonOperator operator, SqlExpression left, SqlExpression right) {
    super(Arrays.asList(left, right));
    this.operator = operator;
  }

  ComparisonOperator operator() {
    return operator;
  }

  SqlExpression left() {
    return children().get(0);
  }

  SqlExpression right() {
    return children().get(1);
  }

  /** {@inheritDoc} */
  @Override
  public boolean equals(Object other) {
    return other instanceof Comparison && sameContents((Comparison) other);
  }

  @Override
  List<?> attributes() {
    return Collections.singletonList(operator);
  }
}
