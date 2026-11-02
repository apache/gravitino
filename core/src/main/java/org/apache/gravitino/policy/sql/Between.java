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

/** An inclusive SQL range test. */
public final class Between extends SqlExpression {
  Between(SqlExpression operand, SqlExpression lower, SqlExpression upper) {
    super(Arrays.asList(operand, lower, upper));
  }

  /**
   * Returns the source operand.
   *
   * @return the source operand
   */
  public SqlExpression operand() {
    return children().get(0);
  }

  /**
   * Returns the inclusive lower bound.
   *
   * @return the inclusive lower bound
   */
  public SqlExpression lower() {
    return children().get(1);
  }

  /**
   * Returns the inclusive upper bound.
   *
   * @return the inclusive upper bound
   */
  public SqlExpression upper() {
    return children().get(2);
  }

  /** {@inheritDoc} */
  @Override
  public boolean equals(Object other) {
    return other instanceof Between && sameContents((Between) other);
  }

  @Override
  List<?> attributes() {
    return Collections.emptyList();
  }
}
