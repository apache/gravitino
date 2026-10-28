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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/** SQL membership in a literal list, including null members. */
public final class In extends SqlExpression {
  In(SqlExpression operand, List<Literal> values) {
    super(children(operand, values));
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
   * Returns the immutable membership list.
   *
   * @return the immutable membership list
   */
  public List<SqlExpression> values() {
    return children().subList(1, children().size());
  }

  /** {@inheritDoc} */
  @Override
  public boolean equals(Object other) {
    return other instanceof In && sameContents((In) other);
  }

  @Override
  List<?> attributes() {
    return Collections.emptyList();
  }

  private static List<SqlExpression> children(SqlExpression operand, List<Literal> values) {
    List<SqlExpression> result = new ArrayList<>();
    result.add(operand);
    result.addAll(values);
    return result;
  }
}
