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
import java.util.Objects;
import javax.annotation.Nullable;

/** Base for immutable, typed SQL AST nodes. Each operation has its own node class. */
public abstract class SqlExpression {
  private final List<SqlExpression> children;

  SqlExpression(List<SqlExpression> children) {
    this.children = Collections.unmodifiableList(new ArrayList<>(children));
    this.children.forEach(Objects::requireNonNull);
  }

  /**
   * Returns the immutable ordered source operands.
   *
   * @return the immutable ordered source operands
   */
  public List<SqlExpression> children() {
    return children;
  }

  /**
   * Returns the intrinsic logical type, or null when inferred from context.
   *
   * @return the intrinsic logical type, or null when inferred from context
   */
  @Nullable
  public SqlType type() {
    return null;
  }

  int depth() {
    return 1 + children.stream().mapToInt(SqlExpression::depth).max().orElse(0);
  }

  abstract List<?> attributes();

  boolean sameContents(SqlExpression other) {
    return attributes().equals(other.attributes()) && children.equals(other.children());
  }

  /** {@inheritDoc} */
  @Override
  public final int hashCode() {
    return Objects.hash(getClass(), attributes(), children);
  }

  /** {@inheritDoc} */
  @Override
  public final String toString() {
    return getClass().getSimpleName()
        + "{attributes="
        + attributes()
        + ", children="
        + children
        + "}";
  }
}
