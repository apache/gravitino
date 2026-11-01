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
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import javax.annotation.Nullable;

/** A searched or simple CASE, preserving first-match order and an optional ELSE. */
public final class CaseExpression extends SqlExpression {
  @Nullable private final SqlExpression selector;
  private final List<CaseBranch> branches;
  @Nullable private final SqlExpression fallback;

  CaseExpression(
      @Nullable SqlExpression selector,
      List<CaseBranch> branches,
      @Nullable SqlExpression fallback) {
    super(children(selector, branches, fallback));
    this.selector = selector;
    this.branches = Collections.unmodifiableList(new ArrayList<>(branches));
    this.fallback = fallback;
  }

  /**
   * Returns the simple CASE selector, or null for searched CASE.
   *
   * @return the simple CASE selector, or null for searched CASE
   */
  @Nullable
  public SqlExpression selector() {
    return selector;
  }

  /**
   * Returns the immutable ordered WHEN/THEN branches.
   *
   * @return the immutable ordered WHEN/THEN branches
   */
  public List<CaseBranch> branches() {
    return branches;
  }

  /**
   * Returns the ELSE expression, or null when omitted.
   *
   * @return the ELSE expression, or null when omitted
   */
  @Nullable
  public SqlExpression fallback() {
    return fallback;
  }

  /**
   * Returns the immutable explicitly authored result branches.
   *
   * @return the immutable explicitly authored result branches
   */
  public List<SqlExpression> results() {
    List<SqlExpression> result = new ArrayList<>();
    branches.forEach(branch -> result.add(branch.result()));
    if (fallback != null) {
      result.add(fallback);
    }
    return Collections.unmodifiableList(result);
  }

  /** {@inheritDoc} */
  @Override
  public boolean equals(Object other) {
    return other instanceof CaseExpression && sameContents((CaseExpression) other);
  }

  @Override
  List<?> attributes() {
    return Arrays.asList(selector != null, fallback != null);
  }

  private static List<SqlExpression> children(
      @Nullable SqlExpression selector,
      List<CaseBranch> branches,
      @Nullable SqlExpression fallback) {
    List<SqlExpression> result = new ArrayList<>();
    if (selector != null) {
      result.add(selector);
    }
    branches.forEach(
        branch -> {
          result.add(branch.condition());
          result.add(branch.result());
        });
    if (fallback != null) {
      result.add(fallback);
    }
    return result;
  }
}
