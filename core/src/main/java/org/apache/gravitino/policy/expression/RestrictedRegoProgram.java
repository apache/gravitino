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
package org.apache.gravitino.policy.expression;

import com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import org.apache.gravitino.policy.expression.CanonicalExpression.Logical;
import org.apache.gravitino.policy.expression.CanonicalExpression.Not;
import org.apache.gravitino.policy.expression.CanonicalExpression.Operator;

/** A parsed, unresolved program in the {@code restricted-rego-v1} source dialect. */
public interface RestrictedRegoProgram {

  /**
   * Returns the rule type declared by this program.
   *
   * @return rule type
   */
  RuleType ruleType();

  /**
   * Validates the rule and every expression it contains.
   *
   * @throws IllegalArgumentException if the rule is malformed
   */
  void validate() throws IllegalArgumentException;

  /** Rule heads supported by {@code restricted-rego-v1}. */
  enum RuleType {
    /** A row-filter rule. */
    FILTER,
    /** A column-mask rule. */
    MASK
  }

  /** Logical column-mask actions supported by the first restricted profile. */
  enum MaskAction {
    /** Mask alphanumeric characters. */
    MASK_ALPHANUM("mask-alphanum"),
    /** Replace the value with the action's fixed value. */
    MASK_TO_FIXED_VALUE("mask-to-fixed-value"),
    /** Replace the value with null. */
    REPLACE_WITH_NULL("replace-with-null"),
    /** Preserve only the first four characters. */
    SHOW_FIRST_4("show-first-4"),
    /** Preserve only the last four characters. */
    SHOW_LAST_4("show-last-4"),
    /** Truncate a temporal value to its year. */
    TRUNCATE_TO_YEAR("truncate-to-year"),
    /** Truncate a temporal value to its month. */
    TRUNCATE_TO_MONTH("truncate-to-month"),
    /** Apply a global SHA-256 mask. */
    SHA_256_GLOBAL("sha-256-global"),
    /** Apply a query-local SHA-256 mask. */
    SHA_256_QUERY_LOCAL("sha-256-query-local");

    private final String value;

    MaskAction(String value) {
      this.value = value;
    }

    /**
     * Parses a mask-action value.
     *
     * @param value source action value
     * @return parsed action
     * @throws IllegalArgumentException if the action is not allowlisted
     */
    public static MaskAction fromValue(String value) {
      Preconditions.checkArgument(value != null && !value.isEmpty(), "mask action cannot be empty");
      for (MaskAction action : values()) {
        if (action.value.equals(value)) {
          return action;
        }
      }
      throw new IllegalArgumentException("Unsupported restricted-rego-v1 mask action: " + value);
    }

    /**
     * Returns the source action value.
     *
     * @return source action value
     */
    public String value() {
      return value;
    }
  }

  /** One guarded result in a conditional row-filter rule. */
  final class FilterBranch {
    private final CanonicalExpression result;
    private final CanonicalExpression condition;

    FilterBranch(CanonicalExpression result, CanonicalExpression condition) {
      this.result = result;
      this.condition = condition;
    }

    /**
     * Returns the Boolean result selected by this branch.
     *
     * @return unresolved Boolean result
     */
    public CanonicalExpression result() {
      return result;
    }

    /**
     * Returns this branch's Boolean condition.
     *
     * @return unresolved Boolean condition
     */
    public CanonicalExpression condition() {
      return condition;
    }

    /**
     * Validates the branch.
     *
     * @throws IllegalArgumentException if its result or condition is not Boolean
     */
    public void validate() throws IllegalArgumentException {
      Preconditions.checkArgument(result != null, "filter branch result cannot be null");
      Preconditions.checkArgument(condition != null, "filter branch condition cannot be null");
      result.validate();
      condition.validate();
      Preconditions.checkArgument(
          ExpressionValidation.isPredicate(result), "filter branch result must be Boolean");
      Preconditions.checkArgument(
          ExpressionValidation.isPredicate(condition), "filter branch condition must be Boolean");
    }

    @Override
    public boolean equals(Object other) {
      if (!(other instanceof FilterBranch)) {
        return false;
      }
      FilterBranch that = (FilterBranch) other;
      return Objects.equals(result, that.result) && Objects.equals(condition, that.condition);
    }

    @Override
    public int hashCode() {
      return Objects.hash(result, condition);
    }

    @Override
    public String toString() {
      return "FilterBranch{" + "result=" + result + ", condition=" + condition + '}';
    }
  }

  /** A complete {@code filter := ...} row-filter rule. */
  final class RowFilter implements RestrictedRegoProgram {
    private final List<FilterBranch> branches;
    private final CanonicalExpression fallback;

    RowFilter(List<FilterBranch> branches, CanonicalExpression fallback) {
      this.branches =
          branches == null ? null : Collections.unmodifiableList(new ArrayList<>(branches));
      this.fallback = fallback;
    }

    @Override
    public RuleType ruleType() {
      return RuleType.FILTER;
    }

    /**
     * Returns the guarded results in source order.
     *
     * @return immutable branches, or an empty list for an unconditional rule
     */
    public List<FilterBranch> branches() {
      return branches;
    }

    /**
     * Returns the unconditional result or final {@code else} result.
     *
     * @return unresolved Boolean fallback
     */
    public CanonicalExpression fallback() {
      return fallback;
    }

    /**
     * Returns whether this rule contains at least one conditional branch.
     *
     * @return {@code true} for a conditional rule
     */
    public boolean isConditional() {
      return branches != null && !branches.isEmpty();
    }

    /**
     * Lowers the ordered branches to one Boolean predicate with first-match semantics.
     *
     * <p>The returned predicate remains unresolved. Context and column binding can be applied to it
     * by a later resolver.
     *
     * @return lowered unresolved predicate
     */
    public CanonicalExpression lower() {
      validate();
      if (branches.isEmpty()) {
        return fallback;
      }

      List<CanonicalExpression> disjuncts = new ArrayList<>();
      List<CanonicalExpression> precedingBranchMisses = new ArrayList<>();
      for (FilterBranch branch : branches) {
        List<CanonicalExpression> selected = new ArrayList<>(precedingBranchMisses);
        selected.add(branch.condition());
        selected.add(branch.result());
        disjuncts.add(new Logical(Operator.AND, selected));
        precedingBranchMisses.add(new Not(branch.condition()));
      }

      List<CanonicalExpression> fallbackOperands = new ArrayList<>(precedingBranchMisses);
      fallbackOperands.add(fallback);
      disjuncts.add(new Logical(Operator.AND, fallbackOperands));
      CanonicalExpression lowered = new Logical(Operator.OR, disjuncts);
      lowered.validate();
      return lowered;
    }

    @Override
    public void validate() throws IllegalArgumentException {
      Preconditions.checkArgument(branches != null, "filter branches cannot be null");
      Preconditions.checkArgument(fallback != null, "filter fallback cannot be null");
      for (FilterBranch branch : branches) {
        Preconditions.checkArgument(branch != null, "filter branch cannot be null");
        branch.validate();
      }
      fallback.validate();
      Preconditions.checkArgument(
          ExpressionValidation.isPredicate(fallback), "filter fallback must be Boolean");
    }

    @Override
    public boolean equals(Object other) {
      if (!(other instanceof RowFilter)) {
        return false;
      }
      RowFilter that = (RowFilter) other;
      return Objects.equals(branches, that.branches) && Objects.equals(fallback, that.fallback);
    }

    @Override
    public int hashCode() {
      return Objects.hash(branches, fallback);
    }

    @Override
    public String toString() {
      return "RowFilter{" + "branches=" + branches + ", fallback=" + fallback + '}';
    }
  }

  /** One guarded action in a conditional column-mask rule. */
  final class MaskBranch {
    private final MaskAction action;
    private final CanonicalExpression condition;

    MaskBranch(MaskAction action, CanonicalExpression condition) {
      this.action = action;
      this.condition = condition;
    }

    /**
     * Returns the action selected by this branch.
     *
     * @return mask action
     */
    public MaskAction action() {
      return action;
    }

    /**
     * Returns this branch's context-only condition.
     *
     * @return unresolved Boolean condition
     */
    public CanonicalExpression condition() {
      return condition;
    }

    /**
     * Validates the branch.
     *
     * @throws IllegalArgumentException if the action or condition is invalid
     */
    public void validate() throws IllegalArgumentException {
      Preconditions.checkArgument(action != null, "mask branch action cannot be null");
      Preconditions.checkArgument(condition != null, "mask branch condition cannot be null");
      condition.validate();
      Preconditions.checkArgument(
          ExpressionValidation.isPredicate(condition), "mask branch condition must be Boolean");
      Preconditions.checkArgument(
          ExpressionValidation.isContextOnly(condition),
          "column-mask condition cannot contain col(...)");
    }

    @Override
    public boolean equals(Object other) {
      if (!(other instanceof MaskBranch)) {
        return false;
      }
      MaskBranch that = (MaskBranch) other;
      return action == that.action && Objects.equals(condition, that.condition);
    }

    @Override
    public int hashCode() {
      return Objects.hash(action, condition);
    }

    @Override
    public String toString() {
      return "MaskBranch{" + "action=" + action + ", condition=" + condition + '}';
    }
  }

  /** A complete {@code mask := action(...)} column-mask rule. */
  final class ColumnMask implements RestrictedRegoProgram {
    private final List<MaskBranch> branches;
    private final MaskAction fallback;

    ColumnMask(List<MaskBranch> branches, MaskAction fallback) {
      this.branches =
          branches == null ? null : Collections.unmodifiableList(new ArrayList<>(branches));
      this.fallback = fallback;
    }

    @Override
    public RuleType ruleType() {
      return RuleType.MASK;
    }

    /**
     * Returns the guarded actions in source order.
     *
     * @return immutable branches, or an empty list for an unconditional rule
     */
    public List<MaskBranch> branches() {
      return branches;
    }

    /**
     * Returns the unconditional action or final {@code else} action.
     *
     * @return fallback mask action
     */
    public MaskAction fallback() {
      return fallback;
    }

    /**
     * Returns whether this rule contains at least one conditional branch.
     *
     * @return {@code true} for a conditional rule
     */
    public boolean isConditional() {
      return branches != null && !branches.isEmpty();
    }

    @Override
    public void validate() throws IllegalArgumentException {
      Preconditions.checkArgument(branches != null, "mask branches cannot be null");
      Preconditions.checkArgument(fallback != null, "mask fallback cannot be null");
      for (MaskBranch branch : branches) {
        Preconditions.checkArgument(branch != null, "mask branch cannot be null");
        branch.validate();
      }
    }

    @Override
    public boolean equals(Object other) {
      if (!(other instanceof ColumnMask)) {
        return false;
      }
      ColumnMask that = (ColumnMask) other;
      return Objects.equals(branches, that.branches) && fallback == that.fallback;
    }

    @Override
    public int hashCode() {
      return Objects.hash(branches, fallback);
    }

    @Override
    public String toString() {
      return "ColumnMask{" + "branches=" + branches + ", fallback=" + fallback + '}';
    }
  }
}
