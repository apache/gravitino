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
package org.apache.gravitino.iceberg.service.restriction;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import org.apache.iceberg.expressions.And;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.ExpressionParser;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.expressions.Not;
import org.apache.iceberg.expressions.Or;

/** Canonicalizes closed predicates with a separate bound on generated restriction complexity. */
final class SqlRestrictionCanonicalizer {
  private static final int MAX_NODES = 16384;
  private static final int MAX_DEPTH = 256;

  private SqlRestrictionCanonicalizer() {}

  static Expression canonicalize(Expression expression) {
    validateBudget(expression);
    Expression rewritten = Expressions.rewriteNot(expression);
    validateBudget(rewritten);
    return canonical(rewritten);
  }

  static Expression combine(boolean conjunction, List<Expression> children) {
    if (children.isEmpty()) {
      return conjunction ? Expressions.alwaysTrue() : Expressions.alwaysFalse();
    }
    return balanced(conjunction, children, 0, children.size());
  }

  private static Expression canonical(Expression expression) {
    if (expression.op() != Expression.Operation.AND && expression.op() != Expression.Operation.OR) {
      return expression;
    }
    boolean conjunction = expression.op() == Expression.Operation.AND;
    Deque<Expression> pending = new ArrayDeque<>();
    pending.push(expression);
    Map<String, Expression> children = new TreeMap<>();
    boolean absorbing = false;
    while (!pending.isEmpty()) {
      Expression child = pending.pop();
      if (child.op() == expression.op()) {
        children(child).forEach(pending::push);
      } else {
        Expression normalized = canonical(child);
        if (normalized.op()
            == (conjunction ? Expression.Operation.FALSE : Expression.Operation.TRUE)) {
          absorbing = true;
        } else if (normalized.op()
            != (conjunction ? Expression.Operation.TRUE : Expression.Operation.FALSE)) {
          children.put(ExpressionParser.toJson(normalized), normalized);
        }
      }
    }
    if (absorbing) {
      return conjunction ? Expressions.alwaysFalse() : Expressions.alwaysTrue();
    }
    return combine(conjunction, new ArrayList<>(children.values()));
  }

  private static Expression balanced(
      boolean conjunction, List<Expression> children, int from, int to) {
    if (to - from == 1) {
      return children.get(from);
    }
    int middle = (from + to) >>> 1;
    Expression left = balanced(conjunction, children, from, middle);
    Expression right = balanced(conjunction, children, middle, to);
    return conjunction ? Expressions.and(left, right) : Expressions.or(left, right);
  }

  private static void validateBudget(Expression root) {
    IdentityHashMap<Expression, int[]> sizes = new IdentityHashMap<>();
    Deque<Expression> pending = new ArrayDeque<>();
    pending.push(root);
    while (!pending.isEmpty()) {
      Expression expression = pending.peek();
      List<Expression> children = children(expression);
      boolean ready = true;
      for (Expression child : children) {
        if (!sizes.containsKey(child)) {
          pending.push(child);
          ready = false;
        }
      }
      if (!ready) {
        continue;
      }
      int count = 1;
      int depth = 1;
      for (Expression child : children) {
        count += sizes.get(child)[0];
        depth = Math.max(depth, 1 + sizes.get(child)[1]);
      }
      if (count > MAX_NODES || depth > MAX_DEPTH) {
        throw new IllegalArgumentException(
            "Converted SQL restriction exceeds Iceberg expression complexity limits");
      }
      sizes.put(expression, new int[] {count, depth});
      pending.pop();
    }
  }

  private static List<Expression> children(Expression expression) {
    List<Expression> children = new ArrayList<>(2);
    if (expression instanceof And) {
      children.add(((And) expression).left());
      children.add(((And) expression).right());
    } else if (expression instanceof Or) {
      children.add(((Or) expression).left());
      children.add(((Or) expression).right());
    } else if (expression instanceof Not) {
      children.add(((Not) expression).child());
    }
    return children;
  }
}
