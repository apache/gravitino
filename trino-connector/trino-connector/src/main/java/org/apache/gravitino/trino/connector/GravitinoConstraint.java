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
package org.apache.gravitino.trino.connector;

import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.Constraint;
import io.trino.spi.expression.ConnectorExpression;
import io.trino.spi.predicate.NullableValue;
import io.trino.spi.predicate.TupleDomain;
import java.lang.reflect.Method;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Predicate;
import java.util.stream.Collectors;

/** The GravitinoConstraint is used to warp Constraint */
public class GravitinoConstraint extends Constraint {

  // Cache of the reflectively resolved Constraint accessors (predicate/getPredicateColumns), which
  // only exist up to Trino 481. Resolved lazily so the 482+ modules (where they are absent and
  // never
  // invoked) never look them up.
  private static final Map<String, Method> CONSTRAINT_METHODS = new ConcurrentHashMap<>();

  private final Constraint delegate;

  GravitinoConstraint(Constraint constraint) {
    super(constraint.getSummary());
    this.delegate = constraint;
  }

  @Override
  public TupleDomain<ColumnHandle> getSummary() {
    return delegate.getSummary().transformKeys(GravitinoHandle::unWrap);
  }

  @Override
  public ConnectorExpression getExpression() {
    return delegate.getExpression();
  }

  @Override
  public Map<String, ColumnHandle> getAssignments() {
    return GravitinoHandle.unWrap(delegate.getAssignments());
  }

  // Not annotated @Override: Constraint.predicate() exists up to Trino 481 but was removed in Trino
  // 482. On versions that still expose it this method overrides it and wraps the delegate predicate
  // so unwrapped column handles are seen; on Trino 482+ it is never invoked by the SPI (the method
  // no longer exists to override) and is kept only so the shared source compiles across versions.
  @SuppressWarnings("unchecked")
  public Optional<Predicate<Map<ColumnHandle, NullableValue>>> predicate() {
    return ((Optional<Predicate<Map<ColumnHandle, NullableValue>>>) invokeOptional("predicate"))
        .map(GravitinoPredicate::new);
  }

  // Not annotated @Override: see the note on predicate(). Constraint.getPredicateColumns() was also
  // removed in Trino 482.
  @SuppressWarnings("unchecked")
  public Optional<Set<ColumnHandle>> getPredicateColumns() {
    return ((Optional<Set<ColumnHandle>>) invokeOptional("getPredicateColumns"))
        .map(result -> result.stream().map(GravitinoHandle::unWrap).collect(Collectors.toSet()));
  }

  @Override
  public String toString() {
    return delegate.toString();
  }

  private Optional<?> invokeOptional(String methodName) {
    Method method =
        CONSTRAINT_METHODS.computeIfAbsent(
            methodName, GravitinoConstraint::resolveConstraintMethod);
    try {
      return (Optional<?>) method.invoke(delegate);
    } catch (ReflectiveOperationException e) {
      throw new IllegalStateException(
          "Failed invoking Trino SPI method Constraint#" + methodName, e);
    }
  }

  private static Method resolveConstraintMethod(String methodName) {
    try {
      return Constraint.class.getMethod(methodName);
    } catch (NoSuchMethodException e) {
      throw new IllegalStateException(
          "Trino SPI method Constraint#" + methodName + " was not found", e);
    }
  }
}
