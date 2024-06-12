/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.trino.connector;

import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.Constraint;
import io.trino.spi.expression.ConnectorExpression;
import io.trino.spi.predicate.NullableValue;
import io.trino.spi.predicate.TupleDomain;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;

/** The GravitinoConstraint is used to warp Constraint */
public class GravitinoConstraint extends Constraint {
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

  @Override
  public Optional<Predicate<Map<ColumnHandle, NullableValue>>> predicate() {
    return delegate.predicate().map(GravitinoPredicate::new);
  }

  @Override
  public Optional<Set<ColumnHandle>> getPredicateColumns() {
    return delegate
        .getPredicateColumns()
        .map(result -> result.stream().map(GravitinoHandle::unWrap).collect(Collectors.toSet()));
  }

  @Override
  public String toString() {
    return delegate.toString();
  }
}
