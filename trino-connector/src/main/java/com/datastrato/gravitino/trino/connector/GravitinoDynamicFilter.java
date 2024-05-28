/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.trino.connector;

import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.DynamicFilter;
import io.trino.spi.predicate.TupleDomain;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

/** The GravitinoDynamicFilter is used to warp DynamicFilter */
class GravitinoDynamicFilter implements DynamicFilter {
  private final DynamicFilter delegate;

  GravitinoDynamicFilter(DynamicFilter dynamicFilter) {
    delegate = dynamicFilter;
  }

  @Override
  public Set<ColumnHandle> getColumnsCovered() {
    return delegate.getColumnsCovered();
  }

  @Override
  public CompletableFuture<?> isBlocked() {
    return delegate.isBlocked();
  }

  @Override
  public boolean isComplete() {
    return delegate.isComplete();
  }

  @Override
  public boolean isAwaitable() {
    return delegate.isAwaitable();
  }

  @Override
  public TupleDomain<ColumnHandle> getCurrentPredicate() {
    return delegate.getCurrentPredicate().transformKeys(GravitinoHandle::unWrap);
  }
}
