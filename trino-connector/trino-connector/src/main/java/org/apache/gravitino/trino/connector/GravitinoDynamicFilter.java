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
