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
import io.trino.spi.predicate.TupleDomain;
import java.util.Map;

/**
 * The GravitinoConstraint is used to warp Constraint.
 *
 * <p>Trino 482 removed {@code Constraint.predicate()} and {@code getPredicateColumns()}, so only
 * the summary, expression and assignments are unwrapped.
 */
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
  public String toString() {
    return delegate.toString();
  }
}
