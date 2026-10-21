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
import io.trino.spi.predicate.NullableValue;
import java.util.Map;
import java.util.function.Predicate;

/** The GravitinoPredicate is used to warp Predicate */
public class GravitinoPredicate implements Predicate<Map<ColumnHandle, NullableValue>> {

  private final Predicate delegate;

  GravitinoPredicate(Predicate<Map<ColumnHandle, NullableValue>> predicate) {
    this.delegate = predicate;
  }

  @Override
  public boolean test(Map<ColumnHandle, NullableValue> columnHandleNullableValueMap) {
    return delegate.test(columnHandleNullableValueMap);
  }

  @Override
  public Predicate<Map<ColumnHandle, NullableValue>> and(
      Predicate<? super Map<ColumnHandle, NullableValue>> other) {
    return delegate.and(other);
  }

  @Override
  public Predicate<Map<ColumnHandle, NullableValue>> negate() {
    return delegate.negate();
  }

  @Override
  public Predicate<Map<ColumnHandle, NullableValue>> or(
      Predicate<? super Map<ColumnHandle, NullableValue>> other) {
    return delegate.or(other);
  }
}
