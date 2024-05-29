/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.trino.connector;

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