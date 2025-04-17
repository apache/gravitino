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

package org.apache.gravitino.listener.api.info;

import java.util.Optional;
import org.glassfish.jersey.internal.guava.Preconditions;

/**
 * Either represents a value of two possible types (a disjoint union).
 *
 * @param <L> Left type
 * @param <R> Right type
 */
public final class Either<L, R> {
  private final Optional<L> left;
  private final Optional<R> right;

  /**
   * Create a new {@code Either} instance with a left value.
   *
   * @param value Left value
   * @return Either with left value
   * @param <L> Left type
   * @param <R> Right type
   */
  public static <L, R> Either<L, R> left(L value) {
    Preconditions.checkArgument(value != null, "Left value cannot be null");
    return new Either<>(Optional.of(value), Optional.empty());
  }

  /**
   * Create a new {@code Either} instance with a right value.
   *
   * @param value Right value
   * @return Either with right value
   * @param <L> Left type
   * @param <R> Right type
   */
  public static <L, R> Either<L, R> right(R value) {
    Preconditions.checkArgument(value != null, "Right value cannot be null");
    return new Either<>(Optional.empty(), Optional.of(value));
  }

  /** Private constructor. */
  private Either(Optional<L> l, Optional<R> r) {
    left = l;
    right = r;
  }

  /**
   * Returns true if this is a left value.
   *
   * @return True if this is a left value
   */
  public boolean isLeft() {
    return left.isPresent();
  }

  /**
   * Returns true if this is a right value.
   *
   * @return True if this is a right value
   */
  public boolean isRight() {
    return right.isPresent();
  }

  /**
   * Returns the left value if this is a left value, otherwise throws an exception.
   *
   * @return Left value
   * @throws IllegalStateException if this is a right value
   */
  public L getLeft() {
    if (isRight()) {
      throw new IllegalStateException("Not a left value");
    }
    return left.get();
  }

  /**
   * Returns the right value if this is a right value, otherwise throws an exception.
   *
   * @return Right value
   * @throws IllegalStateException if this is a left value
   */
  public R getRight() {
    if (isLeft()) {
      throw new IllegalStateException("Not a right value");
    }
    return right.get();
  }
}
