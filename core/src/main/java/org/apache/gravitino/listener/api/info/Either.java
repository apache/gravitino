/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 *  http://aws.amazon.com/apache2.0
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

package org.apache.gravitino.listener.api.info;

import com.google.common.base.Preconditions;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * Represents a value that can be one of two types.
 *
 * @param <L> Left type
 * @param <R> Right type
 */
public final class Either<L, R> {

  private final Optional<L> left;
  private final Optional<R> right;

  private Either(Optional<L> l, Optional<R> r) {
    left = l;
    right = r;
  }

  /**
   * Maps the Either to a type and returns the resolved value (which may be from the left or the
   * right value).
   *
   * @param lFunc Function that maps the left value if present.
   * @param rFunc Function that maps the right value if present.
   * @param <T> Type that both the left and right should be mapped to.
   * @return Mapped value from either lFunc or rFunc depending on which value is present.
   */
  public <T> T map(Function<? super L, ? extends T> lFunc, Function<? super R, ? extends T> rFunc) {
    return left.<T>map(lFunc).orElseGet(() -> right.map(rFunc).get());
  }

  /**
   * Map the left most value and return a new Either reflecting the new types.
   *
   * @param lFunc Function that maps the left value if present.
   * @param <T> New type of left value.
   * @return New Either bound to the new left type and the same right type.
   */
  public <T> Either<T, R> mapLeft(Function<? super L, ? extends T> lFunc) {
    return new Either<>(left.map(lFunc), right);
  }

  /**
   * Map the right most value and return a new Either reflecting the new types.
   *
   * @param rFunc Function that maps the right value if present.
   * @param <T> New type of right value.
   * @return New Either bound to the same left type and the new right type.
   */
  public <T> Either<L, T> mapRight(Function<? super R, ? extends T> rFunc) {
    return new Either<>(left, right.map(rFunc));
  }

  /**
   * Apply the consumers to the left or the right value depending on which is present.
   *
   * @param lFunc Consumer of left value, invoked if left value is present.
   * @param rFunc Consumer of right value, invoked if right value is present.
   */
  public void apply(Consumer<? super L> lFunc, Consumer<? super R> rFunc) {
    left.ifPresent(lFunc);
    right.ifPresent(rFunc);
  }

  /**
   * Create a new Either with the left type.
   *
   * @param value Left value
   * @param <L> Left type
   * @param <R> Right type
   * @return an Either instance with left value
   */
  public static <L, R> Either<L, R> left(L value) {
    Preconditions.checkArgument(value != null, "Left value cannot be null");
    return new Either<>(Optional.of(value), Optional.empty());
  }

  /**
   * Create a new Either with the right type.
   *
   * @param value Right value
   * @param <L> Left type
   * @param <R> Right type
   * @return an Either instance with right value
   */
  public static <L, R> Either<L, R> right(R value) {
    Preconditions.checkArgument(value != null, "Right value cannot be null");
    return new Either<>(Optional.empty(), Optional.of(value));
  }

  /** @return the left value */
  public Optional<L> left() {
    return left;
  }

  /** @return the right value */
  public Optional<R> right() {
    return right;
  }

  /**
   * Create a new {@code Optional<Either>} from two possibly null values.
   *
   * <p>If both values are null, {@link Optional#empty()} is returned. Only one of the left or right
   * values is allowed to be non-null, otherwise an {@link IllegalArgumentException} is thrown.
   *
   * @param left The left value (possibly null)
   * @param right The right value (possibly null)
   * @param <L> Left type
   * @param <R> Right type
   * @return an Optional Either representing one of the two values or empty if both are null
   */
  public static <L, R> Optional<Either<L, R>> fromNullable(L left, R right) {
    if (left != null && right == null) {
      return Optional.of(left(left));
    }
    if (left == null && right != null) {
      return Optional.of(right(right));
    }
    if (left == null && right == null) {
      return Optional.empty();
    }
    throw new IllegalArgumentException(
        String.format(
            "Only one of either left or right should be non-null. " + "Got (left: %s, right: %s)",
            left, right));
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof Either)) {
      return false;
    }

    Either<?, ?> either = (Either<?, ?>) o;

    return left.equals(either.left) && right.equals(either.right);
  }

  @Override
  public int hashCode() {
    return 31 * left.hashCode() + right.hashCode();
  }
}
