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
package org.apache.gravitino.bulk;

import java.util.Optional;
import javax.annotation.Nullable;

/** Represents the result of one item in a best-effort bulk operation. */
public final class BulkItemResult<T> {

  private final int index;
  private final String name;
  @Nullable private final T value;
  @Nullable private final Exception error;

  private BulkItemResult(int index, String name, @Nullable T value, @Nullable Exception error) {
    this.index = index;
    this.name = name;
    this.value = value;
    this.error = error;
  }

  /**
   * Creates a successful item result with a value.
   *
   * @param index The item index in the request.
   * @param name The item name.
   * @param value The successful value.
   * @return The successful item result.
   * @param <T> The successful value type.
   */
  public static <T> BulkItemResult<T> success(int index, String name, T value) {
    return new BulkItemResult<>(index, name, value, null);
  }

  /**
   * Creates a successful item result without a value.
   *
   * @param index The item index in the request.
   * @param name The item name.
   * @return The successful item result.
   * @param <T> The successful value type.
   */
  public static <T> BulkItemResult<T> success(int index, String name) {
    return new BulkItemResult<>(index, name, null, null);
  }

  /**
   * Creates a failed item result.
   *
   * @param index The item index in the request.
   * @param name The item name.
   * @param error The item-level error.
   * @return The failed item result.
   * @param <T> The successful value type.
   */
  public static <T> BulkItemResult<T> failure(int index, String name, Exception error) {
    return new BulkItemResult<>(index, name, null, error);
  }

  /**
   * Returns the item index in the request.
   *
   * @return The item index.
   */
  public int index() {
    return index;
  }

  /**
   * Returns the item name.
   *
   * @return The item name.
   */
  public String name() {
    return name;
  }

  /**
   * Returns whether the item succeeded.
   *
   * @return True if the item succeeded, otherwise false.
   */
  public boolean succeeded() {
    return error == null;
  }

  /**
   * Returns the successful value.
   *
   * @return The successful value.
   */
  public Optional<T> value() {
    return Optional.ofNullable(value);
  }

  /**
   * Returns the item-level error.
   *
   * @return The item-level error.
   */
  public Optional<Exception> error() {
    return Optional.ofNullable(error);
  }
}
