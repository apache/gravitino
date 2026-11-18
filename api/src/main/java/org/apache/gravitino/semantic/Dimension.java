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
package org.apache.gravitino.semantic;

import java.util.Objects;
import javax.annotation.Nullable;
import org.apache.gravitino.annotation.Evolving;

/** Metadata that identifies a Semantic Model field as a dimension. */
@Evolving
public final class Dimension {

  @Nullable private final Boolean isTime;

  private Dimension(Builder builder) {
    this.isTime = builder.isTime;
  }

  /**
   * Creates a builder for dimension metadata.
   *
   * @return A new builder.
   */
  public static Builder builder() {
    return new Builder();
  }

  /**
   * Returns the explicit time-dimension role marker.
   *
   * @return {@code true} or {@code false} when explicitly set, or null when the role should use the
   *     datatype-dependent default.
   */
  @Nullable
  public Boolean isTime() {
    return isTime;
  }

  /**
   * Compares this dimension metadata with another object.
   *
   * @param other The object to compare.
   * @return {@code true} if the object has the same explicit time-dimension marker.
   */
  @Override
  public boolean equals(@Nullable Object other) {
    if (this == other) {
      return true;
    }
    if (!(other instanceof Dimension)) {
      return false;
    }
    Dimension that = (Dimension) other;
    return Objects.equals(isTime, that.isTime);
  }

  /**
   * Returns the hash code of this dimension metadata.
   *
   * @return The hash code.
   */
  @Override
  public int hashCode() {
    return Objects.hash(isTime);
  }

  /**
   * Returns a string representation of this dimension metadata.
   *
   * @return The string representation.
   */
  @Override
  public String toString() {
    return "Dimension{" + "isTime=" + isTime + '}';
  }

  /** A builder for {@link Dimension}. */
  public static final class Builder {

    @Nullable private Boolean isTime;

    private Builder() {}

    /**
     * Sets or clears the explicit time-dimension role marker.
     *
     * @param isTime The explicit marker, or null to leave it unset.
     * @return This builder.
     */
    public Builder withIsTime(@Nullable Boolean isTime) {
      this.isTime = isTime;
      return this;
    }

    /**
     * Builds dimension metadata.
     *
     * @return The immutable dimension metadata.
     */
    public Dimension build() {
      return new Dimension(this);
    }
  }
}
