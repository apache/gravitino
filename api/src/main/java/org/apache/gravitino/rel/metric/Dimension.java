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
package org.apache.gravitino.rel.metric;

import java.util.Objects;
import javax.annotation.Nullable;
import org.apache.gravitino.annotation.Unstable;

/** Optional dimensional metadata for a metric field. */
@Unstable
public final class Dimension {

  @Nullable private final Boolean time;

  private Dimension(@Nullable Boolean time) {
    this.time = time;
  }

  /**
   * Creates a builder for dimensional metadata.
   *
   * @return A new builder.
   */
  public static Builder builder() {
    return new Builder();
  }

  /**
   * Returns whether this is a time dimension.
   *
   * @return The time flag, or {@code null} when unspecified.
   */
  @Nullable
  public Boolean isTime() {
    return time;
  }

  @Override
  public boolean equals(Object other) {
    if (this == other) {
      return true;
    }
    if (!(other instanceof Dimension)) {
      return false;
    }
    Dimension that = (Dimension) other;
    return Objects.equals(time, that.time);
  }

  @Override
  public int hashCode() {
    return Objects.hash(time);
  }

  @Override
  public String toString() {
    return "Dimension{" + "time=" + time + '}';
  }

  /** Builder for {@link Dimension}. */
  public static final class Builder {
    @Nullable private Boolean time;

    private Builder() {}

    /**
     * Sets whether this is a time dimension.
     *
     * @param time The time flag.
     * @return This builder.
     */
    public Builder withTime(@Nullable Boolean time) {
      this.time = time;
      return this;
    }

    /**
     * Builds the dimensional metadata.
     *
     * @return The dimensional metadata.
     */
    public Dimension build() {
      return new Dimension(time);
    }
  }
}
