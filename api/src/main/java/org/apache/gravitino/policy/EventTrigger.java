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
package org.apache.gravitino.policy;

import com.google.common.base.Preconditions;
import java.util.Objects;

/**
 * Event-based policy trigger.
 *
 * <p>Currently supports Iceberg commit ({@code onCommit}). Additional event kinds may be added
 * later without changing the JSON property name {@code eventTrigger}.
 */
public final class EventTrigger {

  private final boolean onCommit;

  /** Default constructor for Jackson field-based deserialization only. */
  private EventTrigger() {
    this(false);
  }

  /**
   * Creates an event trigger.
   *
   * @param onCommit whether an Iceberg commit should evaluate / enqueue this policy
   */
  public EventTrigger(Boolean onCommit) {
    this.onCommit = onCommit != null && onCommit;
  }

  /**
   * @return whether commit-triggered evaluation is enabled
   */
  public boolean onCommit() {
    return onCommit;
  }

  /**
   * Validates that at least one event kind is enabled.
   *
   * @throws IllegalArgumentException if no event is enabled
   */
  public void validate() {
    Preconditions.checkArgument(
        onCommit, "eventTrigger must enable at least one event (e.g. onCommit=true)");
  }

  @Override
  public boolean equals(Object o) {
    if (!(o instanceof EventTrigger)) {
      return false;
    }
    EventTrigger that = (EventTrigger) o;
    return onCommit == that.onCommit;
  }

  @Override
  public int hashCode() {
    return Objects.hash(onCommit);
  }

  @Override
  public String toString() {
    return "EventTrigger{onCommit=" + onCommit + '}';
  }
}
