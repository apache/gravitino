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

import com.google.common.base.Preconditions;
import java.util.Objects;
import javax.annotation.Nullable;
import org.apache.gravitino.annotation.Evolving;

/** Additional context for AI tools, represented as either a string or an object. */
@Evolving
public final class AIContext {

  @Nullable private final String text;
  @Nullable private final AIContextObject object;

  private AIContext(String text) {
    this.text = text;
    this.object = null;
  }

  private AIContext(AIContextObject object) {
    this.text = null;
    this.object = object;
  }

  /**
   * Creates AI context from a string value.
   *
   * @param value The string context.
   * @return The AI context.
   * @throws IllegalArgumentException If the value is null.
   */
  public static AIContext of(String value) {
    Preconditions.checkArgument(value != null, "AI context string must not be null");
    return new AIContext(value);
  }

  /**
   * Creates AI context from a structured object.
   *
   * @param value The object context.
   * @return The AI context.
   * @throws IllegalArgumentException If the value is null.
   */
  public static AIContext of(AIContextObject value) {
    Preconditions.checkArgument(value != null, "AI context object must not be null");
    return new AIContext(value);
  }

  /**
   * Returns whether this context contains a string value.
   *
   * @return {@code true} when this context contains a string.
   */
  public boolean isText() {
    return text != null;
  }

  /**
   * Returns the string context when this is the string variant.
   *
   * @return The string context, or null for the object variant.
   */
  @Nullable
  public String text() {
    return text;
  }

  /**
   * Returns the structured context when this is the object variant.
   *
   * @return The object context, or null for the string variant.
   */
  @Nullable
  public AIContextObject object() {
    return object;
  }

  /**
   * Compares this AI context with another object.
   *
   * @param other The object to compare.
   * @return {@code true} if the object contains the same context variant and value.
   */
  @Override
  public boolean equals(@Nullable Object other) {
    if (this == other) {
      return true;
    }
    if (!(other instanceof AIContext)) {
      return false;
    }
    AIContext that = (AIContext) other;
    return Objects.equals(text, that.text) && Objects.equals(object, that.object);
  }

  /**
   * Returns the hash code of this AI context.
   *
   * @return The hash code.
   */
  @Override
  public int hashCode() {
    return Objects.hash(text, object);
  }

  /**
   * Returns a string representation of this AI context.
   *
   * @return The string representation.
   */
  @Override
  public String toString() {
    return "AIContext{" + "text=" + text + ", object=" + object + '}';
  }
}
