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
package org.apache.gravitino.connector;

import java.util.Collections;
import java.util.Objects;
import java.util.Set;
import javax.annotation.Nullable;

/**
 * Property keys to mask or omit when building an API response.
 *
 * <p>{@code keysToMask} are returned as {@link HiddenPropertyMaskUtils#MASKED_VALUE}; {@code
 * keysToOmit} are dropped from the response map.
 */
public final class MaskAndOmitKeys {

  private static final MaskAndOmitKeys EMPTY =
      new MaskAndOmitKeys(Collections.emptySet(), Collections.emptySet());

  private final Set<String> keysToMask;
  private final Set<String> keysToOmit;

  private MaskAndOmitKeys(Set<String> keysToMask, Set<String> keysToOmit) {
    this.keysToMask = keysToMask;
    this.keysToOmit = keysToOmit;
  }

  /**
   * Returns an empty classification with no keys to mask or omit.
   *
   * @return the empty instance
   */
  public static MaskAndOmitKeys empty() {
    return EMPTY;
  }

  /**
   * Creates a classification of keys to mask and keys to omit.
   *
   * @param keysToMask keys whose values should be masked; {@code null} is treated as empty
   * @param keysToOmit keys that should be omitted; {@code null} is treated as empty
   * @return the classification
   */
  public static MaskAndOmitKeys of(
      @Nullable Set<String> keysToMask, @Nullable Set<String> keysToOmit) {
    Set<String> mask = keysToMask == null ? Collections.emptySet() : keysToMask;
    Set<String> omit = keysToOmit == null ? Collections.emptySet() : keysToOmit;
    if (mask.isEmpty() && omit.isEmpty()) {
      return EMPTY;
    }
    return new MaskAndOmitKeys(mask, omit);
  }

  /**
   * Returns keys whose values should be replaced with {@link HiddenPropertyMaskUtils#MASKED_VALUE}.
   *
   * @return keys to mask
   */
  public Set<String> keysToMask() {
    return keysToMask;
  }

  /**
   * Returns keys that should be omitted from the API response.
   *
   * @return keys to omit
   */
  public Set<String> keysToOmit() {
    return keysToOmit;
  }

  @Override
  public boolean equals(Object other) {
    if (this == other) {
      return true;
    }
    if (!(other instanceof MaskAndOmitKeys)) {
      return false;
    }
    MaskAndOmitKeys that = (MaskAndOmitKeys) other;
    return Objects.equals(keysToMask, that.keysToMask)
        && Objects.equals(keysToOmit, that.keysToOmit);
  }

  @Override
  public int hashCode() {
    return Objects.hash(keysToMask, keysToOmit);
  }

  @Override
  public String toString() {
    return "MaskAndOmitKeys{keysToMask=" + keysToMask + ", keysToOmit=" + keysToOmit + '}';
  }
}
