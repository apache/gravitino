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
package org.apache.gravitino.rel.expressions.distributions;

import java.util.Arrays;
import org.apache.gravitino.annotation.Evolving;

/**
 * An enum that defines the distribution strategy.
 *
 * <p>The following strategies are supported:
 *
 * <ul>
 *   <li>Hash: Uses the hash value of the expression to distribute data.
 *   <li>Range: Uses the range of the expression specified to distribute data.
 *   <li>Even: Distributes data evenly across partitions.
 * </ul>
 */
@Evolving
public enum Strategy {
  /**
   * No distribution strategy. This is the default strategy. Will depend on the allocation strategy
   * of the underlying system.
   */
  NONE,

  /** Uses the hash value of the expression to distribute data. */
  HASH,

  /**
   * Uses the range of the expression specified to distribute data. The range is specified using the
   * rangeStart and rangeEnd properties.
   */
  RANGE,

  /** Distributes data evenly across partitions. */
  EVEN;

  /**
   * Get the distribution strategy by name.
   *
   * @param name The name of the distribution strategy.
   * @return The distribution strategy.
   */
  public static Strategy getByName(String name) {
    String upperName = name.toUpperCase();
    switch (upperName) {
      case "NONE":
        return NONE;
      case "HASH":
        return HASH;
      case "RANGE":
        return RANGE;
      case "EVEN":
      case "RANDOM":
        return EVEN;
      default:
        throw new IllegalArgumentException(
            "Invalid distribution strategy: "
                + name
                + ". Valid values are: "
                + Arrays.toString(Strategy.values()));
    }
  }
}
