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
package org.apache.gravitino.stats;

import org.apache.gravitino.annotation.Evolving;

/**
 * Represents a statistic value in statistics. The value can be of different types, such as Integer,
 * Double, String, etc.
 *
 * @param <T> the type of the statistic value, which can be Long, Double, String, List, Map, or
 *     Boolean.
 */
@Evolving
public interface StatisticValue<T> {

  /**
   * Returns the type of the statistic value.
   *
   * @return The type of the statistic value.
   */
  Type type();

  /**
   * Returns the actual value of the statistic.
   *
   * @return The actual value of the statistic.
   */
  T value();

  /** Enumeration representing the different types of statistic values. */
  enum Type {
    /** Represents an integer value. */
    LONG,
    /** Represents a decimal value. */
    DECIMAL,
    /** Represents a string value. */
    STRING,
    /** Represents a list of statistic values. */
    LIST,
    /** Represents a map of statistic values. */
    MAP,
    /** Represents a boolean value. */
    BOOLEAN
  }
}
