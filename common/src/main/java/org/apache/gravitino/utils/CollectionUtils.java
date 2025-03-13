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

package org.apache.gravitino.utils;

import java.util.Collection;

/** Utility class for working with collection. */
public class CollectionUtils {
  private CollectionUtils() {}

  /**
   * Returns true if the two collections are equal.
   *
   * @param c1 the first collection, may be null
   * @param c2 the second collection, may be null
   * @return true if the two collections are equal
   */
  public static boolean isEqualCollection(Collection<?> c1, Collection<?> c2) {
    if (c1 == c2) {
      return true;
    }
    if (c1 == null || c2 == null) {
      return false;
    }
    return org.apache.commons.collections4.CollectionUtils.isEqualCollection(c1, c2);
  }
}
