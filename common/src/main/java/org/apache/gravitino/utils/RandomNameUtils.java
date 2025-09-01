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

import java.util.UUID;
import org.apache.commons.lang3.StringUtils;

/** Tools to generate random values. */
public class RandomNameUtils {
  private RandomNameUtils() {
    throw new IllegalStateException("Utility class");
  }

  /**
   * Generate a random string with the prefix.
   *
   * @param prefix Prefix of the random value.
   * @return A random string value.
   */
  public static String genRandomName(String prefix) {
    if (StringUtils.isBlank(prefix)) {
      throw new IllegalArgumentException("Prefix cannot be null or empty");
    }
    return prefix + "_" + UUID.randomUUID().toString().replace("-", "").substring(0, 8);
  }
}
