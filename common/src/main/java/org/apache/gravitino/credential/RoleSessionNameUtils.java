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

package org.apache.gravitino.credential;

import java.nio.charset.StandardCharsets;
import java.util.UUID;

/** Builds cloud STS role session names from arbitrary user names. */
public final class RoleSessionNameUtils {
  private static final int MAX_LENGTH = 64;

  private RoleSessionNameUtils() {}

  /**
   * Builds an ASCII session name with a stable suffix identifying the original user.
   *
   * @param prefix the provider's session name prefix
   * @param userName the authenticated user name
   * @return a session name accepted by AWS and Aliyun STS
   */
  public static String create(String prefix, String userName) {
    String suffix =
        "_"
            + UUID.nameUUIDFromBytes(userName.getBytes(StandardCharsets.UTF_8))
                .toString()
                .substring(0, 16);
    String cleaned = userName.replaceAll("[^A-Za-z0-9_@.-]", "_");
    int available = MAX_LENGTH - prefix.length() - suffix.length();
    if (available < 1) {
      throw new IllegalArgumentException("Role session prefix is too long");
    }
    return prefix + cleaned.substring(0, Math.min(cleaned.length(), available)) + suffix;
  }
}
