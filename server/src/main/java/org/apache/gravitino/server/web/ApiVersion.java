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
package org.apache.gravitino.server.web;

import java.util.Comparator;
import java.util.TreeSet;

public enum ApiVersion {
  V_1(1);

  private static final TreeSet<ApiVersion> VERSIONS =
      new TreeSet<>(Comparator.comparingInt(o -> o.version));

  static {
    VERSIONS.add(V_1);
  }

  private final int version;

  ApiVersion(int version) {
    this.version = version;
  }

  public int version() {
    return version;
  }

  public static ApiVersion latestVersion() {
    return VERSIONS.last();
  }

  public static boolean isSupportedVersion(int version) {
    for (ApiVersion v : ApiVersion.values()) {
      if (v.version == version) {
        return true;
      }
    }

    return false;
  }
}
