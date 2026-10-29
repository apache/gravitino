/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.gravitino.integration.test.container;

/** The Hive version a {@link HiveFixture} runs. Both versions run from the same image. */
public enum HiveVersion {
  /** Hive 2, the image default. */
  HIVE2(HiveContainer.HIVE2),
  /** Hive 3. */
  HIVE3(HiveContainer.HIVE3);

  private final String runtimeVersion;

  HiveVersion(String runtimeVersion) {
    this.runtimeVersion = runtimeVersion;
  }

  /**
   * Returns the value of the image's {@link HiveContainer#HIVE_RUNTIME_VERSION} variable.
   *
   * @return the runtime version, such as {@code hive2}
   */
  public String runtimeVersion() {
    return runtimeVersion;
  }
}
