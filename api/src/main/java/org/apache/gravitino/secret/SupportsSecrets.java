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
package org.apache.gravitino.secret;

import java.util.Map;

/**
 * Interface to retrieve plaintext secret properties for a metadata object.
 *
 * <p>Returns:
 *
 * <ul>
 *   <li>Every secret-URN property value, resolved via the secret manager (including keys that may
 *       also be delivered via {@link org.apache.gravitino.credential.SupportsCredentials}).
 *   <li>Stored plaintext for property keys whose names look sensitive (contain {@code secret},
 *       {@code password}, {@code token}, {@code credential}, or {@code access}, case-insensitive),
 *       so mistyped / undeclared credential properties remain usable after API responses mask them
 *       as {@code ******}.
 * </ul>
 *
 * <p>Normal non-sensitive properties are not included; combine with {@code load*().properties()} on
 * the client.
 */
public interface SupportsSecrets {

  /**
   * Returns plaintext secret properties for this metadata object.
   *
   * @return a map of property key to plaintext value; never null, may be empty
   */
  Map<String, String> getSecrets();
}
