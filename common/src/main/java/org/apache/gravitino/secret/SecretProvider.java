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
import org.apache.gravitino.annotation.DeveloperApi;

/** Service provider interface for secret backends. */
@DeveloperApi
public interface SecretProvider {

  /**
   * Initializes this provider after construction.
   *
   * <p>Implementations must override this method and explicitly decide whether configuration is
   * required. An empty body is acceptable for providers that need no setup.
   *
   * @param name the configured provider instance name
   * @param config provider-specific configuration (without the {@code gravitino.secret.provider.
   *     <name>.} prefix)
   */
  void initialize(String name, Map<String, String> config);

  /**
   * Returns the provider type identifier.
   *
   * @return the provider type
   */
  String type();

  /**
   * Writes a plaintext secret and returns its URN.
   *
   * @param plaintext the secret plaintext
   * @param context the write context
   * @return the secret URN
   */
  String writeSecret(String plaintext, SecretWriteContext context);

  /**
   * Reads a secret by URN.
   *
   * @param urn the secret URN
   * @return the secret plaintext
   */
  String readSecret(String urn);

  /**
   * Deletes a secret by URN.
   *
   * @param urn the secret URN
   */
  void deleteSecret(String urn);

  /**
   * Releases resources owned by this provider.
   *
   * <p>Implementations must override this method and explicitly release any held resources. An
   * empty body is acceptable when there is nothing to clean up.
   */
  void close();
}
