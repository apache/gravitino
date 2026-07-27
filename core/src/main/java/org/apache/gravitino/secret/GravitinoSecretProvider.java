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

/** Service provider interface for Gravitino secret backends. */
public interface GravitinoSecretProvider {

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
   * Builds a URN for an externally referenced secret.
   *
   * @param propertyKey the property key
   * @param locator the external secret locator
   * @return the secret URN
   */
  default String buildExternalReferenceUrn(String propertyKey, SecretReferenceLocator locator) {
    throw new UnsupportedOperationException(
        String.format("Provider %s does not support external secret references", type()));
  }
}
