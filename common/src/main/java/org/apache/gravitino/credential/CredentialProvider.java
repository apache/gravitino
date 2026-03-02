/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */

package org.apache.gravitino.credential;

import java.io.Closeable;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;

/**
 * An interface for providing credentials to access external systems.
 *
 * <p>Implementations of this interface are discovered using Java's {@link java.util.ServiceLoader}.
 * To prevent class loading issues and unnecessary dependency bloat, all implementations must be
 * lightweight. Any logic that requires heavy external dependencies (e.g., cloud SDKs) should be
 * isolated in a separate {@link CredentialGenerator} class and loaded reflectively at runtime. The
 * {@link CredentialProviderDelegator} provides a convenient base class for this pattern.
 */
public interface CredentialProvider extends Closeable {

  /**
   * Initializes the credential provider with catalog properties.
   *
   * @param properties catalog properties that can be used to configure the provider. The specific
   *     properties required vary by implementation.
   */
  void initialize(Map<String, String> properties);

  /**
   * Returns the type of credential, it should be identical in Gravitino.
   *
   * @return A string identifying the type of credentials.
   */
  String credentialType();

  /**
   * Gets a credential based on the provided context information.
   *
   * @param context A context object providing necessary information for retrieving credentials.
   * @return A Credential object containing the authentication information needed to access a system
   *     or resource. Null will be returned if no credential is available.
   */
  @Nullable
  Credential getCredential(CredentialContext context);

  /**
   * Returns the sensitive property keys used by this credential provider. These properties contain
   * sensitive data such as passwords, secret keys, access tokens, etc.
   *
   * <p>The returned property keys have the following effects:
   *
   * <ul>
   *   <li><b>Metadata registration:</b> Properties are automatically registered as sensitive in the
   *       catalog's PropertiesMetadata, enabling proper validation and handling.
   *   <li><b>API response filtering:</b> When the server configuration {@code
   *       gravitino.authorization.filterSensitiveProperties} is enabled (default: true), these
   *       properties are excluded from catalog API responses unless the requesting user has owner
   *       or alter permissions on the catalog.
   *   <li><b>Security protection:</b> Prevents accidental exposure of credentials in logs, API
   *       responses, and UI displays.
   * </ul>
   *
   * <p>Examples of sensitive properties include:
   *
   * <ul>
   *   <li>S3: {@code s3-access-key-id}, {@code s3-secret-access-key}
   *   <li>Azure: {@code azure-storage-client-secret}, {@code azure-storage-account-key}
   *   <li>OSS: {@code oss-access-key-id}, {@code oss-access-key-secret}
   * </ul>
   *
   * @return A set of sensitive property keys. Returns an empty set by default if this provider does
   *     not use any sensitive catalog properties.
   */
  default Set<String> sensitivePropertyKeys() {
    return Collections.emptySet();
  }
}
