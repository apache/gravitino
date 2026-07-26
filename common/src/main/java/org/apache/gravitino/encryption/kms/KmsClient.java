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
package org.apache.gravitino.encryption.kms;

import java.util.Optional;
import org.apache.gravitino.annotation.DeveloperApi;
import org.apache.gravitino.exceptions.ConnectionFailedException;

/**
 * Inspects keys managed by a configured KMS.
 *
 * <p>This is a server-side operation client, not a credential-vending API. Provider credentials
 * authenticate calls made by the client and must never be returned to callers. This client does not
 * perform cryptographic operations.
 */
@DeveloperApi
public interface KmsClient extends AutoCloseable {

  /**
   * Reads the provider-reported properties of a key.
   *
   * <p>An empty result means the provider authoritatively reported that the key does not exist.
   * Authentication, authorization, timeout, availability, and other indeterminate failures are
   * reported as exceptions, never as an empty result.
   *
   * @param reference non-null key to inspect
   * @return normalized key properties, or empty when the key authoritatively does not exist; never
   *     null
   * @throws IllegalArgumentException if the reference is null or does not belong to this client
   * @throws ConnectionFailedException if the provider cannot be queried
   */
  Optional<KmsKeyProperties> getKeyProperties(KmsReference reference);

  /** Releases resources owned by this client. */
  @Override
  default void close() {}
}
