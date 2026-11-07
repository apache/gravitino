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
package org.apache.gravitino.encryption.seal;

import org.apache.gravitino.annotation.DeveloperApi;

/**
 * Wraps and unwraps key material using a key held by a seal backend, such as an external KMS or a
 * local key file.
 *
 * <p>The seal primitive exists so envelope encryption can protect the Gravitino master key: the
 * backend is exercised once per process start to unwrap the master key, while the runtime hot path
 * stays local. Unlike the read-only {@code KmsClient} inspection contract, this interface performs
 * cryptographic operations.
 *
 * <p>Implementations must never log key material: neither the plaintext passed to {@link
 * #wrap(byte[])}, the plaintext returned by {@link #unwrap(byte[])}, nor any backing key bytes.
 * Diagnostics may include provider names, key identifiers, and blob sizes only.
 *
 * <p>Implementations must support concurrent operation calls on one client instance. Operation
 * failures must not close the client; later calls reuse the same instance.
 */
@DeveloperApi
public interface SealClient extends AutoCloseable {

  /**
   * Encrypts (wraps) plaintext key material under the backing seal key.
   *
   * @param plaintext non-null, non-empty key material to wrap; the caller retains ownership of the
   *     array and should zero it when it is no longer needed
   * @return the opaque wrapped blob; never null
   * @throws IllegalArgumentException if the plaintext is null or empty
   * @throws IllegalStateException if the client is closed
   * @throws SealOperationException if the backend fails to wrap the plaintext
   */
  byte[] wrap(byte[] plaintext);

  /**
   * Decrypts (unwraps) a blob previously produced by {@link #wrap(byte[])}.
   *
   * @param wrapped non-null blob produced by an earlier {@code wrap} call
   * @return the original plaintext key material; the caller should zero the returned array when it
   *     is no longer needed
   * @throws IllegalArgumentException if the blob is null or malformed
   * @throws IllegalStateException if the client is closed
   * @throws SealOperationException if the blob cannot be unwrapped, for example because it was
   *     wrapped under a different key or was corrupted
   */
  byte[] unwrap(byte[] wrapped);

  /**
   * Releases resources owned by this client and destroys held key material on a best-effort basis.
   * This operation is idempotent.
   */
  @Override
  default void close() {}
}
