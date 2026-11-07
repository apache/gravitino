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
package org.apache.gravitino.encryption.seal.local;

import java.nio.file.InvalidPathException;
import java.nio.file.Paths;
import java.util.Map;
import org.apache.gravitino.encryption.seal.SealClient;
import org.apache.gravitino.encryption.seal.SealClientFactory;
import org.apache.gravitino.encryption.seal.SealConfigurationException;

/**
 * Creates a seal client backed by a symmetric AES key file on local disk. Intended for development
 * and tests only; production deployments should use a cloud KMS seal implementation.
 *
 * <p>Configuration:
 *
 * <pre>{@code
 * gravitino.seal.providers=local
 * gravitino.seal.provider.local.className=org.apache.gravitino.encryption.seal.local.LocalSealClientFactory
 * gravitino.seal.provider.local.keyFile=/path/to/key.b64
 * }</pre>
 *
 * <p>The key file holds the Base64 encoding of a 128, 192, or 256-bit AES key and is protected only
 * by filesystem permissions.
 */
public final class LocalSealClientFactory implements SealClientFactory {

  /** Provider property holding the path of the Base64-encoded AES key file. */
  public static final String KEY_FILE = "keyFile";

  /** Creates a factory instance. */
  public LocalSealClientFactory() {}

  @Override
  public SealClient create(String provider, Map<String, String> properties) {
    String keyFile = properties == null ? null : properties.get(KEY_FILE);
    if (keyFile == null || keyFile.trim().isEmpty()) {
      throw new SealConfigurationException(
          "Local seal provider '%s' requires property '%s' naming a Base64-encoded AES key file",
          provider, KEY_FILE);
    }

    try {
      return new LocalSealClient(Paths.get(keyFile.trim()));
    } catch (InvalidPathException e) {
      throw new SealConfigurationException(
          e, "Local seal provider '%s' has an invalid key file path", provider);
    }
  }
}
