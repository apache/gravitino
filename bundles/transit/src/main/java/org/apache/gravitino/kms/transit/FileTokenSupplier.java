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
package org.apache.gravitino.kms.transit;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import org.apache.gravitino.exceptions.ConnectionFailedException;

final class FileTokenSupplier {

  private static final int MAX_TOKEN_BYTES = 16 * 1024;

  private final String providerName;
  private final Path tokenFile;
  private volatile String cachedToken;

  FileTokenSupplier(String providerName, Path tokenFile) {
    this.providerName = providerName;
    this.tokenFile = tokenFile;
  }

  String token() {
    String token = cachedToken;
    if (token != null) {
      return token;
    }
    synchronized (this) {
      if (cachedToken == null) {
        cachedToken = readToken();
      }
      return cachedToken;
    }
  }

  synchronized String reload() {
    cachedToken = readToken();
    return cachedToken;
  }

  private String readToken() {
    byte[] tokenBytes;
    try {
      if (Files.size(tokenFile) > MAX_TOKEN_BYTES) {
        throw new ConnectionFailedException("%s token file exceeds the allowed size", providerName);
      }
      tokenBytes = Files.readAllBytes(tokenFile);
    } catch (ConnectionFailedException e) {
      throw e;
    } catch (IOException | RuntimeException e) {
      throw new ConnectionFailedException(e, "Failed to read the %s token file", providerName);
    }

    if (tokenBytes.length > MAX_TOKEN_BYTES) {
      throw new ConnectionFailedException("%s token file exceeds the allowed size", providerName);
    }
    String token = new String(tokenBytes, StandardCharsets.UTF_8).trim();
    if (token.isEmpty() || token.indexOf('\r') >= 0 || token.indexOf('\n') >= 0) {
      throw new ConnectionFailedException("%s token file is empty or malformed", providerName);
    }
    return token;
  }
}
