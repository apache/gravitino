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
package org.apache.gravitino.server.web.rest;

import java.io.IOException;
import java.security.GeneralSecurityException;
import java.security.SecureRandom;
import java.util.Arrays;
import java.util.Base64;
import javax.crypto.Cipher;
import javax.crypto.SecretKey;
import javax.crypto.spec.GCMParameterSpec;
import javax.crypto.spec.SecretKeySpec;
import org.apache.gravitino.glossary.GlossaryManager.BindingCursor;
import org.apache.gravitino.json.JsonUtils;

/**
 * Authenticated, opaque continuations for glossary binding scans. Shared by all requests on a
 * server.
 */
public final class BindingPageTokens {
  private static final int IV_SIZE = 12;
  private static final int KEY_SIZE = 32;
  private static final int TAG_BITS = 128;
  private static final String PREFIX = "b1.";
  private final SecureRandom random = new SecureRandom();
  private final SecretKey key;

  /**
   * Creates the codec with a base64-encoded 256-bit cluster secret, or a process key when empty.
   */
  public BindingPageTokens(String secret) {
    byte[] bytes;
    if (secret.isEmpty()) {
      bytes = new byte[KEY_SIZE];
      random.nextBytes(bytes);
    } else {
      try {
        bytes = Base64.getDecoder().decode(secret);
      } catch (IllegalArgumentException e) {
        throw new IllegalArgumentException("Invalid glossary binding page-token key");
      }
      if (bytes.length != KEY_SIZE) {
        throw new IllegalArgumentException("Glossary binding page-token key must be 256 bits");
      }
    }
    key = new SecretKeySpec(bytes, "AES");
  }

  String encode(BindingCursor cursor) {
    byte[] iv = new byte[IV_SIZE];
    random.nextBytes(iv);
    try {
      byte[] encrypted =
          cipher(Cipher.ENCRYPT_MODE, iv)
              .doFinal(JsonUtils.objectMapper().writeValueAsBytes(cursor));
      byte[] token = Arrays.copyOf(iv, iv.length + encrypted.length);
      System.arraycopy(encrypted, 0, token, iv.length, encrypted.length);
      return PREFIX + Base64.getUrlEncoder().withoutPadding().encodeToString(token);
    } catch (GeneralSecurityException | IOException e) {
      throw new IllegalStateException("Cannot encode glossary binding continuation", e);
    }
  }

  BindingCursor decode(String token) {
    if (!token.startsWith(PREFIX) || token.length() > 4096) {
      throw invalidToken();
    }
    try {
      String encoded = token.substring(PREFIX.length());
      byte[] bytes = Base64.getUrlDecoder().decode(encoded);
      if (bytes.length <= IV_SIZE + TAG_BITS / 8
          || !Base64.getUrlEncoder().withoutPadding().encodeToString(bytes).equals(encoded)) {
        throw invalidToken();
      }
      byte[] content =
          cipher(Cipher.DECRYPT_MODE, Arrays.copyOf(bytes, IV_SIZE))
              .doFinal(bytes, IV_SIZE, bytes.length - IV_SIZE);
      return JsonUtils.objectMapper().readValue(content, BindingCursor.class);
    } catch (GeneralSecurityException | IOException | IllegalArgumentException e) {
      throw invalidToken();
    }
  }

  private Cipher cipher(int mode, byte[] iv) throws GeneralSecurityException {
    Cipher cipher = Cipher.getInstance("AES/GCM/NoPadding");
    cipher.init(mode, key, new GCMParameterSpec(TAG_BITS, iv));
    return cipher;
  }

  private static IllegalArgumentException invalidToken() {
    return new IllegalArgumentException("Invalid glossary binding pageToken");
  }
}
