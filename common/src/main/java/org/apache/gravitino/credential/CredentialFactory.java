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

import java.util.Collections;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.ServiceLoader;

/** Create a specific credential according to the credential information. */
public class CredentialFactory {

  /**
   * Holder for the lazily-initialized, immutable mapping from credential type to its implementation
   * class. {@link Credential} is a fixed, built-in SPI (all implementations ship in the {@code api}
   * and {@code common} modules on the same class loader as this factory; custom credentials are
   * added through {@link CredentialProvider}, not by registering new {@link Credential} services),
   * so the {@link ServiceLoader} result is stable for the lifetime of the JVM and is safe to scan
   * once and cache instead of on every {@link #create} call.
   */
  private static class CredentialClassesHolder {
    private static final Map<String, Class<? extends Credential>> CREDENTIAL_CLASSES =
        loadCredentialClasses();

    private static Map<String, Class<? extends Credential>> loadCredentialClasses() {
      Map<String, Class<? extends Credential>> classes = new HashMap<>();
      for (Credential credential : ServiceLoader.load(Credential.class)) {
        String type = credential.credentialType().toLowerCase(Locale.ROOT);
        Class<? extends Credential> existing = classes.put(type, credential.getClass());
        if (existing != null) {
          throw new RuntimeException(
              "Multiple credentials found for: " + credential.credentialType());
        }
      }
      return Collections.unmodifiableMap(classes);
    }
  }

  /**
   * Creates a {@link Credential} instance based on the provided credential type, information, and
   * expiration time.
   *
   * @param credentialType The type of the credential to be created. This string is used to look up
   *     the corresponding credential class.
   * @param credentialInfo A {@link Map} containing key-value pairs of information needed to
   *     initialize the credential.
   * @param expireTimeInMs The expiration time of the credential in milliseconds.
   * @return A newly created and initialized {@link Credential} object.
   */
  public static Credential create(
      String credentialType, Map<String, String> credentialInfo, long expireTimeInMs) {
    Class<? extends Credential> credentialClz = lookupCredential(credentialType);
    try {
      Credential credential = credentialClz.getDeclaredConstructor().newInstance();
      credential.initialize(credentialInfo, expireTimeInMs);
      return credential;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private static Class<? extends Credential> lookupCredential(String credentialType) {
    Class<? extends Credential> credentialClz =
        CredentialClassesHolder.CREDENTIAL_CLASSES.get(credentialType.toLowerCase(Locale.ROOT));
    if (credentialClz == null) {
      throw new RuntimeException("No credential found for: " + credentialType);
    }
    return credentialClz;
  }
}
