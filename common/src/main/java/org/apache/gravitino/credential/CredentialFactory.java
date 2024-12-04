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

import com.google.common.collect.Iterables;
import com.google.common.collect.Streams;
import java.util.List;
import java.util.Map;
import java.util.ServiceLoader;
import java.util.stream.Collectors;

/** Create a specific credential according to the credential information. */
public class CredentialFactory {
  public static Credential create(
      String credentialType, Map<String, String> credentialInfo, long expireTimeInMs) {
    Class<? extends Credential> credentialClz = lookupCredential(credentialType);
    try {
      Credential credential = credentialClz.getDeclaredConstructor().newInstance();
      credential.initWithCredentialInfo(credentialInfo, expireTimeInMs);
      return credential;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private static Class<? extends Credential> lookupCredential(String credentialType) {
    ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
    ServiceLoader<Credential> serviceLoader = ServiceLoader.load(Credential.class, classLoader);
    List<Class<? extends Credential>> credentials =
        Streams.stream(serviceLoader.iterator())
            .filter(credential -> credentialType.equalsIgnoreCase(credential.credentialType()))
            .map(Credential::getClass)
            .collect(Collectors.toList());
    if (credentials.isEmpty()) {
      throw new RuntimeException("No credential found for: " + credentialType);
    } else if (credentials.size() > 1) {
      throw new RuntimeException("Multiple credential found for: " + credentialType);
    } else {
      return Iterables.getOnlyElement(credentials);
    }
  }
}
