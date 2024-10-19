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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CredentialProviderFactory {
  private static final Logger LOG = LoggerFactory.getLogger(CredentialProviderFactory.class);

  public static CredentialProvider create(
      String credentialType, Map<String, String> catalogProperties) {
    Class<? extends CredentialProvider> providerClz = lookupCredentialProvider(credentialType);
    try {
      CredentialProvider provider = providerClz.getDeclaredConstructor().newInstance();
      provider.initialize(catalogProperties);
      return provider;
    } catch (Exception e) {
      LOG.warn("Create credential provider failed, {}", credentialType, e);
      throw new RuntimeException(e);
    }
  }

  private static Class<? extends CredentialProvider> lookupCredentialProvider(
      String credentialType) {
    ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
    ServiceLoader<CredentialProvider> serviceLoader =
        ServiceLoader.load(CredentialProvider.class, classLoader);
    List<Class<? extends CredentialProvider>> providers =
        Streams.stream(serviceLoader.iterator())
            .filter(
                credentialProvider ->
                    credentialType.equalsIgnoreCase(credentialProvider.credentialType()))
            .map(CredentialProvider::getClass)
            .collect(Collectors.toList());

    if (providers.isEmpty()) {
      throw new IllegalArgumentException("No credential provider found for: " + credentialType);
    } else if (providers.size() > 1) {
      throw new IllegalArgumentException(
          "Multiple credential providers found for: " + credentialType);
    } else {
      return Iterables.getOnlyElement(providers);
    }
  }
}
