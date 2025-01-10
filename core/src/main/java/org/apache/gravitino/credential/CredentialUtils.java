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
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.apache.gravitino.credential.config.CredentialConfig;

public class CredentialUtils {

  public static Map<String, CredentialProvider> loadCredentialProviders(
      Map<String, String> catalogProperties) {
    CredentialConfig credentialConfig = new CredentialConfig(catalogProperties);
    List<String> credentialProviders = credentialConfig.get(CredentialConfig.CREDENTIAL_PROVIDERS);

    return credentialProviders.stream()
        .collect(
            Collectors.toMap(
                String::toString,
                credentialType ->
                    CredentialProviderFactory.create(credentialType, catalogProperties)));
  }

  /**
   * Get Credential providers from properties supplier.
   *
   * <p>If there are multiple properties suppliers, will try to get the credential providers in the
   * input order.
   *
   * @param propertiesSuppliers The properties suppliers.
   * @return A set of credential providers.
   */
  public static Set<String> getCredentialProvidersByOrder(
      Supplier<Map<String, String>>... propertiesSuppliers) {

    for (Supplier<Map<String, String>> supplier : propertiesSuppliers) {
      Map<String, String> properties = supplier.get();
      Set<String> providers = getCredentialProvidersFromProperties(properties);
      if (!providers.isEmpty()) {
        return providers;
      }
    }

    return Collections.emptySet();
  }

  private static Set<String> getCredentialProvidersFromProperties(Map<String, String> properties) {
    if (properties == null) {
      return Collections.emptySet();
    }

    CredentialConfig credentialConfig = new CredentialConfig(properties);
    return credentialConfig.get(CredentialConfig.CREDENTIAL_PROVIDERS).stream()
        .collect(Collectors.toSet());
  }
}
