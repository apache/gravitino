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

import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.gravitino.exceptions.NoSuchCredentialException;

/**
 * Manages credentials in one catalog, including different credential providers, credential caching.
 */
public class CatalogCredentialManager {

  private Map<String, CredentialProvider> credentialProviderMap;

  public void initialize(Map<String, String> catalogProperties) {
    Set<String> credentialProviders =
        CredentialUtils.getCredentialProviders(() -> catalogProperties);

    this.credentialProviderMap =
        credentialProviders.stream()
            .collect(
                Collectors.toMap(
                    String::toString,
                    credentialType ->
                        CredentialProviderFactory.create(credentialType, catalogProperties)));
  }

  public Credential getCredential(String credentialType, CredentialContext context) {
    // todo: add credential cache
    CredentialProvider credentialProvider = credentialProviderMap.get(credentialType);
    if (credentialProvider == null) {
      throw new NoSuchCredentialException("No such credential: %s", credentialType);
    }
    return credentialProvider.getCredential(context);
  }
}
