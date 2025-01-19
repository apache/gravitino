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

import com.google.common.base.Preconditions;
import java.io.Closeable;
import java.io.IOException;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Manage lifetime of the credential provider in one catalog, dispatch credential request to the
 * corresponding credential provider.
 */
public class CatalogCredentialManager implements Closeable {

  private static final Logger LOG = LoggerFactory.getLogger(CatalogCredentialManager.class);

  private final CredentialCache<CredentialCacheKey> credentialCache;

  private final String catalogName;
  private final Map<String, CredentialProvider> credentialProviders;

  public CatalogCredentialManager(String catalogName, Map<String, String> catalogProperties) {
    this.catalogName = catalogName;
    this.credentialProviders = CredentialUtils.loadCredentialProviders(catalogProperties);
    this.credentialCache = new CredentialCache();
    credentialCache.initialize(catalogProperties);
  }

  public Credential getCredential(String credentialType, CredentialContext context) {
    CredentialCacheKey credentialCacheKey = new CredentialCacheKey(credentialType, context);
    return credentialCache.getCredential(credentialCacheKey, cacheKey -> doGetCredential(cacheKey));
  }

  // Get credential with only one credential provider.
  public Credential getCredential(CredentialContext context) {
    if (credentialProviders.size() == 0) {
      throw new IllegalArgumentException("There are no credential provider for the catalog.");
    } else if (credentialProviders.size() > 1) {
      throw new UnsupportedOperationException(
          "There are multiple credential providers for the catalog.");
    }
    return getCredential(credentialProviders.keySet().iterator().next(), context);
  }

  @Override
  public void close() {
    credentialProviders
        .values()
        .forEach(
            credentialProvider -> {
              try {
                credentialProvider.close();
              } catch (IOException e) {
                LOG.warn(
                    "Close credential provider failed, catalog: {}, credential provider: {}",
                    catalogName,
                    credentialProvider.credentialType(),
                    e);
              }
            });
    try {
      credentialCache.close();
    } catch (IOException e) {
      LOG.warn("Close credential cache failed, catalog: {}", catalogName, e);
    }
  }

  private Credential doGetCredential(CredentialCacheKey credentialCacheKey) {
    String credentialType = credentialCacheKey.getCredentialType();
    CredentialContext context = credentialCacheKey.getCredentialContext();
    LOG.debug("Try get credential, credential type: {}, context: {}.", credentialType, context);
    Preconditions.checkState(
        credentialProviders.containsKey(credentialType),
        String.format("Credential %s not found", credentialType));
    return credentialProviders.get(credentialType).getCredential(context);
  }
}
