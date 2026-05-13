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
import java.net.URI;
import java.util.ArrayList;
import java.util.Map;
import java.util.Optional;
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

  public Optional<CredentialProvider> getCredentialProvider(String credentialType) {
    return Optional.ofNullable(credentialProviders.get(credentialType));
  }

  public Credential getCredentialByPath(String path, CredentialContext context) {
    String scheme = extractScheme(path);
    String matchedCredentialType = null;
    CredentialProvider matchedCredentialProvider = null;
    ArrayList<String> matchedCredentialTypes = new ArrayList<>();

    for (Map.Entry<String, CredentialProvider> entry : credentialProviders.entrySet()) {
      if (!entry.getValue().supportsScheme(scheme)) {
        continue;
      }

      matchedCredentialType = entry.getKey();
      matchedCredentialProvider = entry.getValue();
      matchedCredentialTypes.add(entry.getKey());
      if (matchedCredentialTypes.size() > 1) {
        break;
      }
    }

    if (matchedCredentialType == null) {
      throw new IllegalArgumentException(
          String.format("No credential provider found for path %s with scheme %s", path, scheme));
    }

    if (matchedCredentialTypes.size() > 1) {
      throw new UnsupportedOperationException(
          String.format(
              "Multiple credential providers found for path %s with scheme %s: %s",
              path, scheme, matchedCredentialTypes));
    }

    Optional<CredentialContext> filteredContext =
        CredentialOperationDispatcher.filterContextByProvider(matchedCredentialProvider, context);
    if (!filteredContext.isPresent()) {
      throw new IllegalArgumentException(
          String.format(
              "No supported path in credential context for provider %s, path %s with scheme %s",
              matchedCredentialType, path, scheme));
    }

    return getCredential(matchedCredentialType, filteredContext.get());
  }

  private String extractScheme(String path) {
    Preconditions.checkArgument(path != null, "Path should not be null");
    String scheme = URI.create(path).getScheme();
    if (scheme == null) {
      return "file";
    }
    return scheme;
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
