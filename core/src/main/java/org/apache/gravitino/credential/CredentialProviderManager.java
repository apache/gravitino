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
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import javax.annotation.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CredentialProviderManager {

  private static final Logger LOG = LoggerFactory.getLogger(CredentialProviderManager.class);
  private Map<String, CredentialProvider> credentialProviders;

  public CredentialProviderManager() {
    this.credentialProviders = new ConcurrentHashMap<>();
  }

  public void registerCredentialProvider(
      String catalogName, CredentialProvider credentialProvider) {
    CredentialProvider current = credentialProviders.putIfAbsent(catalogName, credentialProvider);
    Preconditions.checkState(
        !credentialProvider.equals(current),
        String.format(
            "Should not register multiple times to CredentialProviderManager, catalog: %s, "
                + "credential provider: %s",
            catalogName, credentialProvider.credentialType()));
    LOG.info(
        "Register catalog:%s credential provider:%s to CredentialProviderManager",
        catalogName, credentialProvider.credentialType());
  }

  public void unregisterCredentialProvider(String catalogName) {
    CredentialProvider credentialProvider = credentialProviders.remove(catalogName);
    // Not all catalog has credential provider
    if (credentialProvider != null) {
      LOG.info(
          "Unregister catalog:{} credential provider:{} to CredentialProviderManager",
          catalogName,
          credentialProvider.credentialType());
      try {
        credentialProvider.close();
      } catch (IOException e) {
        LOG.warn("Close credential provider failed", e);
      }
    }
  }

  @Nullable
  public CredentialProvider getCredentialProvider(String catalogName) {
    return credentialProviders.get(catalogName);
  }
}
