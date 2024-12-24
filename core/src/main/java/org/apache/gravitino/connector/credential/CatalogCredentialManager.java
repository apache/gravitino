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

package org.apache.gravitino.connector.credential;

import com.google.common.base.Preconditions;
import java.io.Closeable;
import java.io.IOException;
import java.util.Map;
import org.apache.gravitino.catalog.CatalogManager;
import org.apache.gravitino.credential.Credential;
import org.apache.gravitino.credential.CredentialProvider;
import org.apache.gravitino.credential.CredentialUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Manages credentials in one catalog. */
public class CatalogCredentialManager implements Closeable {

  private static final Logger LOG = LoggerFactory.getLogger(CatalogManager.class);

  private Map<String, CredentialProvider> credentialProviders;
  private String catalogName;

  public CatalogCredentialManager(String catalogName, Map<String, String> catalogProperties) {
    this.catalogName = catalogName;
    this.credentialProviders = CredentialUtils.loadCredentialProviders(catalogProperties);
  }

  public Credential getCredential(String credentialType, CredentialContext context) {
    // todo: add credential cache
    Preconditions.checkState(
        credentialProviders.containsKey(credentialType),
        String.format("Credential %s not found", credentialType));
    return credentialProviders.get(credentialType).getCredential(context);
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
  }
}
