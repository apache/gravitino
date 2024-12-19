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

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import org.apache.gravitino.NameIdentifier;

/**
 * This interface defines how to get credentials, the catalog operation must implement this
 * interface to support credential vending.
 */
public interface SupportsCredentialOperations {

  /**
   * Get the mapping of credential provider and {@link CredentialContext}.
   *
   * <p>In most cases, there will be only one element in the map, For fileset catalog which supports
   * multiple credentials, there will be multiple elements.
   *
   * @param nameIdentifier The name identifier for the catalog, fileset, table, etc.
   * @param privilege The credential privilege object.
   * @return A map with credential provider and {@link CredentialContext}.
   */
  Map<String, CredentialContext> getCredentialContexts(
      NameIdentifier nameIdentifier, CredentialPrivilege privilege);

  /**
   * Gets the catalog credential manager.
   *
   * @return An instance of {@link CatalogCredentialManager}.
   */
  CatalogCredentialManager getCatalogCredentialManager();

  /**
   * Obtains the corresponding list of credentials based on the name identifier and the credential
   * privilege.
   *
   * @param nameIdentifier The name identifier for the catalog, fileset, table, etc.
   * @param privilege The credential privilege object.
   * @return A list of {@link Credential}.
   */
  default List<Credential> getCredentials(
      NameIdentifier nameIdentifier, CredentialPrivilege privilege) {
    Map<String, CredentialContext> contexts = getCredentialContexts(nameIdentifier, privilege);
    CatalogCredentialManager catalogCredentialManager = getCatalogCredentialManager();
    return contexts.entrySet().stream()
        .map(entry -> catalogCredentialManager.getCredential(entry.getKey(), entry.getValue()))
        .filter(Objects::nonNull)
        .collect(Collectors.toList());
  }
}
