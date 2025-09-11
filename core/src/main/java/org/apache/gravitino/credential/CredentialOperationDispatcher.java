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

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import javax.ws.rs.NotAuthorizedException;
import javax.ws.rs.NotSupportedException;
import org.apache.gravitino.EntityStore;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.catalog.CatalogManager;
import org.apache.gravitino.catalog.OperationDispatcher;
import org.apache.gravitino.connector.BaseCatalog;
import org.apache.gravitino.connector.credential.PathContext;
import org.apache.gravitino.connector.credential.SupportsPathBasedCredentials;
import org.apache.gravitino.exceptions.NoSuchCatalogException;
import org.apache.gravitino.storage.IdGenerator;
import org.apache.gravitino.utils.NameIdentifierUtil;
import org.apache.gravitino.utils.PrincipalUtils;

/** Get credentials with the specific catalog classloader. */
public class CredentialOperationDispatcher extends OperationDispatcher {

  public CredentialOperationDispatcher(
      CatalogManager catalogManager, EntityStore store, IdGenerator idGenerator) {
    super(catalogManager, store, idGenerator);
  }

  public List<Credential> getCredentials(NameIdentifier identifier) {
    CredentialPrivilege privilege =
        getCredentialPrivilege(PrincipalUtils.getCurrentUserName(), identifier);
    return doWithCatalog(
        NameIdentifierUtil.getCatalogIdentifier(identifier),
        catalogWrapper ->
            catalogWrapper.doWithCredentialOps(
                baseCatalog -> getCredentials(baseCatalog, identifier, privilege)),
        NoSuchCatalogException.class);
  }

  private List<Credential> getCredentials(
      BaseCatalog baseCatalog, NameIdentifier nameIdentifier, CredentialPrivilege privilege) {
    Map<String, CredentialContext> contexts =
        getCredentialContexts(baseCatalog, nameIdentifier, privilege);
    return contexts.entrySet().stream()
        .map(
            entry ->
                baseCatalog
                    .catalogCredentialManager()
                    .getCredential(entry.getKey(), entry.getValue()))
        .filter(Objects::nonNull)
        .collect(Collectors.toList());
  }

  private Map<String, CredentialContext> getCredentialContexts(
      BaseCatalog baseCatalog, NameIdentifier nameIdentifier, CredentialPrivilege privilege) {
    if (nameIdentifier.equals(NameIdentifierUtil.getCatalogIdentifier(nameIdentifier))) {
      return getCatalogCredentialContexts(baseCatalog.properties());
    }

    if (baseCatalog.ops() instanceof SupportsPathBasedCredentials) {
      List<PathContext> pathContexts =
          ((SupportsPathBasedCredentials) baseCatalog.ops()).getPathContext(nameIdentifier);
      return getPathBasedCredentialContexts(privilege, pathContexts);
    }
    throw new NotSupportedException(
        String.format("Catalog %s doesn't support generate credentials", baseCatalog.name()));
  }

  private Map<String, CredentialContext> getCatalogCredentialContexts(
      Map<String, String> catalogProperties) {
    CatalogCredentialContext context =
        new CatalogCredentialContext(PrincipalUtils.getCurrentUserName());
    Set<String> providers = CredentialUtils.getCredentialProvidersByOrder(() -> catalogProperties);
    return providers.stream().collect(Collectors.toMap(provider -> provider, provider -> context));
  }

  public static Map<String, CredentialContext> getPathBasedCredentialContexts(
      CredentialPrivilege privilege, List<PathContext> pathContexts) {
    return pathContexts.stream()
        .collect(
            Collectors.toMap(
                pathContext -> pathContext.credentialType(),
                pathContext -> {
                  String path = pathContext.path();
                  Set<String> writePaths = new HashSet<>();
                  Set<String> readPaths = new HashSet<>();
                  if (CredentialPrivilege.WRITE.equals(privilege)) {
                    writePaths.add(path);
                  } else {
                    readPaths.add(path);
                  }
                  return new PathBasedCredentialContext(
                      PrincipalUtils.getCurrentUserName(), writePaths, readPaths);
                },
                CredentialOperationDispatcher::mergeContexts));
  }

  private static PathBasedCredentialContext mergeContexts(
      CredentialContext oldValue, CredentialContext newValue) {
    PathBasedCredentialContext oldContext = (PathBasedCredentialContext) oldValue;
    PathBasedCredentialContext newContext = (PathBasedCredentialContext) newValue;
    oldContext.getWritePaths().addAll(newContext.getWritePaths());
    oldContext.getReadPaths().addAll(newContext.getReadPaths());
    return oldContext;
  }

  @SuppressWarnings("UnusedVariable")
  private CredentialPrivilege getCredentialPrivilege(String user, NameIdentifier identifier)
      throws NotAuthorizedException {
    // TODO: will implement in another PR
    return CredentialPrivilege.WRITE;
  }
}
