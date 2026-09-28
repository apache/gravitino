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

import static org.apache.gravitino.Entity.EntityType.FILESET;
import static org.apache.gravitino.Entity.EntityType.SCHEMA;

import com.google.common.collect.ImmutableSet;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import javax.ws.rs.NotSupportedException;
import org.apache.gravitino.EntityStore;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.catalog.CatalogManager;
import org.apache.gravitino.catalog.OperationDispatcher;
import org.apache.gravitino.connector.BaseCatalog;
import org.apache.gravitino.connector.credential.PathContext;
import org.apache.gravitino.connector.credential.SupportsPathBasedCredentials;
import org.apache.gravitino.exceptions.NoSuchCatalogException;
import org.apache.gravitino.meta.FilesetEntity;
import org.apache.gravitino.meta.SchemaEntity;
import org.apache.gravitino.secret.SecretManager;
import org.apache.gravitino.storage.IdGenerator;
import org.apache.gravitino.utils.NameIdentifierUtil;
import org.apache.gravitino.utils.PrincipalUtils;

/** Get credentials with the specific catalog classloader. */
public class CredentialOperationDispatcher extends OperationDispatcher {

  public CredentialOperationDispatcher(
      CatalogManager catalogManager,
      EntityStore store,
      IdGenerator idGenerator,
      SecretManager secretManager) {
    super(catalogManager, store, idGenerator, secretManager);
  }

  public List<Credential> getCredentials(NameIdentifier identifier, CredentialPrivilege privilege) {
    return doWithCatalog(
        NameIdentifierUtil.getCatalogIdentifier(identifier),
        catalogWrapper ->
            catalogWrapper.doWithCredentialOps(
                baseCatalog -> getCredentials(baseCatalog, identifier, privilege)),
        NoSuchCatalogException.class);
  }

  List<Credential> getCredentials(
      BaseCatalog baseCatalog, NameIdentifier nameIdentifier, CredentialPrivilege privilege) {
    Map<String, CredentialContext> contexts =
        getCredentialContexts(baseCatalog, nameIdentifier, privilege);
    List<Credential> credentials = new ArrayList<>();
    for (Map.Entry<String, CredentialContext> entry : contexts.entrySet()) {
      Optional<CredentialProvider> providerOptional =
          baseCatalog.catalogCredentialManager().getCredentialProvider(entry.getKey());
      if (!providerOptional.isPresent()) {
        continue;
      }

      Optional<CredentialContext> contextOptional =
          filterContextByProvider(providerOptional.get(), entry.getValue());
      if (!contextOptional.isPresent()) {
        continue;
      }

      baseCatalog
          .catalogCredentialManager()
          .getCredential(entry.getKey(), contextOptional.get())
          .ifPresent(credentials::add);
    }
    // Path-based (fileset) requests: catalog CredentialProviders are initialized with catalog
    // properties only. For static secret-key types already selected for this request, rebuild from
    // fileset→schema→catalog merged plaintext so schema/fileset AK/SK overrides win. Do not
    // introduce static types omitted from the effective credential-providers list (e.g. s3-token
    // only). Fileset/topic/model share a three-level namespace; this dispatcher is used from
    // fileset catalogs for path-based credentials.
    if (NameIdentifierUtil.hasThreeLevelNamespace(nameIdentifier)) {
      return overlayFilesetStaticCredentials(
          baseCatalog, nameIdentifier, credentials, contexts.keySet());
    }
    return credentials;
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

  static Optional<CredentialContext> filterContextByProvider(
      CredentialProvider credentialProvider, CredentialContext context) {
    if (!(context instanceof PathBasedCredentialContext)) {
      return Optional.of(context);
    }

    PathBasedCredentialContext pathBasedCredentialContext = (PathBasedCredentialContext) context;
    Set<String> supportedWritePaths =
        pathBasedCredentialContext.getWritePaths().stream()
            .filter(path -> isPathSupported(credentialProvider, path))
            .collect(ImmutableSet.toImmutableSet());
    Set<String> supportedReadPaths =
        pathBasedCredentialContext.getReadPaths().stream()
            .filter(path -> isPathSupported(credentialProvider, path))
            .collect(ImmutableSet.toImmutableSet());

    if (supportedWritePaths.isEmpty() && supportedReadPaths.isEmpty()) {
      return Optional.empty();
    }

    return Optional.of(
        new PathBasedCredentialContext(
            pathBasedCredentialContext.getUserName(), supportedWritePaths, supportedReadPaths));
  }

  private Map<String, CredentialContext> getCredentialContexts(
      BaseCatalog baseCatalog, NameIdentifier nameIdentifier, CredentialPrivilege privilege) {
    if (nameIdentifier.equals(NameIdentifierUtil.getCatalogIdentifier(nameIdentifier))) {
      return getCatalogCredentialContexts(baseCatalog.propertiesWithCredentialProviders());
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

  private List<Credential> overlayFilesetStaticCredentials(
      BaseCatalog baseCatalog,
      NameIdentifier filesetIdent,
      List<Credential> credentials,
      Set<String> selectedProviderTypes) {
    Map<String, String> merged = mergeFilesetPlaintextProperties(baseCatalog, filesetIdent);
    if (merged.isEmpty()) {
      return credentials;
    }
    return StaticSecretKeyCredentialFactory.overlay(credentials, merged, selectedProviderTypes);
  }

  private Map<String, String> mergeFilesetPlaintextProperties(
      BaseCatalog baseCatalog, NameIdentifier filesetIdent) {
    Map<String, String> merged = new HashMap<>(baseCatalog.propertiesWithCredentialProviders());
    NameIdentifier schemaIdent = NameIdentifierUtil.getSchemaIdentifier(filesetIdent);
    SchemaEntity schemaEntity = getEntity(schemaIdent, SCHEMA, SchemaEntity.class);
    if (schemaEntity != null) {
      merged.putAll(secretManager.toPlaintextProperties(schemaEntity.properties()));
    }
    FilesetEntity filesetEntity = getEntity(filesetIdent, FILESET, FilesetEntity.class);
    if (filesetEntity != null) {
      merged.putAll(secretManager.toPlaintextProperties(filesetEntity.properties()));
    }
    return merged;
  }

  private static boolean isPathSupported(CredentialProvider credentialProvider, String path) {
    if (path == null) {
      return false;
    }

    try {
      String scheme = URI.create(path).getScheme();
      if (scheme == null) {
        return false;
      }
      return credentialProvider.supportsScheme(scheme);
    } catch (Exception e) {
      return false;
    }
  }

  private static PathBasedCredentialContext mergeContexts(
      CredentialContext oldValue, CredentialContext newValue) {
    PathBasedCredentialContext oldContext = (PathBasedCredentialContext) oldValue;
    PathBasedCredentialContext newContext = (PathBasedCredentialContext) newValue;
    oldContext.getWritePaths().addAll(newContext.getWritePaths());
    oldContext.getReadPaths().addAll(newContext.getReadPaths());
    return oldContext;
  }
}
