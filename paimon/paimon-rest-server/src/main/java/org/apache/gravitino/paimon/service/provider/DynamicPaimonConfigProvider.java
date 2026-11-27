/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.gravitino.paimon.service.provider;

import static org.apache.gravitino.connector.BaseCatalog.CATALOG_BYPASS_PREFIX;

import com.google.common.base.Preconditions;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.GravitinoEnv;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.auth.AuthProperties;
import org.apache.gravitino.catalog.CatalogManager;
import org.apache.gravitino.catalog.lakehouse.paimon.PaimonCatalogPropertiesConverter;
import org.apache.gravitino.client.DefaultOAuth2TokenProvider;
import org.apache.gravitino.client.GravitinoClient;
import org.apache.gravitino.connector.BaseCatalog;
import org.apache.gravitino.connector.HiddenPropertyMaskUtils;
import org.apache.gravitino.credential.CatalogCredentialManager;
import org.apache.gravitino.credential.Credential;
import org.apache.gravitino.credential.CredentialConstants;
import org.apache.gravitino.credential.CredentialPrivilege;
import org.apache.gravitino.credential.CredentialUtils;
import org.apache.gravitino.credential.OSSTokenCredential;
import org.apache.gravitino.credential.PathBasedCredentialContext;
import org.apache.gravitino.credential.S3TokenCredential;
import org.apache.gravitino.credential.SupportsCredentials;
import org.apache.gravitino.exceptions.NoSuchCatalogException;
import org.apache.gravitino.exceptions.RESTException;
import org.apache.gravitino.paimon.common.PaimonCatalogOptions;
import org.apache.gravitino.secret.SupportsSecrets;
import org.apache.gravitino.utils.MapUtils;
import org.apache.gravitino.utils.NameIdentifierUtil;
import org.apache.gravitino.utils.PrincipalUtils;
import org.apache.paimon.rest.exceptions.NotImplementedException;
import org.apache.paimon.rest.exceptions.ServiceUnavailableException;

/** Resolves registered Gravitino Paimon catalogs using authenticated HTTP or in-process leases. */
public class DynamicPaimonConfigProvider implements PaimonConfigProvider {
  private final boolean auxiliary;
  private volatile GravitinoClient client;
  private Map<String, String> properties;
  private String metalake;
  private CatalogManager catalogManager;
  private Consumer<NameIdentifier> removalListener;

  /**
   * Creates a provider for a deployment mode.
   *
   * @param auxiliary whether Gravitino's in-process catalog manager is available
   */
  public DynamicPaimonConfigProvider(boolean auxiliary) {
    this.auxiliary = auxiliary;
  }

  DynamicPaimonConfigProvider(GravitinoClient client) {
    this(false);
    this.client = client;
  }

  @Override
  public void initialize(Map<String, String> properties) {
    this.properties = Map.copyOf(properties);
    metalake = properties.getOrDefault("gravitino-metalake", "default_metalake");
    Preconditions.checkArgument(!metalake.isBlank(), "gravitino-metalake must not be blank");
    if (auxiliary) {
      catalogManager = GravitinoEnv.getInstance().catalogManager();
      Preconditions.checkState(catalogManager != null, "Auxiliary mode requires a CatalogManager");
    } else {
      required(properties, "gravitino-uri");
      String authType =
          properties.getOrDefault("gravitino-auth-type", AuthProperties.SIMPLE_AUTH_TYPE);
      Preconditions.checkArgument(
          AuthProperties.isSimple(authType) || AuthProperties.isOAuth2(authType),
          "Unsupported gravitino-auth-type: %s",
          authType);
      if (AuthProperties.isOAuth2(authType)) {
        for (String key : Set.of("server-uri", "credential", "token-path", "scope")) {
          required(properties, "gravitino-oauth2." + key);
        }
      }
    }
  }

  @Override
  public Optional<CatalogConfig> getCatalogConfig(String name) {
    try {
      if (auxiliary) {
        return catalogManager.doWithCatalog(
            NameIdentifierUtil.ofCatalog(metalake, name), this::resolve);
      }
      return resolve(getClient().loadCatalog(name));
    } catch (NoSuchCatalogException e) {
      return Optional.empty();
    } catch (org.apache.gravitino.exceptions.ForbiddenException e) {
      throw new org.apache.paimon.rest.exceptions.ForbiddenException(
          "Gravitino denied catalog access.");
    } catch (org.apache.gravitino.exceptions.UnauthorizedException e) {
      throw new org.apache.paimon.rest.exceptions.NotAuthorizedException(
          "Gravitino client authentication failed.");
    } catch (RESTException e) {
      throw new ServiceUnavailableException(
          "Could not resolve the catalog from the Gravitino server.");
    } catch (org.apache.gravitino.exceptions.ServiceUnavailableException
        | org.apache.gravitino.exceptions.ConnectionFailedException e) {
      throw new ServiceUnavailableException(
          "Could not resolve the catalog from the Gravitino server.");
    } catch (RuntimeException e) {
      if (!auxiliary && e.getClass() == RuntimeException.class) {
        throw new ServiceUnavailableException(
            "Could not resolve the catalog from the Gravitino server.");
      }
      throw e;
    }
  }

  private Optional<CatalogConfig> resolve(Catalog catalog) {
    if (!"lakehouse-paimon".equals(catalog.provider())) {
      return Optional.empty();
    }
    Map<String, String> raw;
    Credential[] credentials;
    if (catalog instanceof BaseCatalog) {
      raw = new HashMap<>(((BaseCatalog<?>) catalog).propertiesWithCredentialProviders());
      checkAuthentication(raw);
      credentials =
          GravitinoEnv.getInstance()
              .credentialOperationDispatcher()
              .getCredentials(
                  NameIdentifierUtil.ofCatalog(metalake, catalog.name()), CredentialPrivilege.READ)
              .toArray(new Credential[0]);
    } else {
      raw = new HashMap<>(catalog.properties());
      raw.put(CredentialConstants.S3_CREDENTIAL_LIST_LOCATION_PREFIX, "true");
      checkAuthentication(raw);
      try {
        SupportsSecrets secrets = catalog.supportsSecrets();
        if (secrets != null) {
          raw.putAll(secrets.getSecrets());
        }
      } catch (UnsupportedOperationException e) {
        // Catalogs without secret storage can still use inline properties or credentials.
      }
      SupportsCredentials provider;
      try {
        provider = catalog.supportsCredentials();
      } catch (UnsupportedOperationException e) {
        provider = null;
      }
      credentials = provider == null ? new Credential[0] : provider.getCredentials();
    }
    Set<String> providers = CredentialUtils.getCredentialProvidersByOrder(() -> raw);
    if (providers.contains(S3TokenCredential.S3_TOKEN_CREDENTIAL_TYPE)
        || providers.contains(OSSTokenCredential.OSS_TOKEN_CREDENTIAL_TYPE)) {
      if (catalog instanceof BaseCatalog) {
        credentials =
            storageCredentials(
                ((BaseCatalog<?>) catalog).catalogCredentialManager(), raw, credentials, providers);
      } else {
        try (CatalogCredentialManager manager = new CatalogCredentialManager(catalog.name(), raw)) {
          credentials = storageCredentials(manager, raw, credentials, providers);
        }
      }
    }
    Map<String, String> options = new HashMap<>(MapUtils.getPrefixMap(raw, CATALOG_BYPASS_PREFIX));
    options.putAll(PaimonCatalogPropertiesConverter.toInnerProperties(raw, false));
    // Service-specific options have no counterpart in the existing catalog's property metadata.
    raw.forEach(
        (key, value) -> {
          if (key.equals("data-token.enabled") || key.startsWith("client.")) {
            options.put(key, value);
          }
        });
    properties.forEach(
        (key, value) -> {
          if (key.equals("data-token.enabled") || key.startsWith("client.")) {
            options.putIfAbsent(key, value);
          }
        });
    long expiry = 0;
    if (credentials != null) {
      List<Credential> ordered =
          Arrays.stream(credentials)
              .filter(Objects::nonNull)
              .sorted(Comparator.comparingLong(Credential::expireTimeInMs))
              .collect(Collectors.toList());
      for (Credential credential : ordered) {
        options.putAll(
            PaimonCatalogOptions.fromProperties(
                    PaimonCatalogPropertiesConverter.toInnerProperties(
                        credential.credentialInfo(), true))
                .toMap());
        if (credential.expireTimeInMs() > 0) {
          expiry =
              expiry == 0
                  ? credential.expireTimeInMs()
                  : Math.min(expiry, credential.expireTimeInMs());
        }
      }
    }
    if (options.values().stream().anyMatch(HiddenPropertyMaskUtils::isMaskedPlaceholder)) {
      throw new ServiceUnavailableException(
          "Paimon catalog contains an unresolved or inaccessible secret.");
    }
    return Optional.of(new CatalogConfig(options, expiry));
  }

  private Credential[] storageCredentials(
      CatalogCredentialManager manager,
      Map<String, String> raw,
      Credential[] existing,
      Set<String> providers) {
    String warehouse = raw.get("warehouse");
    Preconditions.checkArgument(warehouse != null, "STS credentials require a catalog warehouse");
    URI location = URI.create(warehouse);
    Preconditions.checkArgument(
        location.getAuthority() != null
            && location.getQuery() == null
            && location.getFragment() == null
            && location.normalize().equals(location),
        "STS credentials require an absolute warehouse URI");
    PathBasedCredentialContext context =
        new PathBasedCredentialContext(
            PrincipalUtils.getCurrentUserName(), Set.of(warehouse), Set.of());
    List<Credential> credentials = new ArrayList<>();
    if (existing != null) {
      Arrays.stream(existing)
          .filter(Objects::nonNull)
          .filter(
              credential ->
                  !(credential instanceof S3TokenCredential
                      && providers.contains(S3TokenCredential.S3_TOKEN_CREDENTIAL_TYPE)))
          .filter(
              credential ->
                  !(credential instanceof OSSTokenCredential
                      && providers.contains(OSSTokenCredential.OSS_TOKEN_CREDENTIAL_TYPE)))
          .forEach(credentials::add);
    }
    for (String type :
        Set.of(
            S3TokenCredential.S3_TOKEN_CREDENTIAL_TYPE,
            OSSTokenCredential.OSS_TOKEN_CREDENTIAL_TYPE)) {
      if (providers.contains(type)) {
        boolean supports =
            manager
                .getCredentialProvider(type)
                .map(provider -> provider.supportsScheme(location.getScheme()))
                .orElse(false);
        if (!supports) {
          throw new ServiceUnavailableException(
              "The configured STS provider does not support the catalog warehouse.");
        }
        Credential credential =
            manager
                .getCredential(type, context)
                .orElseThrow(
                    () ->
                        new ServiceUnavailableException(
                            "The configured STS provider returned no storage credential."));
        if (credential.expireTimeInMs() <= 0) {
          throw new ServiceUnavailableException(
              "The STS provider must return the actual storage credential expiry.");
        }
        credentials.add(credential);
      }
    }
    return credentials.toArray(new Credential[0]);
  }

  @Override
  public Optional<String> getMetalakeName() {
    return Optional.of(metalake);
  }

  @Override
  public Set<String> catalogNames() {
    if (auxiliary) {
      return Arrays.stream(catalogManager.listCatalogs(Namespace.of(metalake)))
          .map(NameIdentifier::name)
          .collect(Collectors.toSet());
    }
    return Set.of(getClient().loadMetalake(metalake).listCatalogs());
  }

  @Override
  public void addInvalidationListener(Consumer<String> listener) {
    if (auxiliary) {
      removalListener =
          identifier -> {
            if (identifier.namespace().level(0).equals(metalake)) {
              listener.accept(identifier.name());
            }
          };
      catalogManager.addCatalogCacheRemoveListener(removalListener);
    }
  }

  @Override
  public synchronized void close() {
    if (removalListener != null) {
      catalogManager.removeCatalogCacheRemoveListener(removalListener);
      removalListener = null;
    }
    if (client != null) {
      client.close();
      client = null;
    }
  }

  private GravitinoClient getClient() {
    if (client == null) {
      synchronized (this) {
        if (client == null) {
          client = createClient(properties, metalake);
        }
      }
    }
    return client;
  }

  static GravitinoClient createClient(Map<String, String> properties, String metalake) {
    GravitinoClient.ClientBuilder builder =
        GravitinoClient.builder(required(properties, "gravitino-uri")).withMetalake(metalake);
    String authType =
        properties.getOrDefault("gravitino-auth-type", AuthProperties.SIMPLE_AUTH_TYPE);
    if (AuthProperties.isSimple(authType)) {
      builder.withSimpleAuth(
          properties.getOrDefault("gravitino-simple.user-name", "paimon-rest-server"));
    } else if (AuthProperties.isOAuth2(authType)) {
      builder.withOAuth(
          DefaultOAuth2TokenProvider.builder()
              .withUri(required(properties, "gravitino-oauth2.server-uri"))
              .withCredential(required(properties, "gravitino-oauth2.credential"))
              .withPath(required(properties, "gravitino-oauth2.token-path"))
              .withScope(required(properties, "gravitino-oauth2.scope"))
              .build());
    } else {
      throw new IllegalArgumentException("Unsupported gravitino-auth-type: " + authType);
    }
    return builder.build();
  }

  private static void checkAuthentication(Map<String, String> properties) {
    String type =
        properties.getOrDefault(
            "authentication.type", properties.get(CATALOG_BYPASS_PREFIX + "authentication.type"));
    if ("kerberos".equalsIgnoreCase(type)) {
      throw new NotImplementedException(
          "Paimon REST does not support Kerberos catalog authentication yet.");
    }
  }

  private static String required(Map<String, String> properties, String key) {
    String value = properties.get(key);
    Preconditions.checkArgument(value != null && !value.isBlank(), "Missing %s", key);
    return value;
  }
}
