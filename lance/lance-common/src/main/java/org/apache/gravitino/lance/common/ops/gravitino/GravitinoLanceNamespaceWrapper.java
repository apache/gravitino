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
package org.apache.gravitino.lance.common.ops.gravitino;

import static org.apache.gravitino.lance.common.config.LanceConfig.METALAKE_NAME;
import static org.apache.gravitino.lance.common.config.LanceConfig.NAMESPACE_BACKEND_URI;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import java.util.HashMap;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.auth.AuthProperties;
import org.apache.gravitino.client.DefaultOAuth2TokenProvider;
import org.apache.gravitino.client.GravitinoClient;
import org.apache.gravitino.client.GravitinoClient.ClientBuilder;
import org.apache.gravitino.config.ConfigEntry;
import org.apache.gravitino.exceptions.NoSuchCatalogException;
import org.apache.gravitino.lance.common.config.LanceConfig;
import org.apache.gravitino.lance.common.ops.LanceNamespaceOperations;
import org.apache.gravitino.lance.common.ops.LanceTableOperations;
import org.apache.gravitino.lance.common.ops.NamespaceWrapper;
import org.lance.namespace.errors.NamespaceNotFoundException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GravitinoLanceNamespaceWrapper extends NamespaceWrapper {

  private static final Logger LOG = LoggerFactory.getLogger(GravitinoLanceNamespaceWrapper.class);
  private GravitinoClient client;

  private LanceNamespaceOperations namespaceOperations;
  private LanceTableOperations tableOperations;

  @VisibleForTesting
  GravitinoLanceNamespaceWrapper() {
    super(null);
  }

  public GravitinoLanceNamespaceWrapper(LanceConfig config) {
    super(config);
  }

  public GravitinoClient getClient() {
    return client;
  }

  @Override
  protected void initialize() {
    String uri = config().get(NAMESPACE_BACKEND_URI);
    String metalakeName = config().get(METALAKE_NAME);
    Preconditions.checkArgument(
        StringUtils.isNotBlank(metalakeName),
        "Metalake name must be provided for Lance Gravitino namespace backend");

    // Extract client configuration properties (e.g., connection pool settings)
    Map<String, String> clientProperties = new HashMap<>();
    config()
        .getAllConfig()
        .forEach(
            (key, value) -> {
              if (key.startsWith("gravitino.client.")) {
                clientProperties.put(key, value);
                LOG.info("Applying client config: {} = {}", key, value);
              }
            });

    this.client = createGravitinoClient(uri, metalakeName, clientProperties, config());

    LOG.info(
        "GravitinoClient initialized with auth type {} and {} client properties for metalake: {}",
        config().getGravitinoAuthType(),
        clientProperties.size(),
        metalakeName);

    this.namespaceOperations = new GravitinoLanceNameSpaceOperations(this);
    this.tableOperations = new GravitinoLanceTableOperations(this);
  }

  @Override
  public LanceNamespaceOperations newNamespaceOps() {
    return namespaceOperations;
  }

  @Override
  protected LanceTableOperations newTableOps() {
    return tableOperations;
  }

  @Override
  public void close() {
    if (client != null) {
      try {
        client.close();
      } catch (Exception e) {
        LOG.warn("Error closing Gravitino client", e);
      }
    }
  }

  public boolean isLakehouseCatalog(Catalog catalog) {
    return catalog.type().equals(Catalog.Type.RELATIONAL)
        && "lakehouse-generic".equals(catalog.provider());
  }

  public Catalog loadAndValidateLakehouseCatalog(String catalogName) {
    Catalog catalog;
    try {
      catalog = client.loadCatalog(catalogName);
    } catch (NoSuchCatalogException e) {
      throw new NamespaceNotFoundException(
          "Catalog not found: " + catalogName, CommonUtil.formatCurrentStackTrace(), catalogName);
    }
    if (!isLakehouseCatalog(catalog)) {
      throw new NamespaceNotFoundException(
          "Catalog is not a lakehouse catalog: " + catalogName,
          CommonUtil.formatCurrentStackTrace(),
          catalogName);
    }
    return catalog;
  }

  static GravitinoClient createGravitinoClient(
      String uri, String metalake, Map<String, String> clientProperties, LanceConfig config) {
    return newClientBuilder(uri, metalake, clientProperties, config).build();
  }

  /**
   * Builds and configures the client builder, including the credentials the Lance REST service
   * presents to the Gravitino server. Separated from {@link #createGravitinoClient} so that the
   * configuration can be exercised without contacting a server.
   *
   * @param uri the Gravitino server URI
   * @param metalake the metalake name
   * @param clientProperties additional client properties, such as connection pool settings
   * @param config the Lance REST service configuration holding the auth settings
   * @return a configured client builder
   */
  @VisibleForTesting
  static ClientBuilder newClientBuilder(
      String uri, String metalake, Map<String, String> clientProperties, LanceConfig config) {
    ClientBuilder builder = GravitinoClient.builder(uri).withMetalake(metalake);
    builder.withClientConfig(clientProperties);
    String authType = config.getGravitinoAuthType();
    if (AuthProperties.isSimple(authType)) {
      builder.withSimpleAuth(config.get(LanceConfig.GRAVITINO_SIMPLE_USERNAME));
    } else if (AuthProperties.isOAuth2(authType)) {
      DefaultOAuth2TokenProvider tokenProvider =
          DefaultOAuth2TokenProvider.builder()
              .withUri(requireConfig(config, LanceConfig.GRAVITINO_OAUTH2_SERVER_URI, "server-uri"))
              .withCredential(
                  requireConfig(config, LanceConfig.GRAVITINO_OAUTH2_CREDENTIAL, "credential"))
              .withPath(
                  requireConfig(config, LanceConfig.GRAVITINO_OAUTH2_TOKEN_PATH, "token-path"))
              .withScope(requireConfig(config, LanceConfig.GRAVITINO_OAUTH2_SCOPE, "scope"))
              .build();
      builder.withOAuth(tokenProvider);
    } else {
      throw new UnsupportedOperationException(
          String.format(
              "Unsupported value for %sgravitino-%s: %s. Supported values are %s and %s.",
              LanceConfig.LANCE_CONFIG_PREFIX,
              LanceConfig.CONFIG_AUTH_TYPE,
              authType,
              AuthProperties.SIMPLE_AUTH_TYPE,
              AuthProperties.OAUTH2_AUTH_TYPE));
    }
    return builder;
  }

  private static String requireConfig(LanceConfig config, ConfigEntry<String> entry, String name) {
    String value = config.get(entry);
    Preconditions.checkArgument(
        StringUtils.isNotBlank(value),
        "%sgravitino-oauth2.%s must be set when %sgravitino-%s is %s",
        LanceConfig.LANCE_CONFIG_PREFIX,
        name,
        LanceConfig.LANCE_CONFIG_PREFIX,
        LanceConfig.CONFIG_AUTH_TYPE,
        AuthProperties.OAUTH2_AUTH_TYPE);
    return value;
  }
}
