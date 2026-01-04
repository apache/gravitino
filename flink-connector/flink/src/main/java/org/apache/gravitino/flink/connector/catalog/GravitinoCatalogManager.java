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
package org.apache.gravitino.flink.connector.catalog;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.Sets;
import java.security.PrivilegedAction;
import java.util.Arrays;
import java.util.Map;
import java.util.Set;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.auth.AuthenticatorType;
import org.apache.gravitino.client.DefaultOAuth2TokenProvider;
import org.apache.gravitino.client.GravitinoAdminClient;
import org.apache.gravitino.client.GravitinoMetalake;
import org.apache.gravitino.client.KerberosTokenProvider;
import org.apache.gravitino.flink.connector.store.GravitinoCatalogStoreFactoryOptions;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** GravitinoCatalogManager is used to retrieve catalogs from Apache Gravitino server. */
public class GravitinoCatalogManager {

  private static final Logger LOG = LoggerFactory.getLogger(GravitinoCatalogManager.class);
  private static GravitinoCatalogManager gravitinoCatalogManager;

  private volatile boolean isClosed = false;
  private final GravitinoMetalake metalake;
  private final GravitinoAdminClient gravitinoClient;

  private final String gravitinoUri;
  private final String metalakeName;
  private final Map<String, String> gravitinoClientConfig;

  private GravitinoCatalogManager(
      String gravitinoUri, String metalakeName, Map<String, String> gravitinoClientConfig) {
    Preconditions.checkArgument(
        !Strings.isNullOrEmpty(gravitinoUri), "Gravitino uri cannot be null or empty");
    Preconditions.checkArgument(
        !Strings.isNullOrEmpty(metalakeName), "MetalakeName cannot be null or empty");
    Preconditions.checkNotNull(gravitinoClientConfig, "GravitinoClientConfig cannot be null");
    this.gravitinoUri = gravitinoUri;
    this.metalakeName = metalakeName;
    this.gravitinoClientConfig = gravitinoClientConfig;

    String authType = gravitinoClientConfig.get(GravitinoCatalogStoreFactoryOptions.AUTH_TYPE);

    // Only OAuth is explicitly configured; otherwise follow Flink security (Kerberos if enabled,
    // simple auth otherwise).
    if (AuthenticatorType.OAUTH.name().equalsIgnoreCase(authType)) {
      this.gravitinoClient = buildOAuthClient(gravitinoUri, gravitinoClientConfig);
    } else {
      if (authType != null) {
        throw new IllegalArgumentException(
            String.format(
                "Unsupported auth type '%s'. Only OAUTH is supported; leave %s unset to use Flink Kerberos settings (or simple auth if security is disabled).",
                authType, GravitinoCatalogStoreFactoryOptions.AUTH_TYPE));
      }

      if (UserGroupInformation.isSecurityEnabled()) {
        if (getUgi().getAuthenticationMethod()
            != UserGroupInformation.AuthenticationMethod.KERBEROS) {
          throw new IllegalStateException(
              String.format(
                  "Flink security is enabled, but current user authentication method is %s rather than KERBEROS",
                  getUgi().getAuthenticationMethod()));
        }
        this.gravitinoClient = buildKerberosClient(gravitinoUri, gravitinoClientConfig);
      } else {
        this.gravitinoClient = buildSimpleClient(gravitinoUri, gravitinoClientConfig);
      }
    }

    this.metalake = gravitinoClient.loadMetalake(metalakeName);
  }

  /**
   * Create GravitinoCatalogManager with Gravitino server uri, metalake name and client properties
   * map.
   *
   * @param gravitinoUri Gravitino server uri
   * @param metalakeName Metalake name
   * @param gravitinoClientConfig Gravitino client properties map
   * @return GravitinoCatalogManager
   */
  public static GravitinoCatalogManager create(
      String gravitinoUri, String metalakeName, Map<String, String> gravitinoClientConfig) {
    if (gravitinoCatalogManager == null) {
      gravitinoCatalogManager =
          new GravitinoCatalogManager(gravitinoUri, metalakeName, gravitinoClientConfig);
    } else {
      Preconditions.checkState(
          checkEqual(gravitinoUri, metalakeName, gravitinoClientConfig),
          String.format(
              "Creating GravitinoCatalogManager with different configuration is not supported. "
                  + "Current singleton %s. "
                  + "Creating with gravitinoUri=%s, metalakeName=%s, gravitinoClientConfig=%s",
              gravitinoCatalogManager, gravitinoUri, metalakeName, gravitinoClientConfig));
    }
    return gravitinoCatalogManager;
  }

  /**
   * Get GravitinoCatalogManager instance.
   *
   * @return GravitinoCatalogManager
   */
  public static GravitinoCatalogManager get() {
    Preconditions.checkState(
        gravitinoCatalogManager != null, "GravitinoCatalogManager has not created yet");
    Preconditions.checkState(
        !gravitinoCatalogManager.isClosed, "GravitinoCatalogManager is already closed");
    return gravitinoCatalogManager;
  }

  /**
   * Close GravitinoCatalogManager.
   *
   * <p>After close, GravitinoCatalogManager can not be used anymore.
   */
  public void close() {
    if (!isClosed) {
      isClosed = true;
      gravitinoClient.close();
      gravitinoCatalogManager = null;
    }
  }

  /**
   * Get GravitinoCatalog by name.
   *
   * @param name Catalog name
   * @return The Gravitino Catalog
   */
  public Catalog getGravitinoCatalogInfo(String name) {
    Catalog catalog = metalake.loadCatalog(name);
    Preconditions.checkArgument(
        Catalog.Type.RELATIONAL.equals(catalog.type()), "Only support relational catalog");
    LOG.info("Load catalog {} from Gravitino successfully.", name);
    return catalog;
  }

  /**
   * Create catalog in Gravitino.
   *
   * @param catalogName Catalog name
   * @param type Catalog type
   * @param comment Catalog comment
   * @param provider Catalog provider
   * @param properties Catalog properties
   * @return Catalog
   */
  public Catalog createCatalog(
      String catalogName,
      Catalog.Type type,
      String comment,
      String provider,
      Map<String, String> properties) {
    return metalake.createCatalog(catalogName, type, provider, comment, properties);
  }

  /**
   * Drop catalog in Gravitino.
   *
   * @param catalogName Catalog name
   * @return boolean
   */
  public boolean dropCatalog(String catalogName) {
    return metalake.dropCatalog(catalogName, true);
  }

  /**
   * List catalogs in Gravitino.
   *
   * @return Set of catalog names
   */
  public Set<String> listCatalogs() {
    String[] catalogNames = metalake.listCatalogs();
    LOG.info(
        "Load metalake {}'s catalogs. catalogs: {}.",
        metalake.name(),
        Arrays.toString(catalogNames));
    return Sets.newHashSet(catalogNames);
  }

  /**
   * Check if catalog exists in Gravitino.
   *
   * @param catalogName Catalog name
   * @return boolean
   */
  public boolean contains(String catalogName) {
    return metalake.catalogExists(catalogName);
  }

  @Override
  public String toString() {
    return "GravitinoCatalogManager{"
        + "gravitinoUri='"
        + gravitinoUri
        + '\''
        + ", metalakeName='"
        + metalakeName
        + '\''
        + ", gravitinoClientConfig="
        + gravitinoClientConfig
        + '}';
  }

  /**
   * Check whether the parameters are the same as the configuration of the GravitinoCatalogManager
   * static variable.
   *
   * @param gravitinoUri Gravitino server uri
   * @param metalakeName Metalake name
   * @param gravitinoClientConfig Gravitino client properties map
   */
  private static boolean checkEqual(
      String gravitinoUri, String metalakeName, Map<String, String> gravitinoClientConfig) {
    return gravitinoCatalogManager.gravitinoUri.equals(gravitinoUri)
        && gravitinoCatalogManager.metalakeName.equals(metalakeName)
        && gravitinoCatalogManager.gravitinoClientConfig.equals(gravitinoClientConfig);
  }

  private static GravitinoAdminClient buildOAuthClient(
      String gravitinoUri, Map<String, String> config) {
    String serverUri = config.get(GravitinoCatalogStoreFactoryOptions.OAUTH2_SERVER_URI);
    String credential = config.get(GravitinoCatalogStoreFactoryOptions.OAUTH2_CREDENTIAL);
    String path = config.get(GravitinoCatalogStoreFactoryOptions.OAUTH2_TOKEN_PATH);
    String scope = config.get(GravitinoCatalogStoreFactoryOptions.OAUTH2_SCOPE);
    Preconditions.checkArgument(
        StringUtils.isNoneBlank(serverUri, credential, path, scope),
        String.format(
            "OAuth2 authentication requires: %s, %s, %s, and %s",
            GravitinoCatalogStoreFactoryOptions.OAUTH2_SERVER_URI,
            GravitinoCatalogStoreFactoryOptions.OAUTH2_CREDENTIAL,
            GravitinoCatalogStoreFactoryOptions.OAUTH2_TOKEN_PATH,
            GravitinoCatalogStoreFactoryOptions.OAUTH2_SCOPE));

    DefaultOAuth2TokenProvider provider =
        DefaultOAuth2TokenProvider.builder()
            .withUri(serverUri)
            .withCredential(credential)
            .withPath(path)
            .withScope(scope)
            .build();

    return GravitinoAdminClient.builder(gravitinoUri)
        .withOAuth(provider)
        .withClientConfig(config)
        .build();
  }

  private static GravitinoAdminClient buildKerberosClient(
      String gravitinoUri, Map<String, String> config) {

    return getUgi()
        .doAs(
            (PrivilegedAction<GravitinoAdminClient>)
                () ->
                    GravitinoAdminClient.builder(gravitinoUri)
                        .withKerberosAuth(KerberosTokenProvider.builder().build())
                        .withClientConfig(config)
                        .build());
  }

  private static GravitinoAdminClient buildSimpleClient(
      String gravitinoUri, Map<String, String> config) {
    String userName = getUgi().getUserName();
    return GravitinoAdminClient.builder(gravitinoUri)
        .withSimpleAuth(userName)
        .withClientConfig(config)
        .build();
  }

  private static UserGroupInformation getUgi() {
    try {
      return UserGroupInformation.getCurrentUser();
    } catch (Exception e) {
      throw new IllegalStateException("Unable to get current user group information", e);
    }
  }
}
