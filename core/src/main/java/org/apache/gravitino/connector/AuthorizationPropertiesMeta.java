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
package org.apache.gravitino.connector;

import com.google.common.collect.ImmutableMap;
import java.util.Map;

public class AuthorizationPropertiesMeta extends BasePropertiesMetadata
    implements WildcardPropertiesMeta {
  private static volatile AuthorizationPropertiesMeta instance = null;

  public static synchronized AuthorizationPropertiesMeta getInstance() {
    if (instance == null) {
      synchronized (AuthorizationPropertiesMeta.class) {
        if (instance == null) {
          instance = new AuthorizationPropertiesMeta();
        }
      }
    }
    return instance;
  }

  /** Ranger admin web URIs */
  private static final String RANGER_ADMIN_URL_KEY = "ranger.admin.url";

  public static final String getRangerAdminUrlKey() {
    return RANGER_ADMIN_URL_KEY;
  }

  public static final String RANGER_ADMIN_URL =
      AuthorizationPropertiesMeta.getInstance().generateFirstNodePropertyKey(RANGER_ADMIN_URL_KEY);
  /** Ranger authentication type kerberos or simple */
  private static final String RANGER_AUTH_TYPE_KEY = "ranger.auth.type";

  public static final String getRangerAuthTypeKey() {
    return RANGER_AUTH_TYPE_KEY;
  }

  public static final String RANGER_AUTH_TYPE =
      AuthorizationPropertiesMeta.getInstance().generateFirstNodePropertyKey(RANGER_AUTH_TYPE_KEY);
  /**
   * Ranger admin web login username(auth_type=simple), or kerberos principal(auth_type=kerberos)
   */
  private static final String RANGER_USERNAME_KEY = "ranger.username";

  public static final String getRangerUsernameKey() {
    return RANGER_USERNAME_KEY;
  }

  public static final String RANGER_USERNAME =
      AuthorizationPropertiesMeta.getInstance().generateFirstNodePropertyKey(RANGER_USERNAME_KEY);
  /**
   * Ranger admin web login user password(auth_type=simple), or path of the keytab
   * file(auth_type=kerberos)
   */
  private static final String RANGER_PASSWORD_KEY = "ranger.password";

  public static final String getRangerPasswordKey() {
    return RANGER_PASSWORD_KEY;
  }

  public static final String RANGER_PASSWORD =
      AuthorizationPropertiesMeta.getInstance().generateFirstNodePropertyKey(RANGER_PASSWORD_KEY);

  /** Ranger service name */
  private static final String RANGER_SERVICE_NAME_KEY = "ranger.service.name";

  public static final String getRangerServiceNameKey() {
    return RANGER_SERVICE_NAME_KEY;
  }

  public static final String RANGER_SERVICE_NAME =
      AuthorizationPropertiesMeta.getInstance()
          .generateFirstNodePropertyKey(RANGER_SERVICE_NAME_KEY);

  /** Chain authorization plugin provider */
  private static final String CHAIN_CATALOG_PROVIDER_KEY = "catalog-provider";

  public static final String getChainCatalogProviderKey() {
    return CHAIN_CATALOG_PROVIDER_KEY;
  }

  public static final String CHAIN_CATALOG_PROVIDER =
      AuthorizationPropertiesMeta.getInstance()
          .getPropertyValue(Constants.WILDCARD, CHAIN_CATALOG_PROVIDER_KEY);

  /** Chain authorization plugin provider */
  private static final String CHAIN_PROVIDER_KEY = "provider";

  public static final String getChainProviderKey() {
    return CHAIN_PROVIDER_KEY;
  }

  public static final String CHAIN_PROVIDER =
      AuthorizationPropertiesMeta.getInstance()
          .getPropertyValue(Constants.WILDCARD, CHAIN_PROVIDER_KEY);
  /** Chain authorization Ranger admin web URIs */
  public static final String CHAIN_RANGER_ADMIN_URL =
      AuthorizationPropertiesMeta.getInstance()
          .getPropertyValue(Constants.WILDCARD, RANGER_ADMIN_URL_KEY);
  /** Chain authorization Ranger authentication type kerberos or simple */
  public static final String CHAIN_RANGER_AUTH_TYPES =
      AuthorizationPropertiesMeta.getInstance()
          .getPropertyValue(Constants.WILDCARD, RANGER_AUTH_TYPE_KEY);
  /** Chain authorization Ranger username */
  public static final String CHAIN_RANGER_USERNAME =
      AuthorizationPropertiesMeta.getInstance()
          .getPropertyValue(Constants.WILDCARD, RANGER_USERNAME_KEY);
  /**
   * Chain authorization Ranger admin web login user password(auth_type=simple), or path of the
   * keytab file(auth_type=kerberos)
   */
  public static final String CHAIN_RANGER_PASSWORD =
      AuthorizationPropertiesMeta.getInstance()
          .getPropertyValue(Constants.WILDCARD, RANGER_PASSWORD_KEY);
  /** Chain authorization Ranger service name */
  public static final String CHAIN_RANGER_SERVICE_NAME =
      AuthorizationPropertiesMeta.getInstance()
          .getPropertyValue(Constants.WILDCARD, RANGER_SERVICE_NAME_KEY);

  public static String chainKeyToPluginKey(String chainKey, String plugin) {
    return chainKey.replace(
        String.format(
            "%s.%s", AuthorizationPropertiesMeta.getInstance().wildcardNodePropertyKey(), plugin),
        AuthorizationPropertiesMeta.getInstance().firstNodeName());
  }

  public static final Map<String, PropertyEntry<?>> AUTHORIZATION_PROPERTY_ENTRIES =
      ImmutableMap.<String, PropertyEntry<?>>builder()
          .put(
              AuthorizationPropertiesMeta.getInstance().wildcardNodePropertyKey(),
              PropertyEntry.wildcardPropertyEntry(
                  AuthorizationPropertiesMeta.getInstance().wildcardNodePropertyKey(),
                  "The Chain authorization plugins"))
          .put(
              CHAIN_CATALOG_PROVIDER,
              PropertyEntry.wildcardPropertyEntry(
                  CHAIN_PROVIDER, "The Chain sub entity catalog provider"))
          .put(
              CHAIN_PROVIDER,
              PropertyEntry.wildcardPropertyEntry(
                  CHAIN_PROVIDER, "The Chain sub entity authorization plugin provider"))
          .put(
              CHAIN_RANGER_SERVICE_NAME,
              PropertyEntry.wildcardPropertyEntry(
                  CHAIN_RANGER_SERVICE_NAME,
                  "The Chain sub entity authorization Ranger service name"))
          .put(
              CHAIN_RANGER_ADMIN_URL,
              PropertyEntry.wildcardPropertyEntry(
                  CHAIN_RANGER_ADMIN_URL,
                  "The Chain sub entity authorization Ranger admin web URIs"))
          .put(
              CHAIN_RANGER_AUTH_TYPES,
              PropertyEntry.wildcardPropertyEntry(
                  CHAIN_RANGER_AUTH_TYPES,
                  "The Chain sub entity authorization Ranger admin web auth type (kerberos/simple)"))
          .put(
              CHAIN_RANGER_USERNAME,
              PropertyEntry.wildcardPropertyEntry(
                  CHAIN_RANGER_USERNAME,
                  "The Chain sub entity authorization Ranger admin web login username"))
          .put(
              CHAIN_RANGER_PASSWORD,
              PropertyEntry.wildcardPropertyEntry(
                  CHAIN_RANGER_PASSWORD,
                  "The Chain sub entity authorization Ranger admin web login password"))
          .put(
              RANGER_SERVICE_NAME,
              PropertyEntry.stringOptionalPropertyEntry(
                  RANGER_SERVICE_NAME, "The Ranger service name", true, null, false))
          .put(
              RANGER_ADMIN_URL,
              PropertyEntry.stringOptionalPropertyEntry(
                  RANGER_ADMIN_URL, "The Ranger admin web URIs", true, null, false))
          .put(
              RANGER_AUTH_TYPE,
              PropertyEntry.stringOptionalPropertyEntry(
                  RANGER_AUTH_TYPE,
                  "The Ranger admin web auth type (kerberos/simple)",
                  true,
                  "simple",
                  false))
          .put(
              RANGER_USERNAME,
              PropertyEntry.stringOptionalPropertyEntry(
                  RANGER_USERNAME, "The Ranger admin web login username", true, null, false))
          .put(
              RANGER_PASSWORD,
              PropertyEntry.stringOptionalPropertyEntry(
                  RANGER_PASSWORD, "The Ranger admin web login password", true, null, false))
          .build();

  @Override
  protected Map<String, PropertyEntry<?>> specificPropertyEntries() {
    return AUTHORIZATION_PROPERTY_ENTRIES;
  }

  @Override
  public String firstNodeName() {
    return "authorization";
  }

  @Override
  public String secondNodeName() {
    return "chain";
  }

  @Override
  public String wildcardNodeName() {
    return "plugins";
  }
}
