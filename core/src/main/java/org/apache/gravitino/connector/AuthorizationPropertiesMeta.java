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

public class AuthorizationPropertiesMeta extends BasePropertiesMetadata {
  private static final String AUTHORIZATION_PREFIX = "authorization";

  public static final String getAuthorizationPrefix() {
    return AUTHORIZATION_PREFIX;
  }
  /** Ranger admin web URIs */
  private static final String RANGER_ADMIN_URL_KEY = "ranger.admin.url";

  public static final String getRangerAdminUrlKey() {
    return RANGER_ADMIN_URL_KEY;
  }

  public static final String RANGER_ADMIN_URL = generatePluginKey(RANGER_ADMIN_URL_KEY);
  /** Ranger authentication type kerberos or simple */
  private static final String RANGER_AUTH_TYPE_KEY = "ranger.auth.type";

  public static final String getRangerAuthTypeKey() {
    return RANGER_AUTH_TYPE_KEY;
  }

  public static final String RANGER_AUTH_TYPE = generatePluginKey(RANGER_AUTH_TYPE_KEY);
  /**
   * Ranger admin web login username(auth_type=simple), or kerberos principal(auth_type=kerberos)
   */
  private static final String RANGER_USERNAME_KEY = "ranger.username";

  public static final String getRangerUsernameKey() {
    return RANGER_USERNAME_KEY;
  }

  public static final String RANGER_USERNAME = generatePluginKey(RANGER_USERNAME_KEY);
  /**
   * Ranger admin web login user password(auth_type=simple), or path of the keytab
   * file(auth_type=kerberos)
   */
  private static final String RANGER_PASSWORD_KEY = "ranger.password";

  public static final String getRangerPasswordKey() {
    return RANGER_PASSWORD_KEY;
  }

  public static final String RANGER_PASSWORD = generatePluginKey(RANGER_PASSWORD_KEY);

  /** Ranger service name */
  private static final String RANGER_SERVICE_NAME_KEY = "ranger.service.name";

  public static final String getRangerServiceNameKey() {
    return RANGER_SERVICE_NAME_KEY;
  }

  public static final String RANGER_SERVICE_NAME = generatePluginKey(RANGER_SERVICE_NAME_KEY);

  private static final String CHAIN_PLUGINS_WILDCARD = "*";

  public static final String getChainPlugsWildcard() {
    return CHAIN_PLUGINS_WILDCARD;
  }

  private static final String CHAIN_PLUGINS_SPLITTER = ",";

  public static final String getChainPluginsSplitter() {
    return CHAIN_PLUGINS_SPLITTER;
  }

  private static final String CHAIN_PREFIX = String.format("%s.chain", AUTHORIZATION_PREFIX);

  public static final String getChainPrefix() {
    return CHAIN_PREFIX;
  }
  /** Chain authorization plugins */
  public static final String CHAIN_PLUGINS = String.format("%s.plugins", CHAIN_PREFIX);
  /** Chain authorization plugin provider */
  private static final String CHAIN_PROVIDER_KEY = "provider";

  public static final String getChainProviderKey() {
    return CHAIN_PROVIDER_KEY;
  }

  public static final String CHAIN_PROVIDER =
      generateChainPluginsKey(CHAIN_PLUGINS_WILDCARD, CHAIN_PROVIDER_KEY);
  /** Chain authorization Ranger admin web URIs */
  public static final String CHAIN_RANGER_ADMIN_URL =
      generateChainPluginsKey(CHAIN_PLUGINS_WILDCARD, RANGER_ADMIN_URL_KEY);
  /** Chain authorization Ranger authentication type kerberos or simple */
  public static final String CHAIN_RANGER_AUTH_TYPES =
      generateChainPluginsKey(CHAIN_PLUGINS_WILDCARD, RANGER_AUTH_TYPE_KEY);
  /** Chain authorization Ranger username */
  public static final String CHAIN_RANGER_USERNAME =
      generateChainPluginsKey(CHAIN_PLUGINS_WILDCARD, RANGER_USERNAME_KEY);
  /**
   * Chain authorization Ranger admin web login user password(auth_type=simple), or path of the
   * keytab file(auth_type=kerberos)
   */
  public static final String CHAIN_RANGER_PASSWORD =
      generateChainPluginsKey(CHAIN_PLUGINS_WILDCARD, RANGER_PASSWORD_KEY);
  /** Chain authorization Ranger service name */
  public static final String CHAIN_RANGER_SERVICE_NAME =
      generateChainPluginsKey(CHAIN_PLUGINS_WILDCARD, RANGER_SERVICE_NAME_KEY);

  public static String generateChainPluginsKey(String pluginName, String key) {
    return String.format("%s.%s.%s", CHAIN_PREFIX, pluginName, key);
  }

  public static String generatePluginKey(String key) {
    return String.format("%s.%s", AUTHORIZATION_PREFIX, key);
  }

  public static String chainKeyToPluginKey(String chainKey, String plugin) {
    return chainKey.replace(String.format("%s.%s", CHAIN_PREFIX, plugin), AUTHORIZATION_PREFIX);
  }

  public static final Map<String, PropertyEntry<?>> AUTHORIZATION_PROPERTY_ENTRIES =
      ImmutableMap.<String, PropertyEntry<?>>builder()
          .put(
              CHAIN_PLUGINS,
              PropertyEntry.wildcardPropertyEntry(CHAIN_PLUGINS, "The Chain authorization plugins"))
          .put(
              CHAIN_PROVIDER,
              PropertyEntry.wildcardPropertyEntry(
                  CHAIN_PROVIDER, "The Chain authorization plugin provider"))
          .put(
              CHAIN_RANGER_SERVICE_NAME,
              PropertyEntry.wildcardPropertyEntry(
                  CHAIN_RANGER_SERVICE_NAME, "The Chain authorization Ranger service name"))
          .put(
              CHAIN_RANGER_ADMIN_URL,
              PropertyEntry.wildcardPropertyEntry(
                  CHAIN_RANGER_ADMIN_URL, "The Chain authorization Ranger admin web URIs"))
          .put(
              CHAIN_RANGER_AUTH_TYPES,
              PropertyEntry.wildcardPropertyEntry(
                  CHAIN_RANGER_AUTH_TYPES,
                  "The Chain authorization Ranger admin web auth type (kerberos/simple)"))
          .put(
              CHAIN_RANGER_USERNAME,
              PropertyEntry.wildcardPropertyEntry(
                  CHAIN_RANGER_USERNAME, "The Chain authorization Ranger admin web login username"))
          .put(
              CHAIN_RANGER_PASSWORD,
              PropertyEntry.wildcardPropertyEntry(
                  CHAIN_RANGER_PASSWORD, "The Chain authorization Ranger admin web login password"))
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
}
