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
package org.apache.gravitino.maintenance.policy;

import java.util.Optional;
import org.apache.gravitino.config.ConfigBuilder;
import org.apache.gravitino.config.ConfigConstants;
import org.apache.gravitino.config.ConfigEntry;

/**
 * Optional TMS cluster-static client auth keys in {@code gravitino.conf}.
 *
 * <p>Credentials are not stored in policy content. Suffix keys are shared by the Gravitino control
 * plane prefix and the Iceberg REST prefix; only the prefix differs.
 */
public final class MaintenanceAuthConfigs {

  /** Prefix for Gravitino control-plane client auth. */
  public static final String GRAVITINO_AUTH_PREFIX = "gravitino.maintenance.gravitinoAuth.";

  /** Prefix for Iceberg REST client auth used by Spark maintenance jobs. */
  public static final String IRC_AUTH_PREFIX = "gravitino.maintenance.ircAuth.";

  /** Auth mode suffix: {@code none}, {@code basic}, {@code oauth2}, or {@code kerberos}. */
  public static final String AUTH_TYPE_SUFFIX = "authType";

  /** HTTP basic user principal suffix. */
  public static final String USERNAME_SUFFIX = "username";

  /** HTTP basic password or secret reference suffix. */
  public static final String PASSWORD_SUFFIX = "password";

  /** OAuth2 token endpoint URL suffix. */
  public static final String OAUTH2_SERVER_URI_SUFFIX = "oauth2ServerUri";

  /** OAuth2 client credential suffix, often {@code client_id:client_secret}. */
  public static final String OAUTH_CREDENTIAL_SUFFIX = "oauthCredential";

  /** OAuth2 scope suffix. */
  public static final String OAUTH_SCOPE_SUFFIX = "oauthScope";

  /** Kerberos service principal suffix. */
  public static final String KERBEROS_PRINCIPAL_SUFFIX = "kerberosPrincipal";

  /** Kerberos keytab path or secret reference suffix. */
  public static final String KEYTAB_SUFFIX = "keytab";

  public static final ConfigEntry<Optional<String>> GRAVITINO_MAINTENANCE_GRAVITINO_AUTH_TYPE =
      optionalAuth(GRAVITINO_AUTH_PREFIX, AUTH_TYPE_SUFFIX, "TMS Gravitino client auth mode.");

  public static final ConfigEntry<Optional<String>> GRAVITINO_MAINTENANCE_GRAVITINO_AUTH_USERNAME =
      optionalAuth(GRAVITINO_AUTH_PREFIX, USERNAME_SUFFIX, "TMS Gravitino client HTTP basic user.");

  public static final ConfigEntry<Optional<String>> GRAVITINO_MAINTENANCE_GRAVITINO_AUTH_PASSWORD =
      optionalAuth(
          GRAVITINO_AUTH_PREFIX, PASSWORD_SUFFIX, "TMS Gravitino client HTTP basic password.");

  public static final ConfigEntry<Optional<String>>
      GRAVITINO_MAINTENANCE_GRAVITINO_AUTH_OAUTH2_SERVER_URI =
          optionalAuth(
              GRAVITINO_AUTH_PREFIX,
              OAUTH2_SERVER_URI_SUFFIX,
              "TMS Gravitino client OAuth2 token endpoint URL.");

  public static final ConfigEntry<Optional<String>>
      GRAVITINO_MAINTENANCE_GRAVITINO_AUTH_OAUTH_CREDENTIAL =
          optionalAuth(
              GRAVITINO_AUTH_PREFIX,
              OAUTH_CREDENTIAL_SUFFIX,
              "TMS Gravitino client OAuth2 client credential.");

  public static final ConfigEntry<Optional<String>>
      GRAVITINO_MAINTENANCE_GRAVITINO_AUTH_OAUTH_SCOPE =
          optionalAuth(
              GRAVITINO_AUTH_PREFIX, OAUTH_SCOPE_SUFFIX, "TMS Gravitino client OAuth2 scope.");

  public static final ConfigEntry<Optional<String>>
      GRAVITINO_MAINTENANCE_GRAVITINO_AUTH_KERBEROS_PRINCIPAL =
          optionalAuth(
              GRAVITINO_AUTH_PREFIX,
              KERBEROS_PRINCIPAL_SUFFIX,
              "TMS Gravitino client Kerberos principal.");

  public static final ConfigEntry<Optional<String>> GRAVITINO_MAINTENANCE_GRAVITINO_AUTH_KEYTAB =
      optionalAuth(
          GRAVITINO_AUTH_PREFIX, KEYTAB_SUFFIX, "TMS Gravitino client Kerberos keytab path.");

  public static final ConfigEntry<Optional<String>> GRAVITINO_MAINTENANCE_IRC_AUTH_TYPE =
      optionalAuth(IRC_AUTH_PREFIX, AUTH_TYPE_SUFFIX, "TMS Iceberg REST client auth mode.");

  public static final ConfigEntry<Optional<String>> GRAVITINO_MAINTENANCE_IRC_AUTH_USERNAME =
      optionalAuth(IRC_AUTH_PREFIX, USERNAME_SUFFIX, "TMS Iceberg REST client HTTP basic user.");

  public static final ConfigEntry<Optional<String>> GRAVITINO_MAINTENANCE_IRC_AUTH_PASSWORD =
      optionalAuth(
          IRC_AUTH_PREFIX, PASSWORD_SUFFIX, "TMS Iceberg REST client HTTP basic password.");

  public static final ConfigEntry<Optional<String>>
      GRAVITINO_MAINTENANCE_IRC_AUTH_OAUTH2_SERVER_URI =
          optionalAuth(
              IRC_AUTH_PREFIX,
              OAUTH2_SERVER_URI_SUFFIX,
              "TMS Iceberg REST client OAuth2 token endpoint URL.");

  public static final ConfigEntry<Optional<String>>
      GRAVITINO_MAINTENANCE_IRC_AUTH_OAUTH_CREDENTIAL =
          optionalAuth(
              IRC_AUTH_PREFIX,
              OAUTH_CREDENTIAL_SUFFIX,
              "TMS Iceberg REST client OAuth2 client credential.");

  public static final ConfigEntry<Optional<String>> GRAVITINO_MAINTENANCE_IRC_AUTH_OAUTH_SCOPE =
      optionalAuth(IRC_AUTH_PREFIX, OAUTH_SCOPE_SUFFIX, "TMS Iceberg REST client OAuth2 scope.");

  public static final ConfigEntry<Optional<String>>
      GRAVITINO_MAINTENANCE_IRC_AUTH_KERBEROS_PRINCIPAL =
          optionalAuth(
              IRC_AUTH_PREFIX,
              KERBEROS_PRINCIPAL_SUFFIX,
              "TMS Iceberg REST client Kerberos principal.");

  public static final ConfigEntry<Optional<String>> GRAVITINO_MAINTENANCE_IRC_AUTH_KEYTAB =
      optionalAuth(IRC_AUTH_PREFIX, KEYTAB_SUFFIX, "TMS Iceberg REST client Kerberos keytab path.");

  private MaintenanceAuthConfigs() {}

  private static ConfigEntry<Optional<String>> optionalAuth(
      String prefix, String suffix, String doc) {
    return new ConfigBuilder(prefix + suffix)
        .doc(doc)
        .version(ConfigConstants.VERSION_2_0_0)
        .stringConf()
        .createWithOptional();
  }
}
