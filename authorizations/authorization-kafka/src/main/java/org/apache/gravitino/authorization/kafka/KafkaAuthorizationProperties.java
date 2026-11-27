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
package org.apache.gravitino.authorization.kafka;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import java.util.Collections;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.authorization.common.AuthorizationProperties;
import org.apache.gravitino.connector.BaseCatalog;
import org.apache.gravitino.connector.authorization.BaseAuthorization;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.common.resource.PatternType;

/**
 * Configuration properties parser and validator for {@link KafkaAuthorizationPlugin}.
 *
 * <p>Consumes catalog-level Kafka settings ({@code bootstrap.servers} and {@code
 * gravitino.bypass.*}) alongside plugin-specific {@code authorization.kafka.*} settings.
 */
public class KafkaAuthorizationProperties extends AuthorizationProperties {

  /** Property prefix for Kafka authorization settings. */
  public static final String PREFIX = "authorization.kafka.";

  /** Property prefix whose suffix is forwarded directly to Kafka {@code AdminClient}. */
  public static final String ADMIN_CLIENT_PREFIX = PREFIX + "admin.";

  /** Catalog property specifying Kafka bootstrap servers. */
  public static final String BOOTSTRAP_SERVERS = "bootstrap.servers";

  /** Optional plugin override specifying Kafka bootstrap servers. */
  public static final String AUTHZ_BOOTSTRAP_SERVERS = PREFIX + "bootstrap.servers";

  /** Principal type prefix used for Gravitino users in Kafka ACL entries (default {@code User}). */
  public static final String USER_PRINCIPAL_TYPE = PREFIX + "user-principal-type";

  /**
   * Principal type prefix used for Gravitino groups in Kafka ACL entries (default {@code Group}).
   */
  public static final String GROUP_PRINCIPAL_TYPE = PREFIX + "group-principal-type";

  /** Resource name pattern for Kafka consumer group ACLs granted on {@code CONSUME_TOPIC}. */
  public static final String CONSUMER_GROUP_PATTERN = PREFIX + "consumer-group-pattern";

  /** Pattern type ({@code LITERAL} or {@code PREFIXED}) for {@link #CONSUMER_GROUP_PATTERN}. */
  public static final String CONSUMER_GROUP_PATTERN_TYPE = PREFIX + "consumer-group-pattern-type";

  /** Host filter written to Kafka ACL entries (default {@code *}). */
  public static final String ACL_HOST = PREFIX + "host";

  /** Whether {@code ALLOW PRODUCE_TOPIC} also grants {@code CLUSTER:IDEMPOTENT_WRITE}. */
  public static final String IDEMPOTENT_WRITE_ACL_ENABLED = PREFIX + "idempotent-write-acl.enabled";

  /**
   * Whether Gravitino group role grants and group ownership are pushed to Kafka ACLs. Disabled by
   * default because Apache Kafka's built-in {@code StandardAuthorizer} does not expand group
   * membership unless paired with a group-aware {@code Authorizer} or {@code
   * KafkaPrincipalBuilder}.
   */
  public static final String GROUP_ACL_ENABLED = PREFIX + "group-acl.enabled";

  /** Timeout in milliseconds for Kafka {@code AdminClient} ACL requests (default {@code 30000}). */
  public static final String REQUEST_TIMEOUT_MS = PREFIX + "request-timeout-ms";

  /** Default Kafka user principal type prefix. */
  public static final String DEFAULT_USER_PRINCIPAL_TYPE = "User";

  /** Default Kafka group principal type prefix. */
  public static final String DEFAULT_GROUP_PRINCIPAL_TYPE = "Group";

  /** Default consumer group resource pattern. */
  public static final String DEFAULT_CONSUMER_GROUP_PATTERN = "*";

  /** Default consumer group resource pattern type. */
  public static final String DEFAULT_CONSUMER_GROUP_PATTERN_TYPE = PatternType.LITERAL.name();

  /** Default ACL host value. */
  public static final String DEFAULT_ACL_HOST = "*";

  /** Default flag for granting cluster idempotent write on produce grants. */
  public static final boolean DEFAULT_IDEMPOTENT_WRITE_ACL_ENABLED = true;

  /** Default flag for pushing group ACLs to Kafka. */
  public static final boolean DEFAULT_GROUP_ACL_ENABLED = false;

  /** Default Kafka AdminClient request timeout in milliseconds. */
  public static final int DEFAULT_REQUEST_TIMEOUT_MS = 30000;

  // Kafka resolves "${provider:path:key}" placeholders in client configs through
  // "config.providers",
  // which can read the Gravitino server's own environment variables and files.
  private static final String CONFIG_PROVIDERS_KEY = "config.providers";

  private final Map<String, String> rawConfig;

  /**
   * Constructs {@link KafkaAuthorizationProperties} from raw catalog and authorization config.
   *
   * @param properties Configuration map passed to {@link KafkaAuthorization#newPlugin}.
   */
  public KafkaAuthorizationProperties(Map<String, String> properties) {
    super(properties == null ? Collections.emptyMap() : properties);
    ImmutableMap.Builder<String, String> builder = ImmutableMap.builder();
    if (properties != null) {
      properties.forEach(
          (k, v) -> {
            if (k != null && v != null) {
              builder.put(k, v);
            }
          });
    }
    this.rawConfig = builder.build();
  }

  @Override
  public String getPropertiesPrefix() {
    return PREFIX;
  }

  @Override
  public void validate() {
    String bootstrapServers = getBootstrapServers();
    if (StringUtils.isBlank(bootstrapServers)) {
      throw new IllegalArgumentException(
          String.format(
              "Missing required Kafka bootstrap servers configuration (%s or %s)",
              BOOTSTRAP_SERVERS, AUTHZ_BOOTSTRAP_SERVERS));
    }

    if (StringUtils.isBlank(getUserPrincipalType())) {
      throw new IllegalArgumentException(USER_PRINCIPAL_TYPE + " must not be blank");
    }
    if ("*".equals(getUserPrincipalType())) {
      throw new IllegalArgumentException(USER_PRINCIPAL_TYPE + " must not be wildcard '*'");
    }
    if (StringUtils.isBlank(getGroupPrincipalType())) {
      throw new IllegalArgumentException(GROUP_PRINCIPAL_TYPE + " must not be blank");
    }
    if ("*".equals(getGroupPrincipalType())) {
      throw new IllegalArgumentException(GROUP_PRINCIPAL_TYPE + " must not be wildcard '*'");
    }
    if (StringUtils.isBlank(getConsumerGroupPattern())) {
      throw new IllegalArgumentException(CONSUMER_GROUP_PATTERN + " must not be blank");
    }
    if (StringUtils.isBlank(getAclHost())) {
      throw new IllegalArgumentException(ACL_HOST + " must not be blank");
    }

    // Validate consumer group pattern type
    getConsumerGroupPatternType();

    // Validate boolean flags strictly
    isIdempotentWriteAclEnabled();
    isGroupAclEnabled();

    // Validate request timeout
    int timeoutMs = getRequestTimeoutMs();
    if (timeoutMs <= 0) {
      throw new IllegalArgumentException(REQUEST_TIMEOUT_MS + " must be greater than 0");
    }

    // Validate catalog ID if provided
    getCatalogId();

    checkNoConfigProviders();
  }

  /**
   * Returns the resolved Kafka bootstrap servers string.
   *
   * @return The Kafka bootstrap servers.
   */
  public String getBootstrapServers() {
    String override = properties.get(AUTHZ_BOOTSTRAP_SERVERS);
    if (StringUtils.isNotBlank(override)) {
      return override.trim();
    }
    String catalogBootstrap = rawConfig.get(BOOTSTRAP_SERVERS);
    return catalogBootstrap == null ? null : catalogBootstrap.trim();
  }

  /**
   * Returns the Kafka principal type prefix used for Gravitino users.
   *
   * @return Principal type string (e.g. {@code User}).
   */
  public String getUserPrincipalType() {
    return properties.getOrDefault(USER_PRINCIPAL_TYPE, DEFAULT_USER_PRINCIPAL_TYPE).trim();
  }

  /**
   * Returns the Kafka principal type prefix used for Gravitino groups.
   *
   * @return Principal type string (e.g. {@code Group}).
   */
  public String getGroupPrincipalType() {
    return properties.getOrDefault(GROUP_PRINCIPAL_TYPE, DEFAULT_GROUP_PRINCIPAL_TYPE).trim();
  }

  /**
   * Returns the Kafka consumer group resource name pattern granted on {@code CONSUME_TOPIC}.
   *
   * @return Consumer group pattern string.
   */
  public String getConsumerGroupPattern() {
    return properties.getOrDefault(CONSUMER_GROUP_PATTERN, DEFAULT_CONSUMER_GROUP_PATTERN).trim();
  }

  /**
   * Returns the Kafka {@link PatternType} for consumer group ACLs.
   *
   * @return {@link PatternType#LITERAL} or {@link PatternType#PREFIXED}.
   */
  public PatternType getConsumerGroupPatternType() {
    String raw =
        properties
            .getOrDefault(CONSUMER_GROUP_PATTERN_TYPE, DEFAULT_CONSUMER_GROUP_PATTERN_TYPE)
            .trim()
            .toUpperCase(Locale.ROOT);
    if (PatternType.LITERAL.name().equals(raw)) {
      return PatternType.LITERAL;
    }
    if (PatternType.PREFIXED.name().equals(raw)) {
      return PatternType.PREFIXED;
    }
    throw new IllegalArgumentException(
        String.format(
            "Unsupported %s '%s'; expected LITERAL or PREFIXED", CONSUMER_GROUP_PATTERN_TYPE, raw));
  }

  /**
   * Returns the host value applied to created Kafka ACL entries.
   *
   * @return ACL host string (default {@code *}).
   */
  public String getAclHost() {
    return properties.getOrDefault(ACL_HOST, DEFAULT_ACL_HOST).trim();
  }

  /**
   * Returns whether {@code ALLOW PRODUCE_TOPIC} includes {@code CLUSTER:IDEMPOTENT_WRITE}.
   *
   * @return {@code true} if idempotent write ACLs should be emitted.
   */
  public boolean isIdempotentWriteAclEnabled() {
    return parseStrictBoolean(IDEMPOTENT_WRITE_ACL_ENABLED, DEFAULT_IDEMPOTENT_WRITE_ACL_ENABLED);
  }

  /**
   * Returns whether Gravitino group grants and group ownership are pushed to Kafka ACLs.
   *
   * @return {@code true} if group ACL push-down is enabled.
   */
  public boolean isGroupAclEnabled() {
    return parseStrictBoolean(GROUP_ACL_ENABLED, DEFAULT_GROUP_ACL_ENABLED);
  }

  private boolean parseStrictBoolean(String key, boolean defaultValue) {
    String raw = properties.get(key);
    if (raw == null) {
      return defaultValue;
    }
    String trimmed = raw.trim();
    if ("true".equalsIgnoreCase(trimmed)) {
      return true;
    }
    if ("false".equalsIgnoreCase(trimmed)) {
      return false;
    }
    throw new IllegalArgumentException(
        String.format("Invalid boolean value '%s' for %s; expected 'true' or 'false'", raw, key));
  }

  /**
   * Returns the Kafka {@code AdminClient} request timeout in milliseconds.
   *
   * @return Positive request timeout in milliseconds.
   */
  public int getRequestTimeoutMs() {
    String raw = properties.get(REQUEST_TIMEOUT_MS);
    if (raw == null) {
      return DEFAULT_REQUEST_TIMEOUT_MS;
    }
    try {
      int parsed = Integer.parseInt(raw.trim());
      if (parsed <= 0) {
        throw new IllegalArgumentException(REQUEST_TIMEOUT_MS + " must be greater than 0");
      }
      return parsed;
    } catch (NumberFormatException e) {
      throw new IllegalArgumentException(
          String.format("Invalid integer value '%s' for %s", raw, REQUEST_TIMEOUT_MS), e);
    }
  }

  /**
   * Returns the Gravitino catalog entity ID injected by {@code BaseAuthorization}, if present.
   *
   * @return The catalog ID, or {@code null} if absent.
   */
  public Long getCatalogId() {
    String rawId = rawConfig.get(BaseAuthorization.CATALOG_ID);
    if (StringUtils.isBlank(rawId)) {
      return null;
    }
    try {
      return Long.parseLong(rawId.trim());
    } catch (NumberFormatException e) {
      throw new IllegalArgumentException(
          String.format("Invalid catalog ID '%s' in %s", rawId, BaseAuthorization.CATALOG_ID), e);
    }
  }

  /**
   * Formats a Gravitino user name into a Kafka ACL principal string.
   *
   * @param username Gravitino user name.
   * @return Formatted Kafka principal (e.g. {@code User:alice}).
   */
  public String formatUserPrincipal(String username) {
    Preconditions.checkArgument(StringUtils.isNotBlank(username), "Username must not be blank");
    Preconditions.checkArgument(
        !"*".equals(username.trim()),
        "Wildcard Kafka principal name '*' is not allowed for Gravitino user ACLs");
    return getUserPrincipalType() + ":" + username;
  }

  /**
   * Formats a Gravitino group name into a Kafka ACL principal string.
   *
   * @param groupName Gravitino group name.
   * @return Formatted Kafka principal (e.g. {@code Group:analytics}).
   */
  public String formatGroupPrincipal(String groupName) {
    Preconditions.checkArgument(StringUtils.isNotBlank(groupName), "Group name must not be blank");
    Preconditions.checkArgument(
        !"*".equals(groupName.trim()),
        "Wildcard Kafka principal name '*' is not allowed for Gravitino group ACLs");
    return getGroupPrincipalType() + ":" + groupName;
  }

  /**
   * Builds Kafka {@link Properties} suitable for constructing a Kafka {@code AdminClient}.
   *
   * <p>Copies all {@code gravitino.bypass.*} entries (with prefix stripped), applies {@code
   * bootstrap.servers} and default request timeouts, and applies any {@code
   * authorization.kafka.admin.*} overrides.
   *
   * @return Properties configured for Kafka {@code AdminClient}.
   */
  public Properties toAdminClientProperties() {
    checkNoConfigProviders();
    Properties adminProps = new Properties();
    rawConfig.forEach(
        (key, value) -> {
          if (key.startsWith(BaseCatalog.CATALOG_BYPASS_PREFIX)) {
            String stripped = key.substring(BaseCatalog.CATALOG_BYPASS_PREFIX.length());
            if (StringUtils.isNotBlank(stripped)) {
              adminProps.put(stripped, value);
            }
          }
        });

    String bootstrapServers = getBootstrapServers();
    if (StringUtils.isNotBlank(bootstrapServers)) {
      adminProps.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    }

    String timeoutValue = String.valueOf(getRequestTimeoutMs());
    if (properties.containsKey(REQUEST_TIMEOUT_MS)) {
      adminProps.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, timeoutValue);
      adminProps.put(AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, timeoutValue);
    } else {
      adminProps.putIfAbsent(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, timeoutValue);
      adminProps.putIfAbsent(AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, timeoutValue);
    }

    rawConfig.forEach(
        (key, value) -> {
          if (key.startsWith(ADMIN_CLIENT_PREFIX)) {
            String stripped = key.substring(ADMIN_CLIENT_PREFIX.length());
            if (StringUtils.isNotBlank(stripped)) {
              adminProps.put(stripped, value);
            }
          }
        });

    if (!adminProps.containsKey(AdminClientConfig.CLIENT_ID_CONFIG)) {
      Long catalogId = getCatalogId();
      adminProps.put(
          AdminClientConfig.CLIENT_ID_CONFIG,
          catalogId != null
              ? "gravitino-kafka-authz-" + catalogId
              : "gravitino-kafka-authz-plugin");
    }
    return adminProps;
  }

  private void checkNoConfigProviders() {
    String disallowedKeys =
        rawConfig.keySet().stream()
            .filter(KafkaAuthorizationProperties::isConfigProviderKey)
            .sorted()
            .collect(Collectors.joining(", "));
    Preconditions.checkArgument(
        disallowedKeys.isEmpty(),
        "Kafka config providers are not supported, please remove the properties: %s",
        disallowedKeys);
  }

  private static boolean isConfigProviderKey(String key) {
    String bypassProvidersKey = BaseCatalog.CATALOG_BYPASS_PREFIX + CONFIG_PROVIDERS_KEY;
    String adminProvidersKey = ADMIN_CLIENT_PREFIX + CONFIG_PROVIDERS_KEY;
    return key.equals(bypassProvidersKey)
        || key.startsWith(bypassProvidersKey + ".")
        || key.equals(adminProvidersKey)
        || key.startsWith(adminProvidersKey + ".");
  }
}
