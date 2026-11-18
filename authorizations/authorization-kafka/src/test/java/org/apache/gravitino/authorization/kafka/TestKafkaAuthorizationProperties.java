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

import com.google.common.collect.ImmutableMap;
import java.util.Map;
import java.util.Properties;
import org.apache.gravitino.authorization.common.ChainedAuthorizationProperties;
import org.apache.gravitino.connector.authorization.BaseAuthorization;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.common.resource.PatternType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestKafkaAuthorizationProperties {

  @Test
  public void testDefaultsAndBypassExtraction() {
    Map<String, String> raw =
        ImmutableMap.<String, String>builder()
            .put("bootstrap.servers", "localhost:9092")
            .put("gravitino.bypass.security.protocol", "SASL_PLAINTEXT")
            .put("gravitino.bypass.sasl.mechanism", "OAUTHBEARER")
            .put("authorization.kafka.admin.security.protocol", "SSL")
            .put(BaseAuthorization.CATALOG_ID, "42")
            .build();

    KafkaAuthorizationProperties props = new KafkaAuthorizationProperties(raw);
    props.validate();

    Assertions.assertEquals("localhost:9092", props.getBootstrapServers());
    Assertions.assertEquals("User", props.getUserPrincipalType());
    Assertions.assertEquals("Group", props.getGroupPrincipalType());
    Assertions.assertEquals("*", props.getConsumerGroupPattern());
    Assertions.assertEquals(PatternType.LITERAL, props.getConsumerGroupPatternType());
    Assertions.assertEquals("*", props.getAclHost());
    Assertions.assertTrue(props.isIdempotentWriteAclEnabled());
    Assertions.assertFalse(props.isGroupAclEnabled());
    Assertions.assertEquals(30000, props.getRequestTimeoutMs());
    Assertions.assertEquals(42L, props.getCatalogId());
    Assertions.assertEquals("User:alice", props.formatUserPrincipal("alice"));
    Assertions.assertEquals("Group:devs", props.formatGroupPrincipal("devs"));

    Properties adminProps = props.toAdminClientProperties();
    Assertions.assertEquals(
        "localhost:9092", adminProps.getProperty(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG));
    // authorization.kafka.admin.* overrides gravitino.bypass.*
    Assertions.assertEquals("SSL", adminProps.getProperty("security.protocol"));
    Assertions.assertEquals("OAUTHBEARER", adminProps.getProperty("sasl.mechanism"));
    Assertions.assertEquals(
        "30000", adminProps.getProperty(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG));
  }

  @Test
  public void testCustomSettingsAndOverrides() {
    Map<String, String> raw =
        ImmutableMap.<String, String>builder()
            .put("bootstrap.servers", "catalog-broker:9092")
            .put("authorization.kafka.bootstrap.servers", "authz-broker:9093")
            .put("authorization.kafka.user-principal-type", "OAuthPrincipal")
            .put("authorization.kafka.group-principal-type", "OAuthGroup")
            .put("authorization.kafka.consumer-group-pattern", "svc-")
            .put("authorization.kafka.consumer-group-pattern-type", "prefixed")
            .put("authorization.kafka.host", "10.0.0.1")
            .put("authorization.kafka.idempotent-write-acl.enabled", "false")
            .put("authorization.kafka.group-acl.enabled", "true")
            .put("authorization.kafka.request-timeout-ms", "15000")
            .build();

    KafkaAuthorizationProperties props = new KafkaAuthorizationProperties(raw);
    props.validate();

    Assertions.assertEquals("authz-broker:9093", props.getBootstrapServers());
    Assertions.assertEquals("OAuthPrincipal:alice", props.formatUserPrincipal("alice"));
    Assertions.assertEquals("OAuthGroup:analysts", props.formatGroupPrincipal("analysts"));
    Assertions.assertEquals("svc-", props.getConsumerGroupPattern());
    Assertions.assertEquals(PatternType.PREFIXED, props.getConsumerGroupPatternType());
    Assertions.assertEquals("10.0.0.1", props.getAclHost());
    Assertions.assertFalse(props.isIdempotentWriteAclEnabled());
    Assertions.assertTrue(props.isGroupAclEnabled());
    Assertions.assertEquals(15000, props.getRequestTimeoutMs());

    // Explicit authorization.kafka.request-timeout-ms overrides gravitino.bypass.* timeouts, while
    // authorization.kafka.admin.* still takes highest precedence.
    KafkaAuthorizationProperties overrideBypassTimeout =
        new KafkaAuthorizationProperties(
            ImmutableMap.of(
                "bootstrap.servers",
                "localhost:9092",
                "gravitino.bypass.request.timeout.ms",
                "60000",
                "gravitino.bypass.default.api.timeout.ms",
                "60000",
                "authorization.kafka.request-timeout-ms",
                "12000"));
    overrideBypassTimeout.validate();
    Properties overrideAdminProps = overrideBypassTimeout.toAdminClientProperties();
    Assertions.assertEquals(
        "12000", overrideAdminProps.getProperty(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG));
    Assertions.assertEquals(
        "12000", overrideAdminProps.getProperty(AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG));

    KafkaAuthorizationProperties explicitAdminTimeout =
        new KafkaAuthorizationProperties(
            ImmutableMap.of(
                "bootstrap.servers",
                "localhost:9092",
                "gravitino.bypass.request.timeout.ms",
                "60000",
                "authorization.kafka.request-timeout-ms",
                "12000",
                "authorization.kafka.admin.request.timeout.ms",
                "8000"));
    explicitAdminTimeout.validate();
    Assertions.assertEquals(
        "8000",
        explicitAdminTimeout
            .toAdminClientProperties()
            .getProperty(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG));
  }

  @Test
  public void testValidationFailures() {
    KafkaAuthorizationProperties missingBootstrap =
        new KafkaAuthorizationProperties(ImmutableMap.of());
    Assertions.assertThrows(IllegalArgumentException.class, missingBootstrap::validate);

    KafkaAuthorizationProperties invalidPatternType =
        new KafkaAuthorizationProperties(
            ImmutableMap.of(
                "bootstrap.servers",
                "localhost:9092",
                "authorization.kafka.consumer-group-pattern-type",
                "MATCH"));
    Assertions.assertThrows(IllegalArgumentException.class, invalidPatternType::validate);

    KafkaAuthorizationProperties invalidBoolean =
        new KafkaAuthorizationProperties(
            ImmutableMap.of(
                "bootstrap.servers",
                "localhost:9092",
                "authorization.kafka.idempotent-write-acl.enabled",
                "yes"));
    Assertions.assertThrows(IllegalArgumentException.class, invalidBoolean::validate);

    KafkaAuthorizationProperties nonPositiveTimeout =
        new KafkaAuthorizationProperties(
            ImmutableMap.of(
                "bootstrap.servers",
                "localhost:9092",
                "authorization.kafka.request-timeout-ms",
                "0"));
    Assertions.assertThrows(IllegalArgumentException.class, nonPositiveTimeout::validate);

    KafkaAuthorizationProperties invalidGroupAclBoolean =
        new KafkaAuthorizationProperties(
            ImmutableMap.of(
                "bootstrap.servers",
                "localhost:9092",
                "authorization.kafka.group-acl.enabled",
                "enabled"));
    Assertions.assertThrows(IllegalArgumentException.class, invalidGroupAclBoolean::validate);

    KafkaAuthorizationProperties wildcardPrincipalType =
        new KafkaAuthorizationProperties(
            ImmutableMap.of(
                "bootstrap.servers",
                "localhost:9092",
                "authorization.kafka.user-principal-type",
                "*"));
    Assertions.assertThrows(IllegalArgumentException.class, wildcardPrincipalType::validate);

    KafkaAuthorizationProperties validProps =
        new KafkaAuthorizationProperties(
            ImmutableMap.of(
                "bootstrap.servers",
                "localhost:9092",
                "authorization.kafka.group-principal-type",
                "User"));
    validProps.validate();
    Assertions.assertThrows(
        IllegalArgumentException.class, () -> validProps.formatUserPrincipal("*"));
    Assertions.assertThrows(
        IllegalArgumentException.class, () -> validProps.formatUserPrincipal(" * "));
    Assertions.assertThrows(
        IllegalArgumentException.class, () -> validProps.formatGroupPrincipal("*"));
    Assertions.assertThrows(
        IllegalArgumentException.class, () -> validProps.formatGroupPrincipal(" * "));

    for (String disallowedKey :
        new String[] {
          "gravitino.bypass.config.providers",
          "gravitino.bypass.config.providers.env.class",
          "authorization.kafka.admin.config.providers",
          "authorization.kafka.admin.config.providers.file.class"
        }) {
      KafkaAuthorizationProperties propsWithConfigProvider =
          new KafkaAuthorizationProperties(
              ImmutableMap.of("bootstrap.servers", "localhost:9092", disallowedKey, "env"));
      IllegalArgumentException ex =
          Assertions.assertThrows(
              IllegalArgumentException.class, propsWithConfigProvider::validate);
      Assertions.assertTrue(ex.getMessage().contains(disallowedKey), ex.getMessage());
      Assertions.assertThrows(
          IllegalArgumentException.class, propsWithConfigProvider::toAdminClientProperties);
    }

    KafkaAuthorizationProperties similarKeyProps =
        new KafkaAuthorizationProperties(
            ImmutableMap.of(
                "bootstrap.servers",
                "localhost:9092",
                "gravitino.bypass.config.providersx",
                "ignored",
                "authorization.kafka.admin.config.providersx",
                "ignored"));
    Assertions.assertDoesNotThrow(similarKeyProps::validate);
  }

  @Test
  public void testChainedAuthorizationPropertiesCompatibility() {
    Map<String, String> catalogProps =
        ImmutableMap.<String, String>builder()
            .put("bootstrap.servers", "catalog-broker:9092")
            .put("authorization-provider", "chain")
            .put("authorization.chain.plugins", "kafka-acl")
            .put("authorization.chain.kafka-acl.provider", "kafka")
            .put(
                "authorization.chain.kafka-acl.kafka.bootstrap.servers",
                "chained-authz-broker:9093")
            .put("authorization.chain.kafka-acl.kafka.admin.security.protocol", "SASL_SSL")
            .put("authorization.chain.kafka-acl.kafka.admin.sasl.mechanism", "OAUTHBEARER")
            .put("authorization.chain.kafka-acl.kafka.host", "10.1.2.3")
            .build();

    ChainedAuthorizationProperties chainedProps = new ChainedAuthorizationProperties(catalogProps);
    chainedProps.validate();
    Map<String, String> forwarded = chainedProps.fetchAuthPluginProperties("kafka-acl");

    KafkaAuthorizationProperties kafkaProps = new KafkaAuthorizationProperties(forwarded);
    kafkaProps.validate();

    Assertions.assertEquals("chained-authz-broker:9093", kafkaProps.getBootstrapServers());
    Assertions.assertEquals("10.1.2.3", kafkaProps.getAclHost());
    Properties adminProps = kafkaProps.toAdminClientProperties();
    Assertions.assertEquals(
        "chained-authz-broker:9093",
        adminProps.getProperty(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG));
    Assertions.assertEquals("SASL_SSL", adminProps.getProperty("security.protocol"));
    Assertions.assertEquals("OAUTHBEARER", adminProps.getProperty("sasl.mechanism"));
  }
}
