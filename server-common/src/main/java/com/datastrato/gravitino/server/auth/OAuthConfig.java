/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.server.auth;

import com.datastrato.gravitino.Configs;
import com.datastrato.gravitino.config.ConfigBuilder;
import com.datastrato.gravitino.config.ConfigEntry;
import io.jsonwebtoken.SignatureAlgorithm;

public interface OAuthConfig extends Configs {

  String OAUTH_CONFIG_PREFIX = "gravitino.authenticator.oauth.";

  ConfigEntry<String> SERVICE_AUDIENCE =
      new ConfigBuilder(OAUTH_CONFIG_PREFIX + "serviceAudience")
          .doc("The audience name when Gravitino uses oauth as the authenticator")
          .version("0.3.0")
          .stringConf()
          .createWithDefault("GravitinoServer");

  ConfigEntry<Long> ALLOW_SKEW_SECONDS =
      new ConfigBuilder(OAUTH_CONFIG_PREFIX + "allowSkewSecs")
          .doc("The jwt allows skew seconds when Gravitino uses oauth as the authenticator")
          .version("0.3.0")
          .longConf()
          .createWithDefault(0L);

  ConfigEntry<String> DEFAULT_SIGN_KEY =
      new ConfigBuilder(OAUTH_CONFIG_PREFIX + "defaultSignKey")
          .doc("The sign key of jwt when Gravitino uses oauth as the authenticator")
          .version("0.3.0")
          .stringConf()
          .createWithDefault(null);

  ConfigEntry<String> SIGNATURE_ALGORITHM_TYPE =
      new ConfigBuilder(OAUTH_CONFIG_PREFIX + "signAlgorithmType")
          .doc("The signature algorithm when Gravitino uses oauth as the authenticator")
          .version("0.3.0")
          .stringConf()
          .createWithDefault(SignatureAlgorithm.RS256.name());
}
