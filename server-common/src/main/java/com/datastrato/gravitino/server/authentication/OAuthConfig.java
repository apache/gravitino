/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.server.authentication;

import com.datastrato.gravitino.Configs;
import com.datastrato.gravitino.config.ConfigBuilder;
import com.datastrato.gravitino.config.ConfigConstants;
import com.datastrato.gravitino.config.ConfigEntry;
import io.jsonwebtoken.SignatureAlgorithm;
import org.apache.commons.lang3.StringUtils;

public interface OAuthConfig extends Configs {
  String OAUTH_CONFIG_PREFIX = "gravitino.authenticator.oauth.";

  ConfigEntry<String> SERVICE_AUDIENCE =
      new ConfigBuilder(OAUTH_CONFIG_PREFIX + "serviceAudience")
          .doc("The audience name when Gravitino uses OAuth as the authenticator")
          .version(ConfigConstants.VERSION_0_3_0)
          .stringConf()
          .createWithDefault("GravitinoServer");

  ConfigEntry<Long> ALLOW_SKEW_SECONDS =
      new ConfigBuilder(OAUTH_CONFIG_PREFIX + "allowSkewSecs")
          .doc("The JWT allows skew seconds when Gravitino uses OAuth as the authenticator")
          .version(ConfigConstants.VERSION_0_3_0)
          .longConf()
          .createWithDefault(0L);

  ConfigEntry<String> DEFAULT_SIGN_KEY =
      new ConfigBuilder(OAUTH_CONFIG_PREFIX + "defaultSignKey")
          .doc("The signing key of JWT when Gravitino uses OAuth as the authenticator")
          .version(ConfigConstants.VERSION_0_3_0)
          .stringConf()
          .checkValue(StringUtils::isNotBlank, ConfigConstants.NOT_BLANK_ERROR_MSG)
          .create();

  ConfigEntry<String> SIGNATURE_ALGORITHM_TYPE =
      new ConfigBuilder(OAUTH_CONFIG_PREFIX + "signAlgorithmType")
          .doc("The signature algorithm when Gravitino uses OAuth as the authenticator")
          .version(ConfigConstants.VERSION_0_3_0)
          .stringConf()
          .createWithDefault(SignatureAlgorithm.RS256.name());

  ConfigEntry<String> DEFAULT_SERVER_URI =
      new ConfigBuilder(OAUTH_CONFIG_PREFIX + "serverUri")
          .doc("The uri of the default OAuth server")
          .version(ConfigConstants.VERSION_0_3_0)
          .stringConf()
          .checkValue(StringUtils::isNotBlank, ConfigConstants.NOT_BLANK_ERROR_MSG)
          .create();

  ConfigEntry<String> DEFAULT_TOKEN_PATH =
      new ConfigBuilder(OAUTH_CONFIG_PREFIX + "tokenPath")
          .doc("The path for token of the default OAuth server")
          .version(ConfigConstants.VERSION_0_3_0)
          .stringConf()
          .checkValue(StringUtils::isNotBlank, ConfigConstants.NOT_BLANK_ERROR_MSG)
          .create();
}
