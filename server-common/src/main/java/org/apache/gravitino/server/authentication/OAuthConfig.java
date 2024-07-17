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

package org.apache.gravitino.server.authentication;

import io.jsonwebtoken.SignatureAlgorithm;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.config.ConfigBuilder;
import org.apache.gravitino.config.ConfigConstants;
import org.apache.gravitino.config.ConfigEntry;

public interface OAuthConfig {

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
