/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.server.auth;

import com.datastrato.gravitino.Configs;
import com.datastrato.gravitino.config.ConfigBuilder;
import com.datastrato.gravitino.config.ConfigConstants;
import com.datastrato.gravitino.config.ConfigEntry;
import org.apache.commons.lang3.StringUtils;

public interface KerberosConfig extends Configs {

  String KERBEROS_CONFIG_PREFIX = "gravitino.authenticator.kerberos.";

  ConfigEntry<String> PRINCIPAL =
      new ConfigBuilder(KERBEROS_CONFIG_PREFIX + "principal")
          .doc("The audience name when Gravitino uses OAuth as the authenticator")
          .version("0.4.0")
          .stringConf()
          .checkValue(StringUtils::isNotBlank, ConfigConstants.NOT_BLANK_ERROR_MSG)
          .create();

  ConfigEntry<String> KEYTAB =
      new ConfigBuilder(KERBEROS_CONFIG_PREFIX + "keytab")
          .doc("The audience name when Gravitino uses OAuth as the authenticator")
          .version("0.4.0")
          .stringConf()
          .checkValue(StringUtils::isNotBlank, ConfigConstants.NOT_BLANK_ERROR_MSG)
          .create();
}
