/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.catalog.hadoop.kerberos;

import com.datastrato.gravitino.Config;
import com.datastrato.gravitino.config.ConfigBuilder;
import com.datastrato.gravitino.config.ConfigConstants;
import com.datastrato.gravitino.config.ConfigEntry;
import com.datastrato.gravitino.connector.PropertyEntry;
import com.google.common.collect.ImmutableMap;
import java.util.Map;

public class AuthenticationConfig extends Config {
  public static final String ENABLE_AUTH_KEY = "authentication.enable";
  public static final String AUTH_TYPE_KEY = "authentication.type";

  public AuthenticationConfig(Map<String, String> properties) {
    super(false);
    loadFromMap(properties, k -> true);
  }

  public static final ConfigEntry<Boolean> ENABLE_AUTH_ENTRY =
      new ConfigBuilder(ENABLE_AUTH_KEY)
          .doc("Whether to enable authentication for Hadoop catalog")
          .version(ConfigConstants.VERSION_0_5_1)
          .booleanConf()
          .createWithDefault(false);

  public static final ConfigEntry<String> AUTH_TYPE_ENTRY =
      new ConfigBuilder(AUTH_TYPE_KEY)
          .doc("The type of authentication for Hadoop catalog, currently we only support kerberos")
          .version(ConfigConstants.VERSION_0_5_1)
          .stringConf()
          .create();

  public boolean isEnableAuth() {
    return get(ENABLE_AUTH_ENTRY);
  }

  public String getAuthType() {
    return get(AUTH_TYPE_ENTRY);
  }

  public static final Map<String, PropertyEntry<?>> AUTHENTICATION_PROPERTY_ENTRIES =
      new ImmutableMap.Builder<String, PropertyEntry<?>>()
          .put(
              ENABLE_AUTH_KEY,
              PropertyEntry.booleanPropertyEntry(
                  ENABLE_AUTH_KEY,
                  "Whether to enable authentication for Hadoop catalog",
                  false,
                  true,
                  false,
                  false,
                  false))
          .put(
              AUTH_TYPE_KEY,
              PropertyEntry.stringImmutablePropertyEntry(
                  AUTH_TYPE_KEY,
                  "The type of authentication for Hadoop catalog, currently we only support kerberos",
                  false,
                  null,
                  false,
                  false))
          .build();
}
