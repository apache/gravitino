/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.catalog.lakehouse.iceberg.authentication.kerberos;

import com.datastrato.gravitino.catalog.lakehouse.iceberg.authentication.AuthenticationConfig;
import com.datastrato.gravitino.config.ConfigBuilder;
import com.datastrato.gravitino.config.ConfigConstants;
import com.datastrato.gravitino.config.ConfigEntry;
import com.datastrato.gravitino.connector.PropertyEntry;
import com.google.common.collect.ImmutableMap;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;

public class KerberosConfig extends AuthenticationConfig {
  public static final String KET_TAB_URI_KEY = "authentication.kerberos.keytab-uri";

  public static final String PRINCIPAL_KEY = "authentication.kerberos.principal";

  public static final String CHECK_INTERVAL_SEC_KEY = "authentication.kerberos.check-interval-sec";

  public static final String FETCH_TIMEOUT_SEC_KEY =
      "authentication.kerberos.keytab-fetch-timeout-sec";

  public static final String GRAVITINO_KEYTAB_FORMAT = "keytabs/gravitino-%s-keytab";

  public static final ConfigEntry<String> PRINCIPAL_ENTRY =
      new ConfigBuilder(PRINCIPAL_KEY)
          .doc("The principal of the Kerberos for Iceberg catalog with Kerberos authentication")
          .version(ConfigConstants.VERSION_0_6_0)
          .stringConf()
          .checkValue(StringUtils::isNotBlank, ConfigConstants.NOT_BLANK_ERROR_MSG)
          .create();

  public static final ConfigEntry<String> KEYTAB_ENTRY =
      new ConfigBuilder(KET_TAB_URI_KEY)
          .doc("The keytab of the Kerberos for Iceberg catalog with Kerberos authentication")
          .version(ConfigConstants.VERSION_0_6_0)
          .stringConf()
          .checkValue(StringUtils::isNotBlank, ConfigConstants.NOT_BLANK_ERROR_MSG)
          .create();

  public static final ConfigEntry<Integer> CHECK_INTERVAL_SEC_ENTRY =
      new ConfigBuilder(CHECK_INTERVAL_SEC_KEY)
          .doc(
              "The check interval of the Kerberos credential for Iceberg catalog with Kerberos authentication")
          .version(ConfigConstants.VERSION_0_6_0)
          .intConf()
          .checkValue(value -> value > 0, ConfigConstants.POSITIVE_NUMBER_ERROR_MSG)
          .createWithDefault(2);

  public static final ConfigEntry<Integer> FETCH_TIMEOUT_SEC_ENTRY =
      new ConfigBuilder(FETCH_TIMEOUT_SEC_KEY)
          .doc(
              "The fetch timeout of the Kerberos key table of Iceberg catalog with Kerberos authentication")
          .version(ConfigConstants.VERSION_0_6_0)
          .intConf()
          .checkValue(value -> value > 0, ConfigConstants.POSITIVE_NUMBER_ERROR_MSG)
          .createWithDefault(2);

  public KerberosConfig(Map<String, String> properties) {
    super(properties);
    loadFromMap(properties, k -> true);
  }

  public boolean isImpersonationEnabled() {
    return get(ENABLE_IMPERSONATION_ENTRY);
  }

  public String getPrincipalName() {
    return get(PRINCIPAL_ENTRY);
  }

  public String getKeytab() {
    return get(KEYTAB_ENTRY);
  }

  public int getCheckIntervalSec() {
    return get(CHECK_INTERVAL_SEC_ENTRY);
  }

  public int getFetchTimeoutSec() {
    return get(FETCH_TIMEOUT_SEC_ENTRY);
  }

  public static final Map<String, PropertyEntry<?>> KERBEROS_PROPERTY_ENTRIES =
      new ImmutableMap.Builder<String, PropertyEntry<?>>()
          .put(
              KET_TAB_URI_KEY,
              PropertyEntry.stringImmutablePropertyEntry(
                  KET_TAB_URI_KEY,
                  "The keytab of the Kerberos for Iceberg catalog with Kerberos authentication",
                  false,
                  null,
                  false,
                  false))
          .put(
              PRINCIPAL_KEY,
              PropertyEntry.stringImmutablePropertyEntry(
                  PRINCIPAL_KEY,
                  "The principal of the Kerberos for Iceberg catalog with Kerberos authentication",
                  false,
                  null,
                  false,
                  false))
          .put(
              CHECK_INTERVAL_SEC_KEY,
              PropertyEntry.integerOptionalPropertyEntry(
                  CHECK_INTERVAL_SEC_KEY,
                  "The check interval of the Kerberos credential for Iceberg catalog with Kerberos authentication",
                  true,
                  60,
                  false))
          .put(
              FETCH_TIMEOUT_SEC_KEY,
              PropertyEntry.integerOptionalPropertyEntry(
                  FETCH_TIMEOUT_SEC_KEY,
                  "The fetch timeout of the Kerberos key table of Iceberg catalog with Kerberos authentication",
                  true,
                  60,
                  false))
          .build();
}
