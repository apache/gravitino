/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.catalog.hadoop;

import com.datastrato.gravitino.catalog.hadoop.authentication.AuthenticationConfig;
import com.datastrato.gravitino.catalog.hadoop.authentication.kerberos.KerberosConfig;
import com.datastrato.gravitino.connector.BasePropertiesMetadata;
import com.datastrato.gravitino.connector.PropertyEntry;
import com.google.common.collect.ImmutableMap;
import java.util.Map;

public class HadoopFilesetPropertiesMetadata extends BasePropertiesMetadata {

  @Override
  protected Map<String, PropertyEntry<?>> specificPropertyEntries() {
    ImmutableMap.Builder<String, PropertyEntry<?>> builder = ImmutableMap.builder();
    builder.putAll(KerberosConfig.KERBEROS_PROPERTY_ENTRIES);
    builder.putAll(AuthenticationConfig.AUTHENTICATION_PROPERTY_ENTRIES);
    return builder.build();
  }
}
