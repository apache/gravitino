/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.catalog.jdbc;

import com.datastrato.gravitino.StringIdentifier;
import com.datastrato.gravitino.connector.BasePropertiesMetadata;
import com.google.common.collect.Maps;
import java.util.HashMap;
import java.util.Map;

public abstract class JdbcTablePropertiesMetadata extends BasePropertiesMetadata {

  public static final String COMMENT_KEY = "comment";

  protected Map<String, String> transformToJdbcProperties(Map<String, String> properties) {
    HashMap<String, String> resultProperties = Maps.newHashMap(properties);
    resultProperties.remove(StringIdentifier.ID_KEY);
    return resultProperties;
  }

  protected Map<String, String> convertFromJdbcProperties(Map<String, String> properties) {
    return properties;
  }
}
