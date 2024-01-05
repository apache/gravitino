/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.catalog.lakehouse.iceberg.web;

import com.fasterxml.jackson.databind.ObjectMapper;
import javax.ws.rs.ext.ContextResolver;
import javax.ws.rs.ext.Provider;

@Provider
public class IcebergObjectMapperProvider implements ContextResolver<ObjectMapper> {
  @Override
  public ObjectMapper getContext(Class<?> type) {
    return IcebergObjectMapper.getInstance();
  }
}
