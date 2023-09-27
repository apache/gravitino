/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.graviton.catalog.lakehouse.iceberg.web;

import com.datastrato.graviton.json.JsonUtils;
import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.PropertyAccessor;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.PropertyNamingStrategy;
import javax.ws.rs.ext.ContextResolver;
import javax.ws.rs.ext.Provider;
import org.apache.iceberg.rest.RESTSerializers;

@Provider
public class IcebergObjectMapperProvider implements ContextResolver<ObjectMapper> {

  @Override
  public ObjectMapper getContext(Class<?> type) {
    ObjectMapper mapper = JsonUtils.objectMapper();
    mapper.setVisibility(PropertyAccessor.FIELD, JsonAutoDetect.Visibility.ANY);
    mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    mapper.setPropertyNamingStrategy(new PropertyNamingStrategy.KebabCaseStrategy());
    RESTSerializers.registerAll(mapper);
    return mapper;
  }
}
