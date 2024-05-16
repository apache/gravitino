/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.server.web;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.cfg.EnumFeature;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import javax.ws.rs.ext.ContextResolver;
import javax.ws.rs.ext.Provider;

@Provider
public class ObjectMapperProvider implements ContextResolver<ObjectMapper> {

  private static class ObjectMapperHolder {
    private static final ObjectMapper INSTANCE =
        JsonMapper.builder()
            .configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false)
            .configure(EnumFeature.WRITE_ENUMS_TO_LOWERCASE, true)
            .enable(MapperFeature.ACCEPT_CASE_INSENSITIVE_ENUMS)
            .build()
            .setSerializationInclusion(JsonInclude.Include.NON_NULL)
            .registerModule(new JavaTimeModule());
  }

  /**
   * Retrieves a globally shared {@link ObjectMapper} instance.
   *
   * <p>Note: This ObjectMapper is a global single instance. If you need to modify the default
   * serialization/deserialization settings, make changes within the INSTANCE builder directly.
   * Avoid modifying properties of the returned {@code ObjectMapper} instance to prevent unintended
   * side effects.
   *
   * @return the globally shared {@link ObjectMapper} instance
   */
  public static ObjectMapper objectMapper() {
    return ObjectMapperHolder.INSTANCE;
  }

  @Override
  public ObjectMapper getContext(Class<?> type) {
    return ObjectMapperHolder.INSTANCE;
  }
}
