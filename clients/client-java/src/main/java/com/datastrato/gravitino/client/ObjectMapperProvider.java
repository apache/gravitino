/*
 *  Copyright 2024 Datastrato Pvt Ltd.
 *  This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.client;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.cfg.EnumFeature;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

/**
 * Provides a singleton {@link ObjectMapper} configured for specific serialization and
 * deserialization behaviors.
 */
public class ObjectMapperProvider {
  private static class ObjectMapperHolder {
    private static final ObjectMapper INSTANCE =
        JsonMapper.builder()
            .configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false)
            .configure(EnumFeature.WRITE_ENUMS_TO_LOWERCASE, true)
            .enable(MapperFeature.ACCEPT_CASE_INSENSITIVE_ENUMS)
            .disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)
            .build()
            .registerModule(new JavaTimeModule());
  }

  /**
   * Retrieves a globally shared {@link ObjectMapper} instance.
   *
   * <p>Note: This ObjectMapper is a global singe instance. If you need to modify the default
   * serialization/deserialization settings, make changes within the INSTANCE builder directly.
   * Avoid modifying properties of the returned {@code ObjectMapper} instance to prevent unintended
   * side effects.
   *
   * @return the globally shared {@link ObjectMapper} instance
   */
  public static ObjectMapper objectMapper() {
    return ObjectMapperHolder.INSTANCE;
  }

  private ObjectMapperProvider() {}
}
