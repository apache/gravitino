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

package org.apache.gravitino.client;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.cfg.EnumFeature;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
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
            .configure(DeserializationFeature.READ_UNKNOWN_ENUM_VALUES_AS_NULL, true)
            .build()
            .registerModule(new JavaTimeModule())
            .registerModule(new Jdk8Module());
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
