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
package org.apache.gravitino.server.web;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.cfg.EnumFeature;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import javax.ws.rs.ext.ContextResolver;
import javax.ws.rs.ext.Provider;
import org.apache.gravitino.dto.responses.ErrorResponse;

@Provider
public class ObjectMapperProvider implements ContextResolver<ObjectMapper> {

  // Keep diagnostic stacks inside the server and accept legacy payloads while allowing operators
  // to omit them from responses.
  @JsonIgnoreProperties(value = "stack", allowSetters = true)
  private abstract static class ErrorResponseMixin {}

  private static class ObjectMapperHolder {
    private static final ObjectMapper WITHOUT_ERROR_STACK_TRACE = createObjectMapper(false);
    private static final ObjectMapper WITH_ERROR_STACK_TRACE = createObjectMapper(true);
  }

  private final ObjectMapper objectMapper;

  /**
   * Creates a provider using the backward-compatible server default, which includes error stack
   * traces.
   */
  public ObjectMapperProvider() {
    this.objectMapper = objectMapper();
  }

  /**
   * Creates a provider with explicit error stack-trace serialization behavior.
   *
   * @param includeErrorStackTrace whether HTTP error responses should include diagnostic stack
   *     traces
   */
  public ObjectMapperProvider(boolean includeErrorStackTrace) {
    this.objectMapper = objectMapper(includeErrorStackTrace);
  }

  /**
   * Retrieves the shared {@link ObjectMapper} using the backward-compatible server default.
   *
   * <p>Do not modify the returned mapper. Use {@link #objectMapper(boolean)} to select explicit
   * error stack-trace behavior.
   *
   * @return the globally shared {@link ObjectMapper} instance
   */
  public static ObjectMapper objectMapper() {
    return objectMapper(JettyServerConfig.INCLUDE_ERROR_STACK_TRACE.getDefaultValue());
  }

  @Override
  public ObjectMapper getContext(Class<?> type) {
    return objectMapper;
  }

  /**
   * Retrieves a shared, preconfigured mapper with explicit error stack-trace serialization
   * behavior.
   *
   * <p>Do not modify the returned mapper.
   *
   * @param includeErrorStackTrace whether HTTP error responses should include diagnostic stack
   *     traces
   * @return a shared {@link ObjectMapper} with the requested behavior
   */
  public static ObjectMapper objectMapper(boolean includeErrorStackTrace) {
    return includeErrorStackTrace
        ? ObjectMapperHolder.WITH_ERROR_STACK_TRACE
        : ObjectMapperHolder.WITHOUT_ERROR_STACK_TRACE;
  }

  private static ObjectMapper createObjectMapper(boolean includeErrorStackTrace) {
    JsonMapper.Builder builder =
        JsonMapper.builder()
            .configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false)
            .configure(EnumFeature.WRITE_ENUMS_TO_LOWERCASE, true)
            .enable(MapperFeature.ACCEPT_CASE_INSENSITIVE_ENUMS);
    if (!includeErrorStackTrace) {
      builder.addMixIn(ErrorResponse.class, ErrorResponseMixin.class);
    }

    return builder
        .build()
        .setSerializationInclusion(JsonInclude.Include.NON_NULL)
        .registerModule(new JavaTimeModule())
        .registerModule(new Jdk8Module());
  }
}
