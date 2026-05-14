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
package org.apache.gravitino.lance.service.rest;

import com.fasterxml.jackson.databind.ObjectMapper;
import javax.ws.rs.ext.ContextResolver;
import javax.ws.rs.ext.Provider;
import org.apache.gravitino.server.web.ObjectMapperProvider;
import org.openapitools.jackson.nullable.JsonNullableModule;

/**
 * JAX-RS {@link ContextResolver} that provides an {@link ObjectMapper} with the {@link
 * JsonNullableModule} registered.
 *
 * <p>lance-namespace 0.7.5 models use {@code JsonNullable<T>} for optional fields, which requires
 * this module for correct Jackson serialization/deserialization.
 */
@Provider
public class JsonNullableMapperProvider implements ContextResolver<ObjectMapper> {

  private static final ObjectMapper MAPPER =
      ObjectMapperProvider.objectMapper().copy().registerModule(new JsonNullableModule());

  @Override
  public ObjectMapper getContext(Class<?> type) {
    return MAPPER;
  }
}
