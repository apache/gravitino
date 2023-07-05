/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package com.datastrato.graviton.client;

import com.datastrato.graviton.dto.responses.ErrorResponse;
import com.datastrato.graviton.rest.RESTRequest;
import com.datastrato.graviton.rest.RESTResponse;
import com.google.common.collect.ImmutableMap;
import java.io.Closeable;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Supplier;

/**
 * Interface for a basic HTTP Client for interfacing with the REST catalog.
 *
 * <p>Referred from core/src/main/java/org/apache/iceberg/rest/RESTClient.java
 */
public interface RESTClient extends Closeable {

  default void head(
      String path, Supplier<Map<String, String>> headers, Consumer<ErrorResponse> errorHandler) {
    head(path, headers.get(), errorHandler);
  }

  void head(String path, Map<String, String> headers, Consumer<ErrorResponse> errorHandler);

  default <T extends RESTResponse> T delete(
      String path,
      Map<String, String> queryParams,
      Class<T> responseType,
      Supplier<Map<String, String>> headers,
      Consumer<ErrorResponse> errorHandler) {
    return delete(path, queryParams, responseType, headers.get(), errorHandler);
  }

  default <T extends RESTResponse> T delete(
      String path,
      Class<T> responseType,
      Supplier<Map<String, String>> headers,
      Consumer<ErrorResponse> errorHandler) {
    return delete(path, ImmutableMap.of(), responseType, headers.get(), errorHandler);
  }

  <T extends RESTResponse> T delete(
      String path,
      Class<T> responseType,
      Map<String, String> headers,
      Consumer<ErrorResponse> errorHandler);

  default <T extends RESTResponse> T delete(
      String path,
      Map<String, String> queryParams,
      Class<T> responseType,
      Map<String, String> headers,
      Consumer<ErrorResponse> errorHandler) {
    if (null != queryParams && !queryParams.isEmpty()) {
      throw new UnsupportedOperationException("Query params are not supported");
    }

    return delete(path, responseType, headers, errorHandler);
  }

  default <T extends RESTResponse> T get(
      String path,
      Class<T> responseType,
      Supplier<Map<String, String>> headers,
      Consumer<ErrorResponse> errorHandler) {
    return get(path, ImmutableMap.of(), responseType, headers, errorHandler);
  }

  default <T extends RESTResponse> T get(
      String path,
      Class<T> responseType,
      Map<String, String> headers,
      Consumer<ErrorResponse> errorHandler) {
    return get(path, ImmutableMap.of(), responseType, headers, errorHandler);
  }

  default <T extends RESTResponse> T get(
      String path,
      Map<String, String> queryParams,
      Class<T> responseType,
      Supplier<Map<String, String>> headers,
      Consumer<ErrorResponse> errorHandler) {
    return get(path, queryParams, responseType, headers.get(), errorHandler);
  }

  <T extends RESTResponse> T get(
      String path,
      Map<String, String> queryParams,
      Class<T> responseType,
      Map<String, String> headers,
      Consumer<ErrorResponse> errorHandler);

  default <T extends RESTResponse> T post(
      String path,
      RESTRequest body,
      Class<T> responseType,
      Supplier<Map<String, String>> headers,
      Consumer<ErrorResponse> errorHandler) {
    return post(path, body, responseType, headers.get(), errorHandler);
  }

  default <T extends RESTResponse> T post(
      String path,
      RESTRequest body,
      Class<T> responseType,
      Supplier<Map<String, String>> headers,
      Consumer<ErrorResponse> errorHandler,
      Consumer<Map<String, String>> responseHeaders) {
    return post(path, body, responseType, headers.get(), errorHandler, responseHeaders);
  }

  default <T extends RESTResponse> T post(
      String path,
      RESTRequest body,
      Class<T> responseType,
      Map<String, String> headers,
      Consumer<ErrorResponse> errorHandler,
      Consumer<Map<String, String>> responseHeaders) {
    if (null != responseHeaders) {
      throw new UnsupportedOperationException("Returning response headers is not supported");
    }

    return post(path, body, responseType, headers, errorHandler);
  }

  <T extends RESTResponse> T post(
      String path,
      RESTRequest body,
      Class<T> responseType,
      Map<String, String> headers,
      Consumer<ErrorResponse> errorHandler);

  default <T extends RESTResponse> T put(
      String path,
      RESTRequest body,
      Class<T> responseType,
      Supplier<Map<String, String>> headers,
      Consumer<ErrorResponse> errorHandler) {
    return put(path, body, responseType, headers.get(), errorHandler);
  }

  default <T extends RESTResponse> T put(
      String path,
      RESTRequest body,
      Class<T> responseType,
      Supplier<Map<String, String>> headers,
      Consumer<ErrorResponse> errorHandler,
      Consumer<Map<String, String>> responseHeaders) {
    return put(path, body, responseType, headers.get(), errorHandler, responseHeaders);
  }

  default <T extends RESTResponse> T put(
      String path,
      RESTRequest body,
      Class<T> responseType,
      Map<String, String> headers,
      Consumer<ErrorResponse> errorHandler,
      Consumer<Map<String, String>> responseHeaders) {
    if (null != responseHeaders) {
      throw new UnsupportedOperationException("Returning response headers is not supported");
    }

    return put(path, body, responseType, headers, errorHandler);
  }

  <T extends RESTResponse> T put(
      String path,
      RESTRequest body,
      Class<T> responseType,
      Map<String, String> headers,
      Consumer<ErrorResponse> errorHandler);

  default <T extends RESTResponse> T postForm(
      String path,
      Map<String, String> formData,
      Class<T> responseType,
      Supplier<Map<String, String>> headers,
      Consumer<ErrorResponse> errorHandler) {
    return postForm(path, formData, responseType, headers.get(), errorHandler);
  }

  <T extends RESTResponse> T postForm(
      String path,
      Map<String, String> formData,
      Class<T> responseType,
      Map<String, String> headers,
      Consumer<ErrorResponse> errorHandler);
}
