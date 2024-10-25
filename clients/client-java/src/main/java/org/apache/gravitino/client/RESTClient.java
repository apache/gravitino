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
package org.apache.gravitino.client;

import com.google.common.collect.ImmutableMap;
import java.io.Closeable;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Supplier;
import org.apache.gravitino.dto.responses.ErrorResponse;
import org.apache.gravitino.rest.RESTRequest;
import org.apache.gravitino.rest.RESTResponse;

/**
 * Interface for a basic HTTP Client for interfacing with the REST catalog.
 *
 * <p>Referred from core/src/main/java/org/apache/iceberg/rest/RESTClient.java
 */
public interface RESTClient extends Closeable {

  /**
   * Perform a HEAD request on the specified path with the given headers and error handling.
   *
   * @param path The path to be requested.
   * @param headers The headers to be included in the request.
   * @param errorHandler The consumer for handling error responses.
   */
  default void head(
      String path, Supplier<Map<String, String>> headers, Consumer<ErrorResponse> errorHandler) {
    head(path, headers.get(), errorHandler);
  }

  /**
   * Perform a HEAD request on the specified path with the given headers and error handling.
   *
   * @param path The path to be requested.
   * @param headers The headers to be included in the request.
   * @param errorHandler The consumer for handling error responses.
   */
  void head(String path, Map<String, String> headers, Consumer<ErrorResponse> errorHandler);

  /**
   * Perform a DELETE request on the specified path with given information.
   *
   * @param path The path to be requested.
   * @param queryParams The query parameters to be included in the request.
   * @param responseType The class representing the type of the response.
   * @param headers The headers to be included in the request.
   * @param errorHandler The consumer for handling error responses.
   * @param <T> The type of the response.
   * @return The response of the DELETE request.
   */
  default <T extends RESTResponse> T delete(
      String path,
      Map<String, String> queryParams,
      Class<T> responseType,
      Supplier<Map<String, String>> headers,
      Consumer<ErrorResponse> errorHandler) {
    return delete(path, queryParams, responseType, headers.get(), errorHandler);
  }

  /**
   * Perform a DELETE request on the specified path with given information and no query parameters.
   *
   * @param path The path to be requested.
   * @param responseType The class representing the type of the response.
   * @param headers The headers to be included in the request.
   * @param errorHandler The consumer for handling error responses.
   * @param <T> The type of the response.
   * @return The response of the DELETE request.
   */
  default <T extends RESTResponse> T delete(
      String path,
      Class<T> responseType,
      Supplier<Map<String, String>> headers,
      Consumer<ErrorResponse> errorHandler) {
    return delete(path, ImmutableMap.of(), responseType, headers.get(), errorHandler);
  }

  /**
   * Perform a DELETE request on the specified path with given information.
   *
   * @param path The path to be requested.
   * @param responseType The class representing the type of the response.
   * @param headers The headers to be included in the request.
   * @param errorHandler The consumer for handling error responses.
   * @param <T> The type of the response.
   * @return The response of the DELETE request.
   */
  default <T extends RESTResponse> T delete(
      String path,
      Class<T> responseType,
      Map<String, String> headers,
      Consumer<ErrorResponse> errorHandler) {
    return delete(path, ImmutableMap.of(), responseType, headers, errorHandler);
  }

  /**
   * Perform a DELETE request on the specified path with given information.
   *
   * @param path The path to be requested.
   * @param queryParams The query parameters to be included in the request (ignored).
   * @param responseType The class representing the type of the response.
   * @param headers The headers to be included in the request.
   * @param errorHandler The consumer for handling error responses.
   * @param <T> The type of the response.
   * @return The response of the DELETE request.
   */
  <T extends RESTResponse> T delete(
      String path,
      Map<String, String> queryParams,
      Class<T> responseType,
      Map<String, String> headers,
      Consumer<ErrorResponse> errorHandler);

  /**
   * Perform a GET request on the specified path with given information and no query parameters.
   *
   * @param path The path to be requested.
   * @param responseType The class representing the type of the response.
   * @param headers The headers to be included in the request.
   * @param errorHandler The consumer for handling error responses.
   * @param <T> The type of the response.
   * @return The response of the GET request.
   */
  default <T extends RESTResponse> T get(
      String path,
      Class<T> responseType,
      Supplier<Map<String, String>> headers,
      Consumer<ErrorResponse> errorHandler) {
    return get(path, ImmutableMap.of(), responseType, headers, errorHandler);
  }

  /**
   * Perform a GET request on the specified path with given information and no query parameters.
   *
   * @param path The path to be requested.
   * @param responseType The class representing the type of the response.
   * @param headers The headers to be included in the request.
   * @param errorHandler The consumer for handling error responses.
   * @param <T> The type of the response.
   * @return The response of the GET request.
   */
  default <T extends RESTResponse> T get(
      String path,
      Class<T> responseType,
      Map<String, String> headers,
      Consumer<ErrorResponse> errorHandler) {
    return get(path, ImmutableMap.of(), responseType, headers, errorHandler);
  }

  /**
   * Perform a GET request on the specified path with given information.
   *
   * @param path The path to be requested.
   * @param queryParams The query parameters to be included in the request.
   * @param responseType The class representing the type of the response.
   * @param headers The headers to be included in the request.
   * @param errorHandler The consumer for handling error responses.
   * @param <T> The type of the response.
   * @return The response of the GET request.
   */
  default <T extends RESTResponse> T get(
      String path,
      Map<String, String> queryParams,
      Class<T> responseType,
      Supplier<Map<String, String>> headers,
      Consumer<ErrorResponse> errorHandler) {
    return get(path, queryParams, responseType, headers.get(), errorHandler);
  }

  /**
   * Perform a GET request on the specified path with given information.
   *
   * @param path The path to be requested.
   * @param queryParams The query parameters to be included in the request.
   * @param responseType The class representing the type of the response.
   * @param headers The headers to be included in the request.
   * @param errorHandler The consumer for handling error responses.
   * @param <T> The type of the response.
   * @return The response of the GET request.
   */
  <T extends RESTResponse> T get(
      String path,
      Map<String, String> queryParams,
      Class<T> responseType,
      Map<String, String> headers,
      Consumer<ErrorResponse> errorHandler);

  /**
   * Perform a POST request on the specified path with given information.
   *
   * @param path The path to be requested.
   * @param body The request body to be included in the POST request.
   * @param responseType The class representing the type of the response.
   * @param headers The headers to be included in the request.
   * @param errorHandler The consumer for handling error responses.
   * @param <T> The type of the response.
   * @return The response of the POST request.
   */
  default <T extends RESTResponse> T post(
      String path,
      RESTRequest body,
      Class<T> responseType,
      Supplier<Map<String, String>> headers,
      Consumer<ErrorResponse> errorHandler) {
    return post(path, body, responseType, headers.get(), errorHandler);
  }

  /**
   * Perform a POST request on the specified path with a given information.
   *
   * @param path The path to be requested.
   * @param body The request body to be included in the POST request.
   * @param responseType The class representing the type of the response.
   * @param headers The headers to be included in the request.
   * @param errorHandler The consumer for handling error responses.
   * @param responseHeaders The consumer for handling response headers (unsupported in this method).
   * @param <T> The type of the response.
   * @return The response of the POST request.
   */
  default <T extends RESTResponse> T post(
      String path,
      RESTRequest body,
      Class<T> responseType,
      Supplier<Map<String, String>> headers,
      Consumer<ErrorResponse> errorHandler,
      Consumer<Map<String, String>> responseHeaders) {
    return post(path, body, responseType, headers.get(), errorHandler, responseHeaders);
  }

  /**
   * Perform a POST request on the specified path with a given information.
   *
   * @param path The path to be requested.
   * @param body The request body to be included in the POST request.
   * @param responseType The class representing the type of the response.
   * @param headers The headers to be included in the request.
   * @param errorHandler The consumer for handling error responses.
   * @param responseHeaders The consumer for handling response headers (unsupported in this method).
   * @param <T> The type of the response.
   * @return The response of the POST request.
   * @throws UnsupportedOperationException If trying to handle response headers (unsupported).
   */
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

  /**
   * Perform a POST request on the specified path with given information.
   *
   * @param path The path to be requested.
   * @param body The request body to be included in the POST request.
   * @param responseType The class representing the type of the response.
   * @param headers The headers to be included in the request.
   * @param errorHandler The consumer for handling error responses.
   * @return The response of the POST request.
   * @param <T> The type of the response.
   */
  <T extends RESTResponse> T post(
      String path,
      RESTRequest body,
      Class<T> responseType,
      Map<String, String> headers,
      Consumer<ErrorResponse> errorHandler);

  /**
   * Perform a PUT request on the specified path with given information.
   *
   * @param path The path to be requested.
   * @param body The request body to be included in the PUT request.
   * @param responseType The class representing the type of the response.
   * @param headers The headers to be included in the request.
   * @param errorHandler The consumer for handling error responses.
   * @param <T> The type of the response.
   * @return The response of the PUT request.
   */
  default <T extends RESTResponse> T put(
      String path,
      RESTRequest body,
      Class<T> responseType,
      Supplier<Map<String, String>> headers,
      Consumer<ErrorResponse> errorHandler) {
    return put(path, body, responseType, headers.get(), errorHandler);
  }

  /**
   * Perform a PUT request on the specified path with given information.
   *
   * @param path The path to be requested.
   * @param body The request body to be included in the PUT request.
   * @param responseType The class representing the type of the response.
   * @param headers The supplier for providing headers to be included in the request.
   * @param errorHandler The consumer for handling error responses.
   * @param responseHeaders The consumer for handling response headers.
   * @param <T> The type of the response.
   * @return The response of the PUT request.
   */
  default <T extends RESTResponse> T put(
      String path,
      RESTRequest body,
      Class<T> responseType,
      Supplier<Map<String, String>> headers,
      Consumer<ErrorResponse> errorHandler,
      Consumer<Map<String, String>> responseHeaders) {
    return put(path, body, responseType, headers.get(), errorHandler, responseHeaders);
  }

  /**
   * Perform a PUT request on the specified path with a request body, response type, headers, and
   * error handling.
   *
   * @param path The path to be requested.
   * @param body The request body to be included in the PUT request.
   * @param responseType The class representing the type of the response.
   * @param headers The headers to be included in the request.
   * @param errorHandler The consumer for handling error responses.
   * @param responseHeaders This parameter is not supported and should be set to null.
   * @param <T> The type of the response.
   * @return The response of the PUT request.
   * @throws UnsupportedOperationException if {@code responseHeaders} is not null, as returning
   *     response headers is not supported.
   */
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

  /**
   * Perform a PUT request on the specified path with given information.
   *
   * @param path The path to be requested.
   * @param body The request body to be included in the PUT request.
   * @param responseType The class representing the type of the response.
   * @param headers The headers to be included in the request.
   * @param errorHandler The consumer for handling error responses.
   * @return The response of the PUT request.
   * @param <T> The type of the response.
   */
  <T extends RESTResponse> T put(
      String path,
      RESTRequest body,
      Class<T> responseType,
      Map<String, String> headers,
      Consumer<ErrorResponse> errorHandler);

  /**
   * Perform a PATCH request on the specified path with given information.
   *
   * @param path The path to be requested.
   * @param body The request body to be included in the PATCH request.
   * @param responseType The class representing the type of the response.
   * @param headers The headers to be included in the request.
   * @param errorHandler The consumer for handling error responses.
   * @return The response of the PATCH request.
   * @param <T> The type of the response.
   */
  <T extends RESTResponse> T patch(
      String path,
      RESTRequest body,
      Class<T> responseType,
      Map<String, String> headers,
      Consumer<ErrorResponse> errorHandler);

  /**
   * Perform a POST request with form data on the specified path with the given information.
   *
   * @param path The path to be requested.
   * @param formData The form data to be included in the POST request body.
   * @param responseType The class representing the type of the response.
   * @param headers The supplier for providing headers to be included in the request.
   * @param errorHandler The consumer for handling error responses.
   * @param <T> The type of the response.
   * @return The response of the POST request.
   */
  default <T extends RESTResponse> T postForm(
      String path,
      Map<String, String> formData,
      Class<T> responseType,
      Supplier<Map<String, String>> headers,
      Consumer<ErrorResponse> errorHandler) {
    return postForm(path, formData, responseType, headers.get(), errorHandler);
  }

  /**
   * Perform a POST request with form data on the specified path with the given information.
   *
   * @param path The path to be requested.
   * @param formData The form data to be included in the POST request body.
   * @param responseType The class representing the type of the response.
   * @param headers The headers to be included in the request.
   * @param errorHandler The consumer for handling error responses.
   * @return The response of the POST request.
   * @param <T> The type of the response.
   */
  <T extends RESTResponse> T postForm(
      String path,
      Map<String, String> formData,
      Class<T> responseType,
      Map<String, String> headers,
      Consumer<ErrorResponse> errorHandler);
}
