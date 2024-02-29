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

package com.datastrato.gravitino.client;

import com.datastrato.gravitino.auth.AuthConstants;
import com.datastrato.gravitino.dto.responses.ErrorResponse;
import com.datastrato.gravitino.exceptions.RESTException;
import com.datastrato.gravitino.json.JsonUtils;
import com.datastrato.gravitino.rest.RESTRequest;
import com.datastrato.gravitino.rest.RESTResponse;
import com.datastrato.gravitino.rest.RESTUtils;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import org.apache.hc.client5.http.classic.methods.HttpUriRequest;
import org.apache.hc.client5.http.classic.methods.HttpUriRequestBase;
import org.apache.hc.client5.http.impl.classic.CloseableHttpClient;
import org.apache.hc.client5.http.impl.classic.CloseableHttpResponse;
import org.apache.hc.client5.http.impl.classic.HttpClientBuilder;
import org.apache.hc.client5.http.impl.classic.HttpClients;
import org.apache.hc.core5.http.ContentType;
import org.apache.hc.core5.http.Header;
import org.apache.hc.core5.http.HttpHeaders;
import org.apache.hc.core5.http.HttpStatus;
import org.apache.hc.core5.http.Method;
import org.apache.hc.core5.http.ParseException;
import org.apache.hc.core5.http.impl.EnglishReasonPhraseCatalog;
import org.apache.hc.core5.http.io.entity.EntityUtils;
import org.apache.hc.core5.http.io.entity.StringEntity;
import org.apache.hc.core5.http.message.BasicHeader;
import org.apache.hc.core5.io.CloseMode;
import org.apache.hc.core5.net.URIBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An HttpClient for usage with the REST catalog.
 *
 * <p>This class provides functionality for making HTTP requests to a REST API and processing the
 * corresponding responses. It supports common HTTP methods like GET, POST, PUT, DELETE, and HEAD.
 * Additionally, it allows handling server error responses using a custom error handler.
 *
 * <p>Referred from core/src/main/java/org/apache/iceberg/rest/HTTPClient.java
 */
public class HTTPClient implements RESTClient {

  private static final Logger LOG = LoggerFactory.getLogger(HTTPClient.class);

  private static final String VERSION_HEADER = "application/vnd.gravitino.v1+json";

  private final String uri;
  private final CloseableHttpClient httpClient;
  private final ObjectMapper mapper;
  private final AuthDataProvider authDataProvider;

  /**
   * Constructs an instance of HTTPClient with the provided information.
   *
   * @param uri The base URI of the REST API.
   * @param baseHeaders A map of base headers to be included in all HTTP requests.
   * @param objectMapper The ObjectMapper used for JSON serialization and deserialization.
   * @param authDataProvider The provider of authentication data.
   */
  private HTTPClient(
      String uri,
      Map<String, String> baseHeaders,
      ObjectMapper objectMapper,
      AuthDataProvider authDataProvider) {
    this.uri = uri;
    this.mapper = objectMapper;

    HttpClientBuilder clientBuilder = HttpClients.custom();

    if (baseHeaders != null) {
      clientBuilder.setDefaultHeaders(
          baseHeaders.entrySet().stream()
              .map(e -> new BasicHeader(e.getKey(), e.getValue()))
              .collect(Collectors.toList()));
    }

    this.httpClient = clientBuilder.build();
    this.authDataProvider = authDataProvider;
  }

  /**
   * Extracts the response body as a string from the provided HTTP response.
   *
   * @param response The HTTP response from which the response body will be extracted.
   * @return The response body as a string.
   * @throws RESTException If an error occurs during conversion of the response body to a string.
   */
  private String extractResponseBodyAsString(CloseableHttpResponse response) {
    try {
      if (response.getEntity() == null) {
        return null;
      }

      // EntityUtils.toString returns null when HttpEntity.getContent returns null.
      return EntityUtils.toString(response.getEntity(), "UTF-8");
    } catch (IOException | ParseException e) {
      throw new RESTException(e, "Failed to convert HTTP response body to string");
    }
  }

  /**
   * Checks if the response indicates a successful response.
   *
   * <p>According to the spec, the only currently defined/used "success" responses are 200 and 202.
   *
   * @param response The response to check for success.
   * @return True if the response is successful, false otherwise.
   */
  private boolean isSuccessful(CloseableHttpResponse response) {
    int code = response.getCode();
    return code == HttpStatus.SC_OK
        || code == HttpStatus.SC_ACCEPTED
        || code == HttpStatus.SC_NO_CONTENT;
  }

  /**
   * Builds an error response based on the provided HTTP response.
   *
   * <p>This method extracts the reason phrase from the response and uses it as the message for the
   * ErrorResponse. If the reason phrase doesn't exist, it retrieves the standard reason phrase from
   * the English phrase catalog.
   *
   * @param response The response from which the ErrorResponse is built.
   * @return An ErrorResponse object representing the REST error response.
   */
  private ErrorResponse buildRestErrorResponse(CloseableHttpResponse response) {
    String responseReason = response.getReasonPhrase();
    String message =
        responseReason != null && !responseReason.isEmpty()
            ? responseReason
            : EnglishReasonPhraseCatalog.INSTANCE.getReason(response.getCode(), null /* ignored */);
    return ErrorResponse.restError(message);
  }

  /**
   * Processes a failed response through the provided error.
   *
   * <p>This method takes a response representing a failed response from an HTTP request. It tries
   * to parse the response body using the provided parseResponse method.
   *
   * @param response The failed response from the HTTP request.
   * @param responseBody The response body as a string (can be null).
   * @param errorHandler The error handler (as a Consumer) used to handle the error response.
   * @throws RESTException If the error handler does not throw an exception or an error occurs
   *     during parsing.
   */
  private void throwFailure(
      CloseableHttpResponse response, String responseBody, Consumer<ErrorResponse> errorHandler) {
    ErrorResponse errorResponse = null;

    if (responseBody != null) {
      try {
        if (errorHandler instanceof ErrorHandler) {
          errorResponse =
              ((ErrorHandler) errorHandler).parseResponse(response.getCode(), responseBody, mapper);
        } else {
          LOG.warn(
              "Unknown error handler {}, response body won't be parsed",
              errorHandler.getClass().getName());
          errorResponse =
              ErrorResponse.unknownError(
                  String.format(
                      "Unknown error handler %s, response body won't be parsed %s",
                      errorHandler.getClass().getName(), responseBody));
        }

      } catch (UncheckedIOException | IllegalArgumentException e) {
        // It's possible to receive a non-successful response that isn't a properly defined
        // BaseResponse due to various reasons, such as server misconfiguration or
        // unanticipated external factors.
        // In such cases, we handle the situation by building an error response for the user.
        // Examples of such scenarios include network timeouts or load balancers returning default
        // 5xx responses.
        LOG.error("Failed to parse an error response. Will create one instead.", e);
      }
    }

    if (errorResponse == null) {
      errorResponse = buildRestErrorResponse(response);
    }

    errorHandler.accept(errorResponse);

    // Throw an exception in case the provided error handler does not throw.
    throw new RESTException("Unhandled error: %s", errorResponse);
  }

  /**
   * Builds a URI for the HTTP request using the given path and query parameters.
   *
   * <p>This method constructs a URI by combining the base URI (stored in the "uri" field) with the
   * provided path. If query parameters are provided in the "params" map, they are added to the URI,
   * ensuring proper encoding of query parameters and that the URI is well-formed.
   *
   * @param path The URL path to append to the base URI.
   * @param params A map of query parameters (key-value pairs) to include in the URI (can be null).
   * @return The constructed URI for the HTTP request.
   * @throws RESTException If there is an issue building the URI from the base URI and query
   *     parameters.
   */
  private URI buildUri(String path, Map<String, String> params) {
    String baseUri = String.format("%s/%s", uri, path);
    try {
      URIBuilder builder = new URIBuilder(baseUri);
      if (params != null) {
        params.forEach(builder::addParameter);
      }
      return builder.build();
    } catch (URISyntaxException e) {
      throw new RESTException(
          "Failed to create request URI from base %s, params %s", baseUri, params);
    }
  }

  /**
   * Executes an HTTP request and processes the corresponding response.
   *
   * <p>This method is a helper function to execute HTTP requests.
   *
   * @param method The HTTP method to use (e.g., GET, POST, PUT, DELETE).
   * @param path The URL path to send the request to.
   * @param queryParams A map of query parameters (key-value pairs) to include in the request URL
   *     (can be null).
   * @param requestBody The content to place in the request body (can be null).
   * @param responseType The class type of the response for deserialization (Must be registered with
   *     the ObjectMapper).
   * @param headers A map of request headers (key-value pairs) to include in the request (can be
   *     null).
   * @param errorHandler The error handler delegated for HTTP responses, which handles server error
   *     responses.
   * @param <T> The class type of the response for deserialization. (Must be registered with the
   *     ObjectMapper).
   * @return The response entity parsed and converted to its type T.
   */
  private <T> T execute(
      Method method,
      String path,
      Map<String, String> queryParams,
      Object requestBody,
      Class<T> responseType,
      Map<String, String> headers,
      Consumer<ErrorResponse> errorHandler) {
    return execute(
        method, path, queryParams, requestBody, responseType, headers, errorHandler, h -> {});
  }

  /**
   * Executes an HTTP request and processes the corresponding response with support for response
   * headers.
   *
   * <p>The method constructs the HTTP request using the provided parameters and sends it to the
   * server. It then processes the server's response, handling successful responses and server error
   * responses accordingly.
   *
   * <p>Response headers from the server are extracted and passed to the responseHeaders Consumer
   * for further processing by the caller.
   *
   * @param method The HTTP method to use (e.g., GET, POST, PUT, DELETE).
   * @param path The URL path to send the request to.
   * @param queryParams A map of query parameters (key-value pairs) to include in the request URL
   *     (can be null).
   * @param requestBody The content to place in the request body (can be null).
   * @param responseType The class type of the response for deserialization (Must be registered with
   *     the ObjectMapper).
   * @param headers A map of request headers (key-value pairs) to include in the request (can be
   *     null).
   * @param errorHandler The error handler delegated for HTTP responses, which handles server error
   *     responses.
   * @param responseHeaders The consumer of the response headers for further processing.
   * @param <T> The class type of the response for deserialization. (Must be registered with the
   *     ObjectMapper).
   * @return The response entity parsed and converted to its type T.
   * @throws RESTException If the provided path is malformed, if there is an issue with the HTTP
   *     request or response processing, or if the errorHandler does not throw an exception for
   *     server error responses.
   */
  @SuppressWarnings("deprecation")
  private <T> T execute(
      Method method,
      String path,
      Map<String, String> queryParams,
      Object requestBody,
      Class<T> responseType,
      Map<String, String> headers,
      Consumer<ErrorResponse> errorHandler,
      Consumer<Map<String, String>> responseHeaders) {
    if (path.startsWith("/")) {
      throw new RESTException(
          "Received a malformed path for a REST request: %s. Paths should not start with /", path);
    }

    HttpUriRequestBase request = new HttpUriRequestBase(method.name(), buildUri(path, queryParams));

    if (requestBody instanceof Map) {
      // encode maps as form data, application/x-www-form-urlencoded
      addRequestHeaders(request, headers, ContentType.APPLICATION_FORM_URLENCODED.getMimeType());
      request.setEntity(toFormEncoding((Map<?, ?>) requestBody));
    } else if (requestBody != null) {
      // other request bodies are serialized as JSON, application/json
      addRequestHeaders(request, headers, ContentType.APPLICATION_JSON.getMimeType());
      request.setEntity(toJson(requestBody));
    } else {
      addRequestHeaders(request, headers, ContentType.APPLICATION_JSON.getMimeType());
    }
    if (authDataProvider != null) {
      request.setHeader(
          AuthConstants.HTTP_HEADER_AUTHORIZATION,
          new String(authDataProvider.getTokenData(), StandardCharsets.UTF_8));
    }

    try (CloseableHttpResponse response = httpClient.execute(request)) {
      Map<String, String> respHeaders = Maps.newHashMap();
      for (Header header : response.getHeaders()) {
        respHeaders.put(header.getName(), header.getValue());
      }

      responseHeaders.accept(respHeaders);

      // Skip parsing the response stream for any successful request not expecting a response body
      if (response.getCode() == HttpStatus.SC_NO_CONTENT
          || (responseType == null && isSuccessful(response))) {
        return null;
      }

      String responseBody = extractResponseBodyAsString(response);

      if (!isSuccessful(response)) {
        // The provided error handler is expected to throw, but a RESTException.java is thrown if
        // not.
        throwFailure(response, responseBody, errorHandler);
      }

      if (responseBody == null) {
        throw new RESTException(
            "Invalid (null) response body for request (expected %s): method=%s, path=%s, status=%d",
            responseType != null ? responseType.getSimpleName() : "unknown",
            method.name(),
            path,
            response.getCode());
      }

      try {
        return mapper.readValue(responseBody, responseType);
      } catch (JsonProcessingException e) {
        throw new RESTException(
            e,
            "Received a success response code of %d, but failed to parse response body into %s",
            response.getCode(),
            responseType != null ? responseType.getSimpleName() : "unknown");
      }
    } catch (IOException e) {
      throw new RESTException(e, "Error occurred while processing %s request", method);
    }
  }

  /**
   * Sends an HTTP HEAD request to the specified path and processes the response.
   *
   * @param path The URL path to send the HEAD request to.
   * @param headers A map of request headers (key-value pairs) to include in the request (can be
   *     null).
   * @param errorHandler The error handler delegated for HTTP responses, which handles server error
   *     responses.
   */
  @Override
  public void head(String path, Map<String, String> headers, Consumer<ErrorResponse> errorHandler) {
    execute(Method.HEAD, path, null, null, null, headers, errorHandler);
  }

  /**
   * Sends an HTTP GET request to the specified path and processes the response.
   *
   * @param path The URL path to send the GET request to.
   * @param queryParams A map of query parameters (key-value pairs) to include in the request URL
   *     (can be null).
   * @param responseType The class type of the response for deserialization (Must be registered with
   *     the ObjectMapper).
   * @param headers A map of request headers (key-value pairs) to include in the request (can be
   *     null).
   * @param errorHandler The error handler delegated for HTTP responses, which handles server error
   *     responses.
   * @param <T> The class type of the response for deserialization.
   * @return The response entity parsed and converted to its type T.
   */
  @Override
  public <T extends RESTResponse> T get(
      String path,
      Map<String, String> queryParams,
      Class<T> responseType,
      Map<String, String> headers,
      Consumer<ErrorResponse> errorHandler) {
    return execute(Method.GET, path, queryParams, null, responseType, headers, errorHandler);
  }

  /**
   * Sends an HTTP POST request to the specified path with the provided request body and processes
   * the response.
   *
   * @param path The URL path to send the POST request to.
   * @param body The REST body to place in the request body.
   * @param responseType The class type of the response for deserialization (Must be registered with
   *     the ObjectMapper).
   * @param headers A map of request headers (key-value pairs) to include in the request (can be
   *     null).
   * @param errorHandler The error handler delegated for HTTP responses, which handles server error
   *     responses.
   * @param <T> The class type of the response for deserialization.
   * @return The response entity parsed and converted to its type T.
   */
  @Override
  public <T extends RESTResponse> T post(
      String path,
      RESTRequest body,
      Class<T> responseType,
      Map<String, String> headers,
      Consumer<ErrorResponse> errorHandler) {
    return execute(Method.POST, path, null, body, responseType, headers, errorHandler);
  }

  /**
   * Sends an HTTP POST request to the specified path with the provided request body and processes
   * the response with support for response headers.
   *
   * @param path The URL path to send the POST request to.
   * @param body The REST request to place in the request body.
   * @param responseType The class type of the response for deserialization (Must be registered with
   *     the ObjectMapper).
   * @param headers A map of request headers (key-value pairs) to include in the request (can be
   *     null).
   * @param errorHandler The error handler delegated for HTTP responses, which handles server error
   *     responses.
   * @param responseHeaders The consumer of the response headers for further processing.
   * @param <T> The class type of the response for deserialization.
   * @return The response entity parsed and converted to its type T.
   */
  @Override
  public <T extends RESTResponse> T post(
      String path,
      RESTRequest body,
      Class<T> responseType,
      Map<String, String> headers,
      Consumer<ErrorResponse> errorHandler,
      Consumer<Map<String, String>> responseHeaders) {
    return execute(
        Method.POST, path, null, body, responseType, headers, errorHandler, responseHeaders);
  }

  /**
   * Sends an HTTP PUT request to the specified path with the provided request body and processes
   * the response.
   *
   * @param path The URL path to send the PUT request to.
   * @param body The REST request to place in the request body.
   * @param responseType The class type of the response for deserialization (Must be registered with
   *     the ObjectMapper).
   * @param headers A map of request headers (key-value pairs) to include in the request (can be
   *     null).
   * @param errorHandler The error handler delegated for HTTP responses, which handles server error
   *     responses.
   * @param <T> The class type of the response for deserialization.
   * @return The response entity parsed and converted to its type T.
   */
  @Override
  public <T extends RESTResponse> T put(
      String path,
      RESTRequest body,
      Class<T> responseType,
      Map<String, String> headers,
      Consumer<ErrorResponse> errorHandler) {
    return execute(Method.PUT, path, null, body, responseType, headers, errorHandler);
  }

  /**
   * Sends an HTTP PUT request to the specified path with the provided request body and processes
   * the response with support for response headers.
   *
   * @param path The URL path to send the PUT request to.
   * @param body The REST request to place in the request body.
   * @param responseType The class type of the response for deserialization (Must be registered with
   *     the ObjectMapper).
   * @param headers A map of request headers (key-value pairs) to include in the request (can be
   *     null).
   * @param errorHandler The error handler delegated for HTTP responses, which handles server error
   *     responses.
   * @param responseHeaders The consumer of the response headers for further processing.
   * @param <T> The class type of the response for deserialization.
   * @return The response entity parsed and converted to its type T.
   */
  @Override
  public <T extends RESTResponse> T put(
      String path,
      RESTRequest body,
      Class<T> responseType,
      Map<String, String> headers,
      Consumer<ErrorResponse> errorHandler,
      Consumer<Map<String, String>> responseHeaders) {
    return execute(
        Method.PUT, path, null, body, responseType, headers, errorHandler, responseHeaders);
  }

  /**
   * Sends an HTTP DELETE request to the specified path without query parameters and processes the
   * response.
   *
   * @param path The URL path to send the DELETE request to.
   * @param responseType The class type of the response for deserialization (Must be registered with
   *     the ObjectMapper).
   * @param headers A map of request headers (key-value pairs) to include in the request (can be
   *     null).
   * @param errorHandler The error handler delegated for HTTP responses, which handles server error
   *     responses.
   * @param <T> The class type of the response for deserialization.
   * @return The response entity parsed and converted to its type T.
   */
  @Override
  public <T extends RESTResponse> T delete(
      String path,
      Class<T> responseType,
      Map<String, String> headers,
      Consumer<ErrorResponse> errorHandler) {
    return execute(Method.DELETE, path, null, null, responseType, headers, errorHandler);
  }

  /**
   * Sends an HTTP DELETE request to the specified path with the provided query parameters and
   * processes the response.
   *
   * @param path The URL path to send the DELETE request to.
   * @param queryParams A map of query parameters (key-value pairs) to include in the request (can
   *     be null).
   * @param responseType The class type of the response for deserialization (Must be registered with
   *     the ObjectMapper).
   * @param headers A map of request headers (key-value pairs) to include in the request (can be
   *     null).
   * @param errorHandler The error handler delegated for HTTP responses, which handles server error
   *     responses.
   * @param <T> The class type of the response for deserialization.
   * @return The response entity parsed and converted to its type T.
   */
  @Override
  public <T extends RESTResponse> T delete(
      String path,
      Map<String, String> queryParams,
      Class<T> responseType,
      Map<String, String> headers,
      Consumer<ErrorResponse> errorHandler) {
    return execute(Method.DELETE, path, queryParams, null, responseType, headers, errorHandler);
  }

  /**
   * Sends an HTTP POST request with form data to the specified path and processes the response.
   *
   * @param path The URL path to send the POST request to.
   * @param formData A map of form data (key-value pairs) to include in the request body.
   * @param responseType The class type of the response for deserialization (Must be registered with
   *     the ObjectMapper).
   * @param headers A map of request headers (key-value pairs) to include in the request (can be
   *     null).
   * @param errorHandler The error handler delegated for HTTP responses, which handles server error
   *     responses.
   * @param <T> The class type of the response for deserialization.
   * @return The response entity parsed and converted to its type T.
   */
  @Override
  public <T extends RESTResponse> T postForm(
      String path,
      Map<String, String> formData,
      Class<T> responseType,
      Map<String, String> headers,
      Consumer<ErrorResponse> errorHandler) {
    return execute(Method.POST, path, null, formData, responseType, headers, errorHandler);
  }

  /**
   * Adds the specified request headers to the given HTTP request along with a specified body MIME
   * type.
   *
   * @param request The HTTP request to which headers are to be added.
   * @param requestHeaders A map of headers to be added to the request.
   * @param bodyMimeType The MIME type of the request body.
   */
  private void addRequestHeaders(
      HttpUriRequest request, Map<String, String> requestHeaders, String bodyMimeType) {
    // Some systems require the Content-Type header to be set even for empty-bodied requests to
    // avoid failures.
    request.setHeader(HttpHeaders.CONTENT_TYPE, bodyMimeType);
    request.setHeader(HttpHeaders.ACCEPT, VERSION_HEADER);
    requestHeaders.forEach(request::setHeader);
  }

  /**
   * Closes the underlying HTTP client gracefully.
   *
   * @throws IOException If an I/O error occurs while closing the HTTP client.
   */
  @Override
  public void close() throws IOException {
    if (authDataProvider != null) {
      authDataProvider.close();
    }
    httpClient.close(CloseMode.GRACEFUL);
  }

  /**
   * Creates a new instance of the HTTPClient.Builder with the specified properties.
   *
   * @param properties A map of properties (key-value pairs) used to configure the HTTP client.
   * @return A new instance of HTTPClient.Builder with the provided properties.
   */
  public static Builder builder(Map<String, String> properties) {
    return new Builder(properties);
  }

  /**
   * Builder class for configuring and creating instances of HTTPClient.
   *
   * <p>This class allows for setting various configuration options for the HTTP client such as base
   * URI, request headers, and ObjectMapper.
   */
  public static class Builder {
    private final Map<String, String> properties;
    private final Map<String, String> baseHeaders = Maps.newHashMap();
    private String uri;
    private ObjectMapper mapper = JsonUtils.objectMapper();
    private AuthDataProvider authDataProvider;

    private Builder(Map<String, String> properties) {
      this.properties = properties;
    }

    /**
     * Sets the base URI for the HTTP client.
     *
     * @param baseUri The base URI to be used for all HTTP requests.
     * @return This Builder instance for method chaining.
     */
    public Builder uri(String baseUri) {
      Preconditions.checkNotNull(baseUri, "Invalid uri for http client: null");
      this.uri = RESTUtils.stripTrailingSlash(baseUri);
      return this;
    }

    /**
     * Adds a single request header to the HTTP client.
     *
     * @param key The header name.
     * @param value The header value.
     * @return This Builder instance for method chaining.
     */
    public Builder withHeader(String key, String value) {
      baseHeaders.put(key, value);
      return this;
    }

    /**
     * Adds multiple request headers to the HTTP client.
     *
     * @param headers A map of request headers (key-value pairs) to be included in all HTTP
     *     requests.
     * @return This Builder instance for method chaining.
     */
    public Builder withHeaders(Map<String, String> headers) {
      baseHeaders.putAll(headers);
      return this;
    }

    /**
     * Sets the custom ObjectMapper for the HTTP client.
     *
     * @param objectMapper The custom ObjectMapper to be used for request/response serialization.
     * @return This Builder instance for method chaining.
     */
    public Builder withObjectMapper(ObjectMapper objectMapper) {
      this.mapper = objectMapper;
      return this;
    }

    /**
     * Sets the AuthDataProvider for the HTTP client.
     *
     * @param authDataProvider The authDataProvider providing the data used to authenticate.
     * @return This Builder instance for method chaining.
     */
    public Builder withAuthDataProvider(AuthDataProvider authDataProvider) {
      this.authDataProvider = authDataProvider;
      return this;
    }

    /**
     * Builds and returns an instance of the HTTPClient with the configured options.
     *
     * @return An instance of HTTPClient with the configured options.
     */
    public HTTPClient build() {

      return new HTTPClient(uri, baseHeaders, mapper, authDataProvider);
    }
  }

  private StringEntity toJson(Object requestBody) {
    try {
      return new StringEntity(mapper.writeValueAsString(requestBody));
    } catch (JsonProcessingException e) {
      throw new RESTException(e, "Failed to write request body: %s", requestBody);
    }
  }

  private StringEntity toFormEncoding(Map<?, ?> formData) {
    return new StringEntity(RESTUtils.encodeFormData(formData));
  }
}
