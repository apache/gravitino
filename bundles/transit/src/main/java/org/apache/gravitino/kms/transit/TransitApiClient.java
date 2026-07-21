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
package org.apache.gravitino.kms.transit;

import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.json.JsonMapper;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Optional;
import java.util.stream.Collectors;
import org.apache.gravitino.encryption.kms.KmsAuthenticationException;
import org.apache.gravitino.exceptions.ConnectionFailedException;
import org.apache.hc.client5.http.classic.methods.HttpGet;
import org.apache.hc.client5.http.config.ConnectionConfig;
import org.apache.hc.client5.http.config.RequestConfig;
import org.apache.hc.client5.http.impl.classic.CloseableHttpClient;
import org.apache.hc.client5.http.impl.classic.HttpClients;
import org.apache.hc.client5.http.impl.io.PoolingHttpClientConnectionManagerBuilder;
import org.apache.hc.core5.http.HttpEntity;
import org.apache.hc.core5.util.Timeout;

final class TransitApiClient {

  private static final String TOKEN_HEADER = "X-Vault-Token";
  private static final int MAX_RESPONSE_BYTES = 1024 * 1024;
  private static final int CONNECT_TIMEOUT_MILLIS = 5_000;
  private static final int REQUEST_TIMEOUT_MILLIS = 10_000;
  private static final ObjectMapper OBJECT_MAPPER = createObjectMapper();

  private final String providerName;
  private final URI serviceAddress;
  private final String transitMount;
  private final FileTokenSupplier tokenSupplier;
  private final CloseableHttpClient httpClient;

  TransitApiClient(String providerName, URI serviceAddress, String transitMount, Path tokenFile) {
    this(
        providerName,
        serviceAddress,
        transitMount,
        new FileTokenSupplier(providerName, tokenFile),
        createHttpClient());
  }

  TransitApiClient(
      String providerName,
      URI serviceAddress,
      String transitMount,
      FileTokenSupplier tokenSupplier,
      CloseableHttpClient httpClient) {
    this.providerName = providerName;
    this.serviceAddress = serviceAddress;
    this.transitMount = transitMount;
    this.tokenSupplier = tokenSupplier;
    this.httpClient = httpClient;
  }

  Optional<TransitReadKeyResponse> readKey(TransitReadKeyRequest request) {
    HttpResult result = execute(request, tokenSupplier.token());
    if (isAuthenticationFailure(result.statusCode)) {
      result = execute(request, tokenSupplier.reload());
    }

    if (result.statusCode == 404) {
      return Optional.empty();
    }
    if (isAuthenticationFailure(result.statusCode)) {
      throw new KmsAuthenticationException(
          "%s rejected its configured credentials (HTTP %s)", providerName, result.statusCode);
    }
    if (result.statusCode < 200 || result.statusCode >= 300) {
      throw new ConnectionFailedException(
          "%s could not inspect key %s (HTTP %s)",
          providerName, request.keyId(), result.statusCode);
    }

    try {
      return Optional.of(OBJECT_MAPPER.readValue(result.body, TransitReadKeyResponse.class));
    } catch (IOException | RuntimeException e) {
      throw new ConnectionFailedException(e, "%s returned a malformed response", providerName);
    }
  }

  void close() {
    try {
      httpClient.close();
    } catch (IOException e) {
      throw new ConnectionFailedException(e, "Failed to close the %s HTTP client", providerName);
    }
  }

  private HttpResult execute(TransitReadKeyRequest request, String token) {
    HttpGet httpRequest = new HttpGet(keyMetadataUri(request.keyId()));
    httpRequest.setHeader("Accept", "application/json");
    httpRequest.setHeader(TOKEN_HEADER, token);
    try {
      return httpClient.execute(
          httpRequest,
          response -> new HttpResult(response.getCode(), readResponse(response.getEntity())));
    } catch (ConnectionFailedException e) {
      throw e;
    } catch (IOException | RuntimeException e) {
      throw new ConnectionFailedException(
          e, "%s is unavailable while inspecting key %s", providerName, request.keyId());
    }
  }

  private URI keyMetadataUri(String keyId) {
    String encodedMount =
        Arrays.stream(transitMount.split("/"))
            .map(TransitApiClient::encodePathSegment)
            .collect(Collectors.joining("/"));
    return URI.create(
        String.format("%s/v1/%s/keys/%s", serviceAddress, encodedMount, encodePathSegment(keyId)));
  }

  private static byte[] readResponse(HttpEntity entity) throws IOException {
    if (entity == null) {
      return new byte[0];
    }
    if (entity.getContentLength() > MAX_RESPONSE_BYTES) {
      throw new ConnectionFailedException("Transit response exceeds the allowed size");
    }

    try (InputStream responseBody = entity.getContent();
        ByteArrayOutputStream output = new ByteArrayOutputStream()) {
      byte[] buffer = new byte[8192];
      int remaining = MAX_RESPONSE_BYTES + 1;
      while (remaining > 0) {
        int read = responseBody.read(buffer, 0, Math.min(buffer.length, remaining));
        if (read < 0) {
          break;
        }
        output.write(buffer, 0, read);
        remaining -= read;
      }
      if (output.size() > MAX_RESPONSE_BYTES) {
        throw new ConnectionFailedException("Transit response exceeds the allowed size");
      }
      return output.toByteArray();
    }
  }

  private static boolean isAuthenticationFailure(int statusCode) {
    return statusCode == 401 || statusCode == 403;
  }

  private static String encodePathSegment(String value) {
    try {
      return URLEncoder.encode(value, StandardCharsets.UTF_8.name()).replace("+", "%20");
    } catch (IOException e) {
      throw new ConnectionFailedException(e, "Failed to encode a Transit key reference");
    }
  }

  private static ObjectMapper createObjectMapper() {
    return JsonMapper.builder().disable(MapperFeature.ALLOW_COERCION_OF_SCALARS).build();
  }

  private static CloseableHttpClient createHttpClient() {
    Timeout connectTimeout = Timeout.ofMilliseconds(CONNECT_TIMEOUT_MILLIS);
    Timeout requestTimeout = Timeout.ofMilliseconds(REQUEST_TIMEOUT_MILLIS);
    ConnectionConfig connectionConfig =
        ConnectionConfig.custom()
            .setConnectTimeout(connectTimeout)
            .setSocketTimeout(requestTimeout)
            .build();
    RequestConfig requestConfig =
        RequestConfig.custom()
            .setConnectionRequestTimeout(requestTimeout)
            .setResponseTimeout(requestTimeout)
            .build();
    return HttpClients.custom()
        .setConnectionManager(
            PoolingHttpClientConnectionManagerBuilder.create()
                .setDefaultConnectionConfig(connectionConfig)
                .build())
        .setDefaultRequestConfig(requestConfig)
        .disableAutomaticRetries()
        .disableRedirectHandling()
        .build();
  }

  private static final class HttpResult {
    private final int statusCode;
    private final byte[] body;

    private HttpResult(int statusCode, byte[] body) {
      this.statusCode = statusCode;
      this.body = body;
    }
  }
}
