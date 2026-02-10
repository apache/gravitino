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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.errorprone.annotations.InlineMe;
import java.io.Closeable;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Collections;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.Version;
import org.apache.gravitino.dto.responses.MetalakeResponse;
import org.apache.gravitino.dto.responses.VersionResponse;
import org.apache.gravitino.exceptions.GravitinoRuntimeException;
import org.apache.gravitino.exceptions.IllegalNameIdentifierException;
import org.apache.gravitino.exceptions.NoSuchMetalakeException;

/**
 * Base class for Gravitino Java client;
 *
 * <p>It uses an underlying {@link RESTClient} to send HTTP requests and receive responses from the
 * API.
 */
public abstract class GravitinoClientBase implements Closeable {

  /** The REST client to communicate with the REST server */
  protected final RESTClient restClient;
  /** The REST API path for listing metalakes */
  protected static final String API_METALAKES_LIST_PATH = "api/metalakes";
  /** The REST API path prefix for load a specific metalake */
  protected static final String API_METALAKES_IDENTIFIER_PATH = API_METALAKES_LIST_PATH + "/";

  /**
   * Constructs a new GravitinoClient with the given URI, authenticator and AuthDataProvider.
   *
   * @param uri The base URI for the Gravitino API.
   * @param authDataProvider The provider of the data which is used for authentication.
   * @param checkVersion Whether to check the version of the Gravitino server.
   * @param headers The base header of the Gravitino API.
   * @param properties A map of properties (key-value pairs) used to configure the Gravitino client.
   */
  protected GravitinoClientBase(
      String uri,
      AuthDataProvider authDataProvider,
      boolean checkVersion,
      Map<String, String> headers,
      Map<String, String> properties) {
    ObjectMapper mapper = ObjectMapperProvider.objectMapper();

    if (checkVersion) {
      this.restClient =
          HTTPClient.builder(properties)
              .uri(uri)
              .withAuthDataProvider(authDataProvider)
              .withObjectMapper(mapper)
              .withPreConnectHandler(this::checkVersion)
              .withHeaders(headers)
              .build();

    } else {
      this.restClient =
          HTTPClient.builder(properties)
              .uri(uri)
              .withAuthDataProvider(authDataProvider)
              .withObjectMapper(mapper)
              .withHeaders(headers)
              .build();
    }
  }

  /**
   * Check the compatibility of the client with the target server.
   *
   * @throws GravitinoRuntimeException If the client version is greater than the server version.
   */
  public void checkVersion() {
    GravitinoVersion serverVersion = serverVersion();
    GravitinoVersion clientVersion = clientVersion();
    if (!clientVersion.compatibleWithServerVersion(serverVersion)) {
      throw new GravitinoRuntimeException(
          "Gravitino does not support the case that the client-side version is higher than the server version."
              + "The client version is %s, and the server version %s",
          clientVersion.version(), serverVersion.version());
    }
  }

  /**
   * Retrieves the version of the Gravitino client.
   *
   * @return A GravitinoVersion instance representing the version of the Gravitino client.
   */
  public GravitinoVersion clientVersion() {
    return new GravitinoVersion(Version.getCurrentVersionDTO());
  }

  /**
   * Loads a specific Metalake from the Gravitino API.
   *
   * @param metalakeName The name of the Metalake to be loaded.
   * @return A GravitinoMetalake instance representing the loaded Metalake.
   * @throws NoSuchMetalakeException If the specified Metalake does not exist.
   */
  public GravitinoMetalake loadMetalake(String metalakeName) throws NoSuchMetalakeException {

    checkMetalakeName(metalakeName);

    MetalakeResponse resp =
        restClient.get(
            API_METALAKES_IDENTIFIER_PATH + metalakeName,
            MetalakeResponse.class,
            Collections.emptyMap(),
            ErrorHandlers.metalakeErrorHandler());
    resp.validate();

    return DTOConverters.toMetaLake(resp.getMetalake(), restClient);
  }

  /**
   * Checks the validity of the Metalake name.
   *
   * @param metalakeName the name of the Metalake to be checked.
   * @throws IllegalNameIdentifierException If the Metalake name is invalid.
   */
  public void checkMetalakeName(String metalakeName) {
    NameIdentifier identifier = NameIdentifier.parse(metalakeName);
    Namespace.check(
        identifier.namespace() != null && identifier.namespace().isEmpty(),
        "Metalake namespace must be non-null and empty, the input namespace is %s",
        identifier.namespace());
  }

  /**
   * Retrieves the version of the Gravitino API.
   *
   * @return A GravitinoVersion instance representing the version of the Gravitino API.
   * @deprecated This method is deprecated because it is a duplicate of {@link #serverVersion()}.
   *     Use {@link #serverVersion()} instead for clarity and consistency.
   */
  @Deprecated
  @InlineMe(replacement = "this.serverVersion()")
  public final GravitinoVersion getVersion() {
    return serverVersion();
  }

  /**
   * Retrieves the server version of the Gravitino server.
   *
   * @return A GravitinoVersion instance representing the server version of the Gravitino API.
   */
  public GravitinoVersion serverVersion() {
    VersionResponse resp =
        restClient.get(
            "api/version",
            VersionResponse.class,
            Collections.emptyMap(),
            ErrorHandlers.restErrorHandler());
    resp.validate();

    return new GravitinoVersion(resp.getVersion());
  }

  /** Closes the GravitinoClient and releases any underlying resources. */
  @Override
  public void close() {
    if (restClient != null) {
      try {
        restClient.close();
      } catch (Exception e) {
        // Swallow the exception
      }
    }
  }

  @VisibleForTesting
  RESTClient restClient() {
    return restClient;
  }

  /** Builder class for constructing a GravitinoClient. */
  public abstract static class Builder<T> {
    /** The base URI for the Gravitino API. */
    protected String uri;
    /** The authentication provider. */
    protected AuthDataProvider authDataProvider;
    /** The check version flag. */
    protected boolean checkVersion = true;
    /** The request base header for the Gravitino API. */
    protected Map<String, String> headers = ImmutableMap.of();
    /** A map of properties (key-value pairs) used to configure the Gravitino client. */
    protected Map<String, String> properties = ImmutableMap.of();

    /**
     * The constructor for the Builder class.
     *
     * @param uri The base URI for the Gravitino API.
     */
    protected Builder(String uri) {
      this.uri = uri;
    }

    /**
     * Sets the simple mode authentication for Gravitino
     *
     * @return This Builder instance for method chaining.
     */
    public Builder<T> withSimpleAuth() {
      this.authDataProvider = new SimpleTokenProvider();
      return this;
    }

    /**
     * Sets the simple mode authentication for Gravitino with a specific username
     *
     * @param userName The username of the user.
     * @return This Builder instance for method chaining.
     */
    public Builder<T> withSimpleAuth(String userName) {
      Preconditions.checkArgument(StringUtils.isNotBlank(userName), "userName can't be blank");
      this.authDataProvider = new SimpleTokenProvider(userName);
      return this;
    }

    /**
     * Optional, set a flag to verify the client is supported to connector the server
     *
     * @return This Builder instance for method chaining.
     */
    public Builder<T> withVersionCheckDisabled() {
      this.checkVersion = false;
      return this;
    }

    /**
     * Sets OAuth2TokenProvider for Gravitino.
     *
     * @param dataProvider The OAuth2TokenProvider used as the provider of authentication data for
     *     Gravitino Client.
     * @return This Builder instance for method chaining.
     */
    public Builder<T> withOAuth(OAuth2TokenProvider dataProvider) {
      this.authDataProvider = dataProvider;
      return this;
    }

    /**
     * Sets KerberosTokenProvider for the Gravitino.
     *
     * @param dataProvider The KerberosTokenProvider used as the provider of authentication data for
     *     Gravitino Client.
     * @return This Builder instance for method chaining.
     */
    public Builder<T> withKerberosAuth(KerberosTokenProvider dataProvider) {
      try {
        if (uri != null) {
          dataProvider.setHost(new URI(uri).getHost());
        }
      } catch (URISyntaxException ue) {
        throw new IllegalArgumentException("URI has the wrong format", ue);
      }
      this.authDataProvider = dataProvider;
      return this;
    }

    /**
     * Sets CustomTokenProvider for the Gravitino.
     *
     * @param dataProvider The CustomTokenProvider used as the provider of authentication data for
     *     Gravitino Client.
     * @return This Builder instance for method chaining.
     */
    public Builder<T> withCustomTokenAuth(CustomTokenProvider dataProvider) {
      this.authDataProvider = dataProvider;
      return this;
    }

    /**
     * Set base header for Gravitino Client.
     *
     * @param headers the base header.
     * @return This Builder instance for method chaining.
     */
    public Builder<T> withHeaders(Map<String, String> headers) {
      if (headers != null) {
        this.headers = ImmutableMap.copyOf(headers);
      }
      return this;
    }

    /**
     * Set base client config for Gravitino Client.
     *
     * @param properties A map of properties (key-value pairs) used to configure the Gravitino
     *     client.
     * @return This Builder instance for method chaining.
     */
    public Builder<T> withClientConfig(Map<String, String> properties) {
      if (properties != null) {
        this.properties = ImmutableMap.copyOf(properties);
      }
      return this;
    }

    /**
     * Builds a new instance. Subclasses should overwrite this method.
     *
     * @return A new instance of Gravitino Client.
     * @throws IllegalArgumentException If the base URI is null or empty.
     * @throws UnsupportedOperationException If subclass has not implemented.
     */
    public abstract T build();
  }
}
