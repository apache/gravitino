/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.client;

import com.datastrato.gravitino.NameIdentifier;
import com.datastrato.gravitino.Version;
import com.datastrato.gravitino.dto.responses.MetalakeResponse;
import com.datastrato.gravitino.dto.responses.VersionResponse;
import com.datastrato.gravitino.exceptions.GravitinoRuntimeException;
import com.datastrato.gravitino.exceptions.NoSuchMetalakeException;
import com.datastrato.gravitino.json.JsonUtils;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import java.io.Closeable;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Collections;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Base class for Gravitino Java client;
 *
 * <p>It uses an underlying {@link RESTClient} to send HTTP requests and receive responses from the
 * API.
 */
public abstract class GravitinoClientBase implements Closeable {

  private static final Logger LOG = LoggerFactory.getLogger(GravitinoClientBase.class);
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
   */
  protected GravitinoClientBase(
      String uri,
      AuthDataProvider authDataProvider,
      boolean checkVersion,
      Map<String, String> headers) {
    ObjectMapper mapper = JsonUtils.objectMapper();
    mapper.disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);

    if (checkVersion) {
      this.restClient =
          HTTPClient.builder(Collections.emptyMap())
              .uri(uri)
              .withAuthDataProvider(authDataProvider)
              .withObjectMapper(mapper)
              .withPreConnectHandle(this::checkVersion)
              .withHeaders(headers)
              .build();

    } else {
      this.restClient =
          HTTPClient.builder(Collections.emptyMap())
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
    if (clientVersion.compareTo(serverVersion) > 0) {
      throw new GravitinoRuntimeException(
          "Gravitino does not support the case that the client-side version is higher than the server-side version."
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
   * @param ident The identifier of the Metalake to be loaded.
   * @return A GravitinoMetalake instance representing the loaded Metalake.
   * @throws NoSuchMetalakeException If the specified Metalake does not exist.
   */
  public GravitinoMetalake loadMetalake(NameIdentifier ident) throws NoSuchMetalakeException {
    NameIdentifier.checkMetalake(ident);

    MetalakeResponse resp =
        restClient.get(
            API_METALAKES_IDENTIFIER_PATH + ident.name(),
            MetalakeResponse.class,
            Collections.emptyMap(),
            ErrorHandlers.metalakeErrorHandler());
    resp.validate();

    return DTOConverters.toMetaLake(resp.getMetalake(), restClient);
  }

  /**
   * Retrieves the version of the Gravitino API.
   *
   * @return A GravitinoVersion instance representing the version of the Gravitino API.
   */
  @Deprecated
  public GravitinoVersion getVersion() {
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
        LOG.warn("Failed to close the HTTP REST client", e);
      }
    }
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
     * Builds a new instance. Subclasses should overwrite this method.
     *
     * @return A new instance of Gravitino Client.
     * @throws IllegalArgumentException If the base URI is null or empty.
     * @throws UnsupportedOperationException If subclass has not implemented.
     */
    public abstract T build();
  }
}
