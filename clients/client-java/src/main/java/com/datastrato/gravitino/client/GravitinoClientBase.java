/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.client;

import com.datastrato.gravitino.NameIdentifier;
import com.datastrato.gravitino.dto.responses.MetalakeResponse;
import com.datastrato.gravitino.dto.responses.VersionResponse;
import com.datastrato.gravitino.exceptions.NoSuchMetalakeException;
import java.io.Closeable;
import java.util.Collections;
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
   */
  protected GravitinoClientBase(String uri, AuthDataProvider authDataProvider) {
    this.restClient =
        HTTPClient.builder(Collections.emptyMap())
            .uri(uri)
            .withAuthDataProvider(authDataProvider)
            .build();
  }

  /**
   * Loads a specific Metalake from the Gravitino API.
   *
   * @param ident The identifier of the Metalake to be loaded.
   * @return A GravitinoMetaLake instance representing the loaded Metalake.
   * @throws NoSuchMetalakeException If the specified Metalake does not exist.
   */
  public GravitinoMetaLake loadMetalake(NameIdentifier ident) throws NoSuchMetalakeException {
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
  public GravitinoVersion getVersion() {
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
}
