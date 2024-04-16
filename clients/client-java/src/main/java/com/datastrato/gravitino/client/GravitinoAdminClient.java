/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.client;

import com.datastrato.gravitino.MetalakeChange;
import com.datastrato.gravitino.NameIdentifier;
import com.datastrato.gravitino.SupportsMetalakes;
import com.datastrato.gravitino.dto.requests.MetalakeCreateRequest;
import com.datastrato.gravitino.dto.requests.MetalakeUpdateRequest;
import com.datastrato.gravitino.dto.requests.MetalakeUpdatesRequest;
import com.datastrato.gravitino.dto.responses.DropResponse;
import com.datastrato.gravitino.dto.responses.MetalakeListResponse;
import com.datastrato.gravitino.dto.responses.MetalakeResponse;
import com.datastrato.gravitino.exceptions.MetalakeAlreadyExistsException;
import com.datastrato.gravitino.exceptions.NoSuchMetalakeException;
import com.google.common.base.Preconditions;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Gravitino Client for the administrator to interact with the Gravitino API, allowing the client to
 * list, load, create, and alter Metalakes.
 *
 * <p>Normal users should use {@link GravitinoClient} to connect with the Gravitino server.
 */
public class GravitinoAdminClient extends GravitinoClientBase implements SupportsMetalakes {

  private static final Logger LOG = LoggerFactory.getLogger(GravitinoAdminClient.class);

  /**
   * Constructs a new GravitinoClient with the given URI, authenticator and AuthDataProvider.
   *
   * @param uri The base URI for the Gravitino API.
   * @param authDataProvider The provider of the data which is used for authentication.
   * @param checkVersion Whether to check the version of the Gravitino server. Gravitino does not
   *     support the case that the client-side version is higher than the server-side version.
   */
  private GravitinoAdminClient(
      String uri, AuthDataProvider authDataProvider, boolean checkVersion) {
    super(uri, authDataProvider, checkVersion);
  }

  /**
   * Constructs a new GravitinoClient with the given URI, authenticator and AuthDataProvider.
   *
   * @param uri The base URI for the Gravitino API.
   * @param authDataProvider The provider of the data which is used for authentication.
   */
  private GravitinoAdminClient(String uri, AuthDataProvider authDataProvider) {
    this(uri, authDataProvider, true);
  }

  /**
   * Retrieves a list of Metalakes from the Gravitino API.
   *
   * @return An array of GravitinoMetalake objects representing the Metalakes.
   */
  @Override
  public GravitinoMetalake[] listMetalakes() {
    MetalakeListResponse resp =
        restClient.get(
            API_METALAKES_LIST_PATH,
            MetalakeListResponse.class,
            Collections.emptyMap(),
            ErrorHandlers.metalakeErrorHandler());
    resp.validate();

    return Arrays.stream(resp.getMetalakes())
        .map(o -> DTOConverters.toMetaLake(o, restClient))
        .toArray(GravitinoMetalake[]::new);
  }

  /**
   * Creates a new Metalake using the Gravitino API.
   *
   * @param ident The identifier of the new Metalake.
   * @param comment The comment for the new Metalake.
   * @param properties The properties of the new Metalake.
   * @return A GravitinoMetalake instance representing the newly created Metalake.
   * @throws MetalakeAlreadyExistsException If a Metalake with the specified identifier already
   *     exists.
   */
  @Override
  public GravitinoMetalake createMetalake(
      NameIdentifier ident, String comment, Map<String, String> properties)
      throws MetalakeAlreadyExistsException {
    NameIdentifier.checkMetalake(ident);

    MetalakeCreateRequest req = new MetalakeCreateRequest(ident.name(), comment, properties);
    req.validate();

    MetalakeResponse resp =
        restClient.post(
            API_METALAKES_LIST_PATH,
            req,
            MetalakeResponse.class,
            Collections.emptyMap(),
            ErrorHandlers.metalakeErrorHandler());
    resp.validate();

    return DTOConverters.toMetaLake(resp.getMetalake(), restClient);
  }

  /**
   * Alters a specific Metalake using the Gravitino API.
   *
   * @param ident The identifier of the Metalake to be altered.
   * @param changes The changes to be applied to the Metalake.
   * @return A GravitinoMetalake instance representing the updated Metalake.
   * @throws NoSuchMetalakeException If the specified Metalake does not exist.
   * @throws IllegalArgumentException If the provided changes are invalid or not applicable.
   */
  @Override
  public GravitinoMetalake alterMetalake(NameIdentifier ident, MetalakeChange... changes)
      throws NoSuchMetalakeException, IllegalArgumentException {
    NameIdentifier.checkMetalake(ident);

    List<MetalakeUpdateRequest> reqs =
        Arrays.stream(changes)
            .map(DTOConverters::toMetalakeUpdateRequest)
            .collect(Collectors.toList());
    MetalakeUpdatesRequest updatesRequest = new MetalakeUpdatesRequest(reqs);
    updatesRequest.validate();

    MetalakeResponse resp =
        restClient.put(
            API_METALAKES_IDENTIFIER_PATH + ident.name(),
            updatesRequest,
            MetalakeResponse.class,
            Collections.emptyMap(),
            ErrorHandlers.metalakeErrorHandler());
    resp.validate();

    return DTOConverters.toMetaLake(resp.getMetalake(), restClient);
  }

  /**
   * Drops a specific Metalake using the Gravitino API.
   *
   * @param ident The identifier of the Metalake to be dropped.
   * @return True if the Metalake was successfully dropped, false otherwise.
   */
  @Override
  public boolean dropMetalake(NameIdentifier ident) {
    NameIdentifier.checkMetalake(ident);

    try {
      DropResponse resp =
          restClient.delete(
              API_METALAKES_IDENTIFIER_PATH + ident.name(),
              DropResponse.class,
              Collections.emptyMap(),
              ErrorHandlers.metalakeErrorHandler());
      resp.validate();
      return resp.dropped();

    } catch (Exception e) {
      LOG.warn("Failed to drop metadata {}", ident, e);
      return false;
    }
  }

  /**
   * Creates a new builder for constructing a GravitinoClient.
   *
   * @param uri The base URI for the Gravitino API.
   * @return A new instance of the Builder class for constructing a GravitinoClient.
   */
  public static AdminClientBuilder builder(String uri) {
    return new AdminClientBuilder(uri);
  }

  /** Builder class for constructing a GravitinoAdminClient. */
  public static class AdminClientBuilder extends GravitinoClientBase.Builder<GravitinoAdminClient> {

    /**
     * The private constructor for the Builder class.
     *
     * @param uri The base URI for the Gravitino API.
     */
    protected AdminClientBuilder(String uri) {
      super(uri);
    }

    /**
     * Builds a new GravitinoClient instance.
     *
     * @return A new instance of GravitinoClient with the specified base URI.
     * @throws IllegalArgumentException If the base URI is null or empty.
     */
    @Override
    public GravitinoAdminClient build() {
      Preconditions.checkArgument(
          uri != null && !uri.isEmpty(), "The argument 'uri' must be a valid URI");

      return new GravitinoAdminClient(uri, authDataProvider, checkVersion);
    }
  }
}
