/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.client;

import com.datastrato.gravitino.MetalakeChange;
import com.datastrato.gravitino.NameIdentifier;
import com.datastrato.gravitino.SupportsMetalakes;
import com.datastrato.gravitino.authorization.SupportsGrantsManagement;
import com.datastrato.gravitino.dto.requests.MetalakeCreateRequest;
import com.datastrato.gravitino.dto.requests.MetalakeUpdateRequest;
import com.datastrato.gravitino.dto.requests.MetalakeUpdatesRequest;
import com.datastrato.gravitino.dto.requests.RoleGrantRequest;
import com.datastrato.gravitino.dto.responses.DropResponse;
import com.datastrato.gravitino.dto.responses.GrantResponse;
import com.datastrato.gravitino.dto.responses.MetalakeListResponse;
import com.datastrato.gravitino.dto.responses.MetalakeResponse;
import com.datastrato.gravitino.dto.responses.RevokeResponse;
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
public class GravitinoAdminClient extends GravitinoClientBase
    implements SupportsMetalakes, SupportsGrantsManagement {

  private static final Logger LOG = LoggerFactory.getLogger(GravitinoAdminClient.class);
  private static final String API_PERMISSION_PATH = "api/metalakes/%s/permissions/%s";
  private static final String PERMISSION_USER_PATH = "users/%s/roles/%s";
  private static final String PERMISSION_GROUP_PATH = "groups/%s/roles/%s";
  private static final String BLANK_PLACE_HOLDER = "";

  /**
   * Constructs a new GravitinoClient with the given URI, authenticator and AuthDataProvider.
   *
   * @param uri The base URI for the Gravitino API.
   * @param authDataProvider The provider of the data which is used for authentication.
   */
  private GravitinoAdminClient(String uri, AuthDataProvider authDataProvider) {
    super(uri, authDataProvider);
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

  @Override
  public boolean grantRoleToUser(String metalake, String role, String user) {
    return grantInternal(metalake, PERMISSION_USER_PATH, role, user);
  }

  @Override
  public boolean grantRoleToGroup(String metalake, String role, String group) {
    return grantInternal(metalake, PERMISSION_GROUP_PATH, role, group);
  }

  @Override
  public boolean revokeRoleFromUser(String metalake, String role, String user) {
    return revokeInternal(metalake, PERMISSION_USER_PATH, role, user);
  }

  @Override
  public boolean revokeRoleFromGroup(String metalake, String role, String group) {
    return revokeInternal(metalake, PERMISSION_GROUP_PATH, role, group);
  }

  private boolean grantInternal(String metalake, String path, String role, String name) {
    try {
      RoleGrantRequest request = new RoleGrantRequest(role);

      GrantResponse resp =
          restClient.post(
              String.format(
                  API_PERMISSION_PATH, metalake, String.format(path, name, BLANK_PLACE_HOLDER)),
              request,
              GrantResponse.class,
              Collections.emptyMap(),
              ErrorHandlers.grantErrorHandler());
      resp.validate();
      return resp.granted();
    } catch (Exception e) {
      LOG.warn("Failed to revoke role {} from {}", role, name, e);
      return false;
    }
  }

  private boolean revokeInternal(String metalake, String pattern, String role, String name) {
    try {
      RevokeResponse resp =
          restClient.delete(
              String.format(API_PERMISSION_PATH, metalake, String.format(pattern, name, role)),
              RevokeResponse.class,
              Collections.emptyMap(),
              ErrorHandlers.grantErrorHandler());
      resp.validate();
      return resp.removed();

    } catch (Exception e) {
      LOG.warn("Failed to grant role {} from {}", role, name, e);
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

      return new GravitinoAdminClient(uri, authDataProvider);
    }
  }
}
