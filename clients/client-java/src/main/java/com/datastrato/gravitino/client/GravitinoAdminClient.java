/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.client;

import com.datastrato.gravitino.MetalakeChange;
import com.datastrato.gravitino.NameIdentifier;
import com.datastrato.gravitino.SupportsMetalakes;
import com.datastrato.gravitino.authorization.Group;
import com.datastrato.gravitino.authorization.SupportsGroupOperation;
import com.datastrato.gravitino.authorization.SupportsUserOperation;
import com.datastrato.gravitino.authorization.User;
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
    implements SupportsMetalakes, SupportsUserOperation, SupportsGroupOperation {

  private static final Logger LOG = LoggerFactory.getLogger(GravitinoAdminClient.class);
  private static final String API_METALAKES_USERS_PATH = "api/metalakes/%s/users/%s";
  private static final String API_METALAKES_GROUPS_PATH = "api/metalakes/%s/groups/%s";

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

  /**
   * Adds a new User.
   *
   * @param metalake The Metalake of the User.
   * @param user The name of the User.
   * @return The added User instance.
   * @throws UserAlreadyExistsException If a User with the same identifier already exists.
   * @throws RuntimeException If adding the User encounters storage issues.
   */
  @Override
  public User addUser(String metalake, String user) throws UserAlreadyExistsException {
    UserAddRequest req = new UserAddRequest(user);
    req.validate();

    UserResponse resp =
        restClient.post(
            String.format(API_METALAKES_USERS_PATH, metalake, ""),
            req,
            UserResponse.class,
            Collections.emptyMap(),
            ErrorHandlers.userErrorHandler());
    resp.validate();

    return resp.getUser();
  }

  /**
   * Removes a User.
   *
   * @param metalake The Metalake of the User.
   * @param user The name of the User.
   * @return `true` if the User was successfully removed, `false` otherwise.
   * @throws RuntimeException If removing the User encounters storage issues.
   */
  @Override
  public boolean removeUser(String metalake, String user) {
    try {
      RemoveResponse resp =
          restClient.delete(
              String.format(API_METALAKES_USERS_PATH, metalake, user),
              RemoveResponse.class,
              Collections.emptyMap(),
              ErrorHandlers.userErrorHandler());
      resp.validate();
      return resp.removed();

    } catch (Exception e) {
      LOG.warn("Failed to remove user {} from metalake {}", user, metalake, e);
      return false;
    }
  }

  /**
   * Gets a User.
   *
   * @param metalake The Metalake of the User.
   * @param user The name of the User.
   * @return The getting User instance.
   * @throws NoSuchUserException If the User with the given identifier does not exist.
   * @throws RuntimeException If getting the User encounters storage issues.
   */
  @Override
  public User getUser(String metalake, String user) throws NoSuchUserException {
    UserResponse resp =
        restClient.get(
            String.format(API_METALAKES_USERS_PATH, metalake, user),
            UserResponse.class,
            Collections.emptyMap(),
            ErrorHandlers.userErrorHandler());
    resp.validate();

    return resp.getUser();
  }

  /**
   * Adds a new Group.
   *
   * @param metalake The Metalake of the Group.
   * @param group The name of the Group.
   * @return The Added Group instance.
   * @throws GroupAlreadyExistsException If a Group with the same identifier already exists.
   * @throws RuntimeException If adding the Group encounters storage issues.
   */
  @Override
  public Group addGroup(String metalake, String group) throws GroupAlreadyExistsException {
    GroupAddRequest req = new GroupAddRequest(group);
    req.validate();

    GroupResponse resp =
        restClient.post(
            String.format(API_METALAKES_GROUPS_PATH, metalake, ""),
            req,
            GroupResponse.class,
            Collections.emptyMap(),
            ErrorHandlers.groupErrorHandler());
    resp.validate();

    return resp.getGroup();
  }

  /**
   * Removes a Group.
   *
   * @param metalake The Metalake of the Group.
   * @param group THe name of the Group.
   * @return `true` if the Group was successfully removed, `false` otherwise.
   * @throws RuntimeException If removing the Group encounters storage issues.
   */
  @Override
  public boolean removeGroup(String metalake, String group) {
    try {
      RemoveResponse resp =
          restClient.delete(
              String.format(API_METALAKES_GROUPS_PATH, metalake, group),
              RemoveResponse.class,
              Collections.emptyMap(),
              ErrorHandlers.groupErrorHandler());
      resp.validate();
      return resp.removed();

    } catch (Exception e) {
      LOG.warn("Failed to remove group {} from metalake {}", group, metalake, e);
      return false;
    }
  }

  /**
   * Gets a Group.
   *
   * @param metalake The Metalake of the Group.
   * @param group THe name of the Group.
   * @return The getting Group instance.
   * @throws NoSuchGroupException If the Group with the given identifier does not exist.
   * @throws RuntimeException If getting the Group encounters storage issues.
   */
  @Override
  public Group getGroup(String metalake, String group) throws NoSuchGroupException {
    GroupResponse resp =
        restClient.get(
            String.format(API_METALAKES_GROUPS_PATH, metalake, group),
            GroupResponse.class,
            Collections.emptyMap(),
            ErrorHandlers.groupErrorHandler());
    resp.validate();

    return resp.getGroup();
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
