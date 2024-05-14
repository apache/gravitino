/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.client;

import com.datastrato.gravitino.MetalakeChange;
import com.datastrato.gravitino.authorization.Group;
import com.datastrato.gravitino.authorization.Privilege;
import com.datastrato.gravitino.authorization.Role;
import com.datastrato.gravitino.authorization.SecurableObject;
import com.datastrato.gravitino.authorization.User;
import com.datastrato.gravitino.client.api.SupportsMetalakes;
import com.datastrato.gravitino.dto.requests.GroupAddRequest;
import com.datastrato.gravitino.dto.requests.MetalakeCreateRequest;
import com.datastrato.gravitino.dto.requests.MetalakeUpdateRequest;
import com.datastrato.gravitino.dto.requests.MetalakeUpdatesRequest;
import com.datastrato.gravitino.dto.requests.RoleCreateRequest;
import com.datastrato.gravitino.dto.requests.RoleGrantRequest;
import com.datastrato.gravitino.dto.requests.RoleRevokeRequest;
import com.datastrato.gravitino.dto.requests.UserAddRequest;
import com.datastrato.gravitino.dto.responses.DeleteResponse;
import com.datastrato.gravitino.dto.responses.DropResponse;
import com.datastrato.gravitino.dto.responses.GroupResponse;
import com.datastrato.gravitino.dto.responses.MetalakeListResponse;
import com.datastrato.gravitino.dto.responses.MetalakeResponse;
import com.datastrato.gravitino.dto.responses.RemoveResponse;
import com.datastrato.gravitino.dto.responses.RoleResponse;
import com.datastrato.gravitino.dto.responses.UserResponse;
import com.datastrato.gravitino.exceptions.GroupAlreadyExistsException;
import com.datastrato.gravitino.exceptions.MetalakeAlreadyExistsException;
import com.datastrato.gravitino.exceptions.NoSuchGroupException;
import com.datastrato.gravitino.exceptions.NoSuchMetalakeException;
import com.datastrato.gravitino.exceptions.NoSuchRoleException;
import com.datastrato.gravitino.exceptions.NoSuchUserException;
import com.datastrato.gravitino.exceptions.RoleAlreadyExistsException;
import com.datastrato.gravitino.exceptions.UserAlreadyExistsException;
import com.datastrato.gravitino.rel.SupportsMetalakes;
import com.google.common.base.Preconditions;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
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
  private static final String API_METALAKES_USERS_PATH = "api/metalakes/%s/users/%s";
  private static final String API_METALAKES_GROUPS_PATH = "api/metalakes/%s/groups/%s";
  private static final String API_METALAKES_ROLES_PATH = "api/metalakes/%s/roles/%s";
  private static final String API_ADMIN_PATH = "api/admins/%s";
  private static final String API_PERMISSION_PATH = "api/metalakes/%s/permissions/%s";
  private static final String BLANK_PLACE_HOLDER = "";

  /**
   * Constructs a new GravitinoClient with the given URI, authenticator and AuthDataProvider.
   *
   * @param uri The base URI for the Gravitino API.
   * @param authDataProvider The provider of the data which is used for authentication.
   * @param checkVersion Whether to check the version of the Gravitino server. Gravitino does not
   *     support the case that the client-side version is higher than the server-side version.
   * @param headers The base header for Gravitino API.
   */
  private GravitinoAdminClient(
      String uri,
      AuthDataProvider authDataProvider,
      boolean checkVersion,
      Map<String, String> headers) {
    super(uri, authDataProvider, checkVersion, headers);
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
   * @param name The name of the new Metalake.
   * @param comment The comment for the new Metalake.
   * @param properties The properties of the new Metalake.
   * @return A GravitinoMetalake instance representing the newly created Metalake.
   * @throws MetalakeAlreadyExistsException If a Metalake with the specified identifier already
   *     exists.
   */
  @Override
  public GravitinoMetalake createMetalake(
      String name, String comment, Map<String, String> properties)
      throws MetalakeAlreadyExistsException {
    checkMetalakeName(name);

    MetalakeCreateRequest req = new MetalakeCreateRequest(name, comment, properties);
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
   * @param name The name of the Metalake to be altered.
   * @param changes The changes to be applied to the Metalake.
   * @return A GravitinoMetalake instance representing the updated Metalake.
   * @throws NoSuchMetalakeException If the specified Metalake does not exist.
   * @throws IllegalArgumentException If the provided changes are invalid or not applicable.
   */
  @Override
  public GravitinoMetalake alterMetalake(String name, MetalakeChange... changes)
      throws NoSuchMetalakeException, IllegalArgumentException {
    checkMetalakeName(name);
    List<MetalakeUpdateRequest> reqs =
        Arrays.stream(changes)
            .map(DTOConverters::toMetalakeUpdateRequest)
            .collect(Collectors.toList());
    MetalakeUpdatesRequest updatesRequest = new MetalakeUpdatesRequest(reqs);
    updatesRequest.validate();

    MetalakeResponse resp =
        restClient.put(
            API_METALAKES_IDENTIFIER_PATH + name,
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
   * @param name The name of the Metalake to be dropped.
   * @return True if the Metalake was successfully dropped, false otherwise.
   */
  @Override
  public boolean dropMetalake(String name) {
    checkMetalakeName(name);
    try {
      DropResponse resp =
          restClient.delete(
              API_METALAKES_IDENTIFIER_PATH + name,
              DropResponse.class,
              Collections.emptyMap(),
              ErrorHandlers.metalakeErrorHandler());
      resp.validate();
      return resp.dropped();

    } catch (Exception e) {
      LOG.warn("Failed to drop metadata {}", name, e);
      return false;
    }
  }

  /**
   * Adds a new User.
   *
   * @param metalake The Metalake of the User.
   * @param user The name of the User.
   * @return The added User instance.
   * @throws UserAlreadyExistsException If a User with the same name already exists.
   * @throws NoSuchMetalakeException If the Metalake with the given name does not exist.
   * @throws RuntimeException If adding the User encounters storage issues.
   */
  public User addUser(String metalake, String user)
      throws UserAlreadyExistsException, NoSuchMetalakeException {
    UserAddRequest req = new UserAddRequest(user);
    req.validate();

    UserResponse resp =
        restClient.post(
            String.format(API_METALAKES_USERS_PATH, metalake, BLANK_PLACE_HOLDER),
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
   * @return True if the User was successfully removed, false only when there's no such user,
   *     otherwise it will throw an exception.
   * @throws NoSuchMetalakeException If the Metalake with the given name does not exist.
   * @throws RuntimeException If removing the User encounters storage issues.
   */
  public boolean removeUser(String metalake, String user) throws NoSuchMetalakeException {
    RemoveResponse resp =
        restClient.delete(
            String.format(API_METALAKES_USERS_PATH, metalake, user),
            RemoveResponse.class,
            Collections.emptyMap(),
            ErrorHandlers.userErrorHandler());
    resp.validate();

    return resp.removed();
  }

  /**
   * Gets a User.
   *
   * @param metalake The Metalake of the User.
   * @param user The name of the User.
   * @return The getting User instance.
   * @throws NoSuchUserException If the User with the given name does not exist.
   * @throws NoSuchMetalakeException If the Metalake with the given name does not exist.
   * @throws RuntimeException If getting the User encounters storage issues.
   */
  public User getUser(String metalake, String user)
      throws NoSuchUserException, NoSuchMetalakeException {
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
   * @throws GroupAlreadyExistsException If a Group with the same name already exists.
   * @throws NoSuchMetalakeException If the Metalake with the given name does not exist.
   * @throws RuntimeException If adding the Group encounters storage issues.
   */
  public Group addGroup(String metalake, String group)
      throws GroupAlreadyExistsException, NoSuchMetalakeException {
    GroupAddRequest req = new GroupAddRequest(group);
    req.validate();

    GroupResponse resp =
        restClient.post(
            String.format(API_METALAKES_GROUPS_PATH, metalake, BLANK_PLACE_HOLDER),
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
   * @return True if the Group was successfully removed, false only when there's no such group,
   *     otherwise it will throw an exception.
   * @throws NoSuchMetalakeException If the Metalake with the given name does not exist.
   * @throws RuntimeException If removing the Group encounters storage issues.
   */
  public boolean removeGroup(String metalake, String group) throws NoSuchMetalakeException {
    RemoveResponse resp =
        restClient.delete(
            String.format(API_METALAKES_GROUPS_PATH, metalake, group),
            RemoveResponse.class,
            Collections.emptyMap(),
            ErrorHandlers.groupErrorHandler());
    resp.validate();

    return resp.removed();
  }

  /**
   * Gets a Group.
   *
   * @param metalake The Metalake of the Group.
   * @param group The name of the Group.
   * @return The getting Group instance.
   * @throws NoSuchGroupException If the Group with the given name does not exist.
   * @throws NoSuchMetalakeException If the Metalake with the given name does not exist.
   * @throws RuntimeException If getting the Group encounters storage issues.
   */
  public Group getGroup(String metalake, String group)
      throws NoSuchGroupException, NoSuchMetalakeException {
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
   * Adds a new metalake admin.
   *
   * @param user The name of the User.
   * @return The added User instance.
   * @throws UserAlreadyExistsException If a metalake admin with the same name already exists.
   * @throws RuntimeException If adding the User encounters storage issues.
   */
  public User addMetalakeAdmin(String user) throws UserAlreadyExistsException {
    UserAddRequest req = new UserAddRequest(user);
    req.validate();

    UserResponse resp =
        restClient.post(
            String.format(API_ADMIN_PATH, BLANK_PLACE_HOLDER),
            req,
            UserResponse.class,
            Collections.emptyMap(),
            ErrorHandlers.userErrorHandler());
    resp.validate();

    return resp.getUser();
  }

  /**
   * Removes a metalake admin.
   *
   * @param user The name of the User.
   * @return True if the User was successfully removed, false only when there's no such metalake
   *     admin, otherwise it will throw an exception.
   * @throws RuntimeException If removing the User encounters storage issues.
   */
  public boolean removeMetalakeAdmin(String user) {
    RemoveResponse resp =
        restClient.delete(
            String.format(API_ADMIN_PATH, user),
            RemoveResponse.class,
            Collections.emptyMap(),
            ErrorHandlers.userErrorHandler());
    resp.validate();

    return resp.removed();
  }

  /**
   * Gets a Role.
   *
   * @param metalake The Metalake of the Role.
   * @param role The name of the Role.
   * @return The getting Role instance.
   * @throws NoSuchRoleException If the Role with the given name does not exist.
   * @throws NoSuchMetalakeException If the Metalake with the given name does not exist.
   * @throws RuntimeException If getting the Role encounters storage issues.
   */
  public Role getRole(String metalake, String role)
      throws NoSuchRoleException, NoSuchMetalakeException {
    RoleResponse resp =
        restClient.get(
            String.format(API_METALAKES_ROLES_PATH, metalake, role),
            RoleResponse.class,
            Collections.emptyMap(),
            ErrorHandlers.roleErrorHandler());
    resp.validate();

    return resp.getRole();
  }

  /**
   * Deletes a Role.
   *
   * @param metalake The Metalake of the Role.
   * @param role The name of the Role.
   * @return True if the Role was successfully deleted, false only when there's no such role,
   *     otherwise it will throw an exception.
   * @throws NoSuchMetalakeException If the Metalake with the given name does not exist.
   * @throws RuntimeException If deleting the Role encounters storage issues.
   */
  public boolean deleteRole(String metalake, String role) throws NoSuchMetalakeException {
    DeleteResponse resp =
        restClient.delete(
            String.format(API_METALAKES_ROLES_PATH, metalake, role),
            DeleteResponse.class,
            Collections.emptyMap(),
            ErrorHandlers.roleErrorHandler());
    resp.validate();

    return resp.deleted();
  }

  /**
   * Creates a new Role.
   *
   * @param metalake The Metalake of the Role.
   * @param role The name of the Role.
   * @param properties The properties of the Role.
   * @param securableObject The securable object of the Role.
   * @param privileges The privileges of the Role.
   * @return The created Role instance.
   * @throws RoleAlreadyExistsException If a Role with the same name already exists.
   * @throws NoSuchMetalakeException If the Metalake with the given name does not exist.
   * @throws RuntimeException If creating the Role encounters storage issues.
   */
  public Role createRole(
      String metalake,
      String role,
      Map<String, String> properties,
      SecurableObject securableObject,
      List<Privilege> privileges)
      throws RoleAlreadyExistsException, NoSuchMetalakeException {
    RoleCreateRequest req =
        new RoleCreateRequest(
            role,
            properties,
            privileges.stream()
                .map(Privilege::name)
                .map(Objects::toString)
                .collect(Collectors.toList()),
            DTOConverters.toSecurableObject(securableObject));
    req.validate();

    RoleResponse resp =
        restClient.post(
            String.format(API_METALAKES_ROLES_PATH, metalake, BLANK_PLACE_HOLDER),
            req,
            RoleResponse.class,
            Collections.emptyMap(),
            ErrorHandlers.roleErrorHandler());
    resp.validate();

    return resp.getRole();
  }
  /**
   * Grant roles to a user.
   *
   * @param metalake The metalake of the User.
   * @param user The name of the User.
   * @param roles The names of the Role.
   * @return The Group after granted.
   * @throws NoSuchUserException If the User with the given name does not exist.
   * @throws NoSuchRoleException If the Role with the given name does not exist.
   * @throws NoSuchMetalakeException If the Metalake with the given name does not exist.
   * @throws RuntimeException If granting roles to a user encounters storage issues.
   */
  public User grantRolesToUser(String metalake, List<String> roles, String user)
      throws NoSuchUserException, NoSuchRoleException, NoSuchMetalakeException {
    RoleGrantRequest request = new RoleGrantRequest(roles);
    UserResponse resp =
        restClient.put(
            String.format(API_PERMISSION_PATH, metalake, String.format("users/%s/grant", user)),
            request,
            UserResponse.class,
            Collections.emptyMap(),
            ErrorHandlers.permissionOperationErrorHandler());
    resp.validate();

    return resp.getUser();
  }

  /**
   * Grant roles to a group.
   *
   * @param metalake The metalake of the Group.
   * @param group The name of the Group.
   * @param roles The names of the Role.
   * @return The Group after granted.
   * @throws NoSuchGroupException If the Group with the given name does not exist.
   * @throws NoSuchRoleException If the Role with the given name does not exist.
   * @throws NoSuchMetalakeException If the Metalake with the given name does not exist.
   * @throws RuntimeException If granting roles to a group encounters storage issues.
   */
  public Group grantRolesToGroup(String metalake, List<String> roles, String group)
      throws NoSuchGroupException, NoSuchRoleException, NoSuchMetalakeException {
    RoleGrantRequest request = new RoleGrantRequest(roles);
    GroupResponse resp =
        restClient.put(
            String.format(API_PERMISSION_PATH, metalake, String.format("groups/%s/grant", group)),
            request,
            GroupResponse.class,
            Collections.emptyMap(),
            ErrorHandlers.permissionOperationErrorHandler());
    resp.validate();

    return resp.getGroup();
  }

  /**
   * Revoke roles from a user.
   *
   * @param metalake The metalake of the User.
   * @param user The name of the User.
   * @param roles The names of the Role.
   * @return The User after revoked.
   * @throws NoSuchUserException If the User with the given name does not exist.
   * @throws NoSuchRoleException If the Role with the given name does not exist.
   * @throws NoSuchMetalakeException If the Metalake with the given name does not exist.
   * @throws RuntimeException If revoking roles from a user encounters storage issues.
   */
  public User revokeRolesFromUser(String metalake, List<String> roles, String user)
      throws NoSuchUserException, NoSuchRoleException, NoSuchMetalakeException {
    RoleRevokeRequest request = new RoleRevokeRequest(roles);
    UserResponse resp =
        restClient.put(
            String.format(API_PERMISSION_PATH, metalake, String.format("users/%s/revoke", user)),
            request,
            UserResponse.class,
            Collections.emptyMap(),
            ErrorHandlers.permissionOperationErrorHandler());
    resp.validate();

    return resp.getUser();
  }

  /**
   * Revoke roles from a group.
   *
   * @param metalake The metalake of the Group.
   * @param group The name of the Group.
   * @param roles The names of the Role.
   * @return The Group after revoked.
   * @throws NoSuchGroupException If the Group with the given name does not exist.
   * @throws NoSuchRoleException If the Role with the given name does not exist.
   * @throws NoSuchMetalakeException If the Metalake with the given name does not exist.
   * @throws RuntimeException If revoking roles from a group encounters storage issues.
   */
  public Group revokeRolesFromGroup(String metalake, List<String> roles, String group)
      throws NoSuchGroupException, NoSuchRoleException, NoSuchMetalakeException {
    RoleRevokeRequest request = new RoleRevokeRequest(roles);
    GroupResponse resp =
        restClient.put(
            String.format(API_PERMISSION_PATH, metalake, String.format("groups/%s/revoke", group)),
            request,
            GroupResponse.class,
            Collections.emptyMap(),
            ErrorHandlers.permissionOperationErrorHandler());
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
      return new GravitinoAdminClient(uri, authDataProvider, checkVersion, headers);
    }
  }
}
