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

import com.google.common.base.Preconditions;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.CatalogChange;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.SupportsCatalogs;
import org.apache.gravitino.authorization.Group;
import org.apache.gravitino.authorization.Owner;
import org.apache.gravitino.authorization.Role;
import org.apache.gravitino.authorization.SecurableObject;
import org.apache.gravitino.authorization.User;
import org.apache.gravitino.exceptions.CatalogAlreadyExistsException;
import org.apache.gravitino.exceptions.GroupAlreadyExistsException;
import org.apache.gravitino.exceptions.NoSuchCatalogException;
import org.apache.gravitino.exceptions.NoSuchGroupException;
import org.apache.gravitino.exceptions.NoSuchMetadataObjectException;
import org.apache.gravitino.exceptions.NoSuchMetalakeException;
import org.apache.gravitino.exceptions.NoSuchRoleException;
import org.apache.gravitino.exceptions.NoSuchTagException;
import org.apache.gravitino.exceptions.NoSuchUserException;
import org.apache.gravitino.exceptions.NotFoundException;
import org.apache.gravitino.exceptions.RoleAlreadyExistsException;
import org.apache.gravitino.exceptions.TagAlreadyExistsException;
import org.apache.gravitino.exceptions.UserAlreadyExistsException;
import org.apache.gravitino.tag.Tag;
import org.apache.gravitino.tag.TagChange;
import org.apache.gravitino.tag.TagOperations;

/**
 * Apache Gravitino Client for a user to interact with the Gravitino API, allowing the client to
 * list, load, create, and alter Catalog.
 *
 * <p>It uses an underlying {@link RESTClient} to send HTTP requests and receive responses from the
 * API.
 */
public class GravitinoClient extends GravitinoClientBase
    implements SupportsCatalogs, TagOperations {

  private final GravitinoMetalake metalake;

  /**
   * Constructs a new GravitinoClient with the given URI, authenticator and AuthDataProvider.
   *
   * @param uri The base URI for the Gravitino API.
   * @param metalakeName The specified metalake name.
   * @param authDataProvider The provider of the data which is used for authentication.
   * @param checkVersion Whether to check the version of the Gravitino server. Gravitino does not
   *     support the case that the client-side version is higher than the server-side version.
   * @param headers The base header for Gravitino API.
   * @throws NoSuchMetalakeException if the metalake with specified name does not exist.
   */
  private GravitinoClient(
      String uri,
      String metalakeName,
      AuthDataProvider authDataProvider,
      boolean checkVersion,
      Map<String, String> headers) {
    super(uri, authDataProvider, checkVersion, headers);
    this.metalake = loadMetalake(metalakeName);
  }

  /**
   * Get the current metalake object
   *
   * @return the {@link GravitinoMetalake} object
   * @throws NoSuchMetalakeException if the metalake with specified name does not exist.
   */
  private GravitinoMetalake getMetalake() {
    return metalake;
  }

  @Override
  public String[] listCatalogs() throws NoSuchMetalakeException {
    return getMetalake().listCatalogs();
  }

  @Override
  public Catalog[] listCatalogsInfo() throws NoSuchMetalakeException {
    return getMetalake().listCatalogsInfo();
  }

  @Override
  public Catalog loadCatalog(String catalogName) throws NoSuchCatalogException {
    return getMetalake().loadCatalog(catalogName);
  }

  @Override
  public Catalog createCatalog(
      String catalogName,
      Catalog.Type type,
      String provider,
      String comment,
      Map<String, String> properties)
      throws NoSuchMetalakeException, CatalogAlreadyExistsException {
    return getMetalake().createCatalog(catalogName, type, provider, comment, properties);
  }

  @Override
  public Catalog alterCatalog(String catalogName, CatalogChange... changes)
      throws NoSuchCatalogException, IllegalArgumentException {
    return getMetalake().alterCatalog(catalogName, changes);
  }

  @Override
  public boolean dropCatalog(String catalogName) {
    return getMetalake().dropCatalog(catalogName);
  }

  /**
   * Adds a new User.
   *
   * @param user The name of the User.
   * @return The added User instance.
   * @throws UserAlreadyExistsException If a User with the same name already exists.
   * @throws NoSuchMetalakeException If the Metalake with the given name does not exist.
   * @throws RuntimeException If adding the User encounters storage issues.
   */
  public User addUser(String user) throws UserAlreadyExistsException, NoSuchMetalakeException {
    return getMetalake().addUser(user);
  }

  /**
   * Removes a User.
   *
   * @param user The name of the User.
   * @return True if the User was successfully removed, false only when there's no such user,
   *     otherwise it will throw an exception.
   * @throws NoSuchMetalakeException If the Metalake with the given name does not exist.
   * @throws RuntimeException If removing the User encounters storage issues.
   */
  public boolean removeUser(String user) throws NoSuchMetalakeException {
    return getMetalake().removeUser(user);
  }

  /**
   * Gets a User.
   *
   * @param user The name of the User.
   * @return The getting User instance.
   * @throws NoSuchUserException If the User with the given name does not exist.
   * @throws NoSuchMetalakeException If the Metalake with the given name does not exist.
   * @throws RuntimeException If getting the User encounters storage issues.
   */
  public User getUser(String user) throws NoSuchUserException, NoSuchMetalakeException {
    return getMetalake().getUser(user);
  }

  /**
   * Adds a new Group.
   *
   * @param group The name of the Group.
   * @return The Added Group instance.
   * @throws GroupAlreadyExistsException If a Group with the same name already exists.
   * @throws NoSuchMetalakeException If the Metalake with the given name does not exist.
   * @throws RuntimeException If adding the Group encounters storage issues.
   */
  public Group addGroup(String group) throws GroupAlreadyExistsException, NoSuchMetalakeException {
    return getMetalake().addGroup(group);
  }

  /**
   * Removes a Group.
   *
   * @param group THe name of the Group.
   * @return True if the Group was successfully removed, false only when there's no such group,
   *     otherwise it will throw an exception.
   * @throws NoSuchMetalakeException If the Metalake with the given name does not exist.
   * @throws RuntimeException If removing the Group encounters storage issues.
   */
  public boolean removeGroup(String group) throws NoSuchMetalakeException {
    return getMetalake().removeGroup(group);
  }

  /**
   * Gets a Group.
   *
   * @param group The name of the Group.
   * @return The getting Group instance.
   * @throws NoSuchGroupException If the Group with the given name does not exist.
   * @throws NoSuchMetalakeException If the Metalake with the given name does not exist.
   * @throws RuntimeException If getting the Group encounters storage issues.
   */
  public Group getGroup(String group) throws NoSuchGroupException, NoSuchMetalakeException {
    return getMetalake().getGroup(group);
  }

  /**
   * Gets a Role.
   *
   * @param role The name of the Role.
   * @return The getting Role instance.
   * @throws NoSuchRoleException If the Role with the given name does not exist.
   * @throws NoSuchMetalakeException If the Metalake with the given name does not exist.
   * @throws RuntimeException If getting the Role encounters storage issues.
   */
  public Role getRole(String role) throws NoSuchRoleException, NoSuchMetalakeException {
    return getMetalake().getRole(role);
  }

  /**
   * Deletes a Role.
   *
   * @param role The name of the Role.
   * @return True if the Role was successfully deleted, false only when there's no such role,
   *     otherwise it will throw an exception.
   * @throws NoSuchMetalakeException If the Metalake with the given name does not exist.
   * @throws RuntimeException If deleting the Role encounters storage issues.
   */
  public boolean deleteRole(String role) throws NoSuchMetalakeException {
    return getMetalake().deleteRole(role);
  }

  /**
   * Creates a new Role.
   *
   * @param role The name of the Role.
   * @param properties The properties of the Role.
   * @param securableObjects The securable objects of the Role.
   * @return The created Role instance.
   * @throws RoleAlreadyExistsException If a Role with the same name already exists.
   * @throws NoSuchMetalakeException If the Metalake with the given name does not exist.
   * @throws RuntimeException If creating the Role encounters storage issues.
   */
  public Role createRole(
      String role, Map<String, String> properties, List<SecurableObject> securableObjects)
      throws RoleAlreadyExistsException, NoSuchMetalakeException {
    return getMetalake().createRole(role, properties, securableObjects);
  }
  /**
   * Grant roles to a user.
   *
   * @param user The name of the User.
   * @param roles The names of the Role.
   * @return The Group after granted.
   * @throws NoSuchUserException If the User with the given name does not exist.
   * @throws NoSuchRoleException If the Role with the given name does not exist.
   * @throws NoSuchMetalakeException If the Metalake with the given name does not exist.
   * @throws RuntimeException If granting roles to a user encounters storage issues.
   */
  public User grantRolesToUser(List<String> roles, String user)
      throws NoSuchUserException, NoSuchRoleException, NoSuchMetalakeException {
    return getMetalake().grantRolesToUser(roles, user);
  }

  /**
   * Grant roles to a group.
   *
   * @param group The name of the Group.
   * @param roles The names of the Role.
   * @return The Group after granted.
   * @throws NoSuchGroupException If the Group with the given name does not exist.
   * @throws NoSuchRoleException If the Role with the given name does not exist.
   * @throws NoSuchMetalakeException If the Metalake with the given name does not exist.
   * @throws RuntimeException If granting roles to a group encounters storage issues.
   */
  public Group grantRolesToGroup(List<String> roles, String group)
      throws NoSuchGroupException, NoSuchRoleException, NoSuchMetalakeException {
    return getMetalake().grantRolesToGroup(roles, group);
  }

  /**
   * Revoke roles from a user.
   *
   * @param user The name of the User.
   * @param roles The names of the Role.
   * @return The User after revoked.
   * @throws NoSuchUserException If the User with the given name does not exist.
   * @throws NoSuchRoleException If the Role with the given name does not exist.
   * @throws NoSuchMetalakeException If the Metalake with the given name does not exist.
   * @throws RuntimeException If revoking roles from a user encounters storage issues.
   */
  public User revokeRolesFromUser(List<String> roles, String user)
      throws NoSuchUserException, NoSuchRoleException, NoSuchMetalakeException {
    return getMetalake().revokeRolesFromUser(roles, user);
  }

  /**
   * Revoke roles from a group.
   *
   * @param group The name of the Group.
   * @param roles The names of the Role.
   * @return The Group after revoked.
   * @throws NoSuchGroupException If the Group with the given name does not exist.
   * @throws NoSuchRoleException If the Role with the given name does not exist.
   * @throws NoSuchMetalakeException If the Metalake with the given name does not exist.
   * @throws RuntimeException If revoking roles from a group encounters storage issues.
   */
  public Group revokeRolesFromGroup(List<String> roles, String group)
      throws NoSuchGroupException, NoSuchRoleException, NoSuchMetalakeException {
    return getMetalake().revokeRolesFromGroup(roles, group);
  }

  /**
   * Get the owner of a metadata object.
   *
   * @param object The metadata object
   * @return The owner of the metadata object. If the metadata object doesn't set the owner, it will
   *     return Optional.empty().
   * @throws NoSuchMetadataObjectException If the metadata object is not found.
   */
  public Optional<Owner> getOwner(MetadataObject object) throws NoSuchMetadataObjectException {
    return getMetalake().getOwner(object);
  }

  /**
   * Set the owner of a metadata object.
   *
   * @param object The metadata object.
   * @param ownerName The name of the owner
   * @param ownerType The type of the owner, The owner can be a user or a group.
   * @throws NotFoundException If the metadata object isn't found or the owner doesn't exist.
   */
  public void setOwner(MetadataObject object, String ownerName, Owner.Type ownerType)
      throws NotFoundException {
    getMetalake().setOwner(object, ownerName, ownerType);
  }

  /**
   * Creates a new builder for constructing a GravitinoClient.
   *
   * @param uri The base URI for the Gravitino API.
   * @return A new instance of the Builder class for constructing a GravitinoClient.
   */
  public static ClientBuilder builder(String uri) {
    return new ClientBuilder(uri);
  }

  /**
   * Test whether a catalog can be created successfully with the specified parameters, without
   * actually creating it.
   *
   * @param catalogName the name of the catalog.
   * @param type the type of the catalog.
   * @param provider the provider of the catalog.
   * @param comment the comment of the catalog.
   * @param properties the properties of the catalog.
   * @throws Exception if the test failed.
   */
  @Override
  public void testConnection(
      String catalogName,
      Catalog.Type type,
      String provider,
      String comment,
      Map<String, String> properties)
      throws Exception {
    getMetalake().testConnection(catalogName, type, provider, comment, properties);
  }

  @Override
  public String[] listTags() throws NoSuchMetalakeException {
    return getMetalake().listTags();
  }

  @Override
  public Tag[] listTagsInfo() throws NoSuchMetalakeException {
    return getMetalake().listTagsInfo();
  }

  @Override
  public Tag getTag(String name) throws NoSuchTagException {
    return getMetalake().getTag(name);
  }

  @Override
  public Tag createTag(String name, String comment, Map<String, String> properties)
      throws TagAlreadyExistsException {
    return getMetalake().createTag(name, comment, properties);
  }

  @Override
  public Tag alterTag(String name, TagChange... changes)
      throws NoSuchTagException, IllegalArgumentException {
    return getMetalake().alterTag(name, changes);
  }

  @Override
  public boolean deleteTag(String name) {
    return getMetalake().deleteTag(name);
  }

  /** Builder class for constructing a GravitinoClient. */
  public static class ClientBuilder extends GravitinoClientBase.Builder<GravitinoClient> {

    /** The name of the metalake that the client is working on. */
    protected String metalakeName;

    /**
     * The private constructor for the Builder class.
     *
     * @param uri The base URI for the Gravitino API.
     */
    protected ClientBuilder(String uri) {
      super(uri);
    }

    /**
     * Optional, set the metalake name for this client.
     *
     * @param metalakeName The name of the metalake that the client is working on.
     * @return This Builder instance for method chaining.
     */
    public ClientBuilder withMetalake(String metalakeName) {
      this.metalakeName = metalakeName;
      return this;
    }

    /**
     * Builds a new GravitinoClient instance.
     *
     * @return A new instance of GravitinoClient with the specified base URI.
     * @throws IllegalArgumentException If the base URI is null or empty.
     * @throws NoSuchMetalakeException if the metalake with specified name does not exist.
     */
    @Override
    public GravitinoClient build() {
      Preconditions.checkArgument(
          uri != null && !uri.isEmpty(), "The argument 'uri' must be a valid URI");
      Preconditions.checkArgument(
          metalakeName != null && !metalakeName.isEmpty(),
          "The argument 'metalakeName' must be a valid name");

      return new GravitinoClient(uri, metalakeName, authDataProvider, checkVersion, headers);
    }
  }
}
