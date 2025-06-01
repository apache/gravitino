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
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.CatalogChange;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.MetadataObjects;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.SupportsCatalogs;
import org.apache.gravitino.authorization.Group;
import org.apache.gravitino.authorization.Owner;
import org.apache.gravitino.authorization.Privilege;
import org.apache.gravitino.authorization.Role;
import org.apache.gravitino.authorization.SecurableObject;
import org.apache.gravitino.authorization.SupportsRoles;
import org.apache.gravitino.authorization.User;
import org.apache.gravitino.dto.AuditDTO;
import org.apache.gravitino.dto.MetalakeDTO;
import org.apache.gravitino.dto.authorization.SecurableObjectDTO;
import org.apache.gravitino.dto.requests.CatalogCreateRequest;
import org.apache.gravitino.dto.requests.CatalogSetRequest;
import org.apache.gravitino.dto.requests.CatalogUpdateRequest;
import org.apache.gravitino.dto.requests.CatalogUpdatesRequest;
import org.apache.gravitino.dto.requests.GroupAddRequest;
import org.apache.gravitino.dto.requests.OwnerSetRequest;
import org.apache.gravitino.dto.requests.PrivilegeGrantRequest;
import org.apache.gravitino.dto.requests.PrivilegeRevokeRequest;
import org.apache.gravitino.dto.requests.RoleCreateRequest;
import org.apache.gravitino.dto.requests.RoleGrantRequest;
import org.apache.gravitino.dto.requests.RoleRevokeRequest;
import org.apache.gravitino.dto.requests.TagCreateRequest;
import org.apache.gravitino.dto.requests.TagUpdateRequest;
import org.apache.gravitino.dto.requests.TagUpdatesRequest;
import org.apache.gravitino.dto.requests.UserAddRequest;
import org.apache.gravitino.dto.responses.CatalogListResponse;
import org.apache.gravitino.dto.responses.CatalogResponse;
import org.apache.gravitino.dto.responses.DeleteResponse;
import org.apache.gravitino.dto.responses.DropResponse;
import org.apache.gravitino.dto.responses.EntityListResponse;
import org.apache.gravitino.dto.responses.ErrorResponse;
import org.apache.gravitino.dto.responses.GroupListResponse;
import org.apache.gravitino.dto.responses.GroupResponse;
import org.apache.gravitino.dto.responses.NameListResponse;
import org.apache.gravitino.dto.responses.OwnerResponse;
import org.apache.gravitino.dto.responses.RemoveResponse;
import org.apache.gravitino.dto.responses.RoleResponse;
import org.apache.gravitino.dto.responses.SetResponse;
import org.apache.gravitino.dto.responses.TagListResponse;
import org.apache.gravitino.dto.responses.TagResponse;
import org.apache.gravitino.dto.responses.UserListResponse;
import org.apache.gravitino.dto.responses.UserResponse;
import org.apache.gravitino.exceptions.CatalogAlreadyExistsException;
import org.apache.gravitino.exceptions.CatalogInUseException;
import org.apache.gravitino.exceptions.GroupAlreadyExistsException;
import org.apache.gravitino.exceptions.IllegalMetadataObjectException;
import org.apache.gravitino.exceptions.IllegalPrivilegeException;
import org.apache.gravitino.exceptions.IllegalRoleException;
import org.apache.gravitino.exceptions.NoSuchCatalogException;
import org.apache.gravitino.exceptions.NoSuchGroupException;
import org.apache.gravitino.exceptions.NoSuchMetadataObjectException;
import org.apache.gravitino.exceptions.NoSuchMetalakeException;
import org.apache.gravitino.exceptions.NoSuchRoleException;
import org.apache.gravitino.exceptions.NoSuchTagException;
import org.apache.gravitino.exceptions.NoSuchUserException;
import org.apache.gravitino.exceptions.NonEmptyEntityException;
import org.apache.gravitino.exceptions.NotFoundException;
import org.apache.gravitino.exceptions.RoleAlreadyExistsException;
import org.apache.gravitino.exceptions.TagAlreadyExistsException;
import org.apache.gravitino.exceptions.UserAlreadyExistsException;
import org.apache.gravitino.rest.RESTUtils;
import org.apache.gravitino.tag.Tag;
import org.apache.gravitino.tag.TagChange;
import org.apache.gravitino.tag.TagOperations;

/**
 * Apache Gravitino Metalake is the top-level metadata repository for users. It contains a list of
 * catalogs as sub-level metadata collections. With {@link GravitinoMetalake}, users can list,
 * create, load, alter and drop a catalog with specified identifier.
 */
public class GravitinoMetalake extends MetalakeDTO
    implements SupportsCatalogs, TagOperations, SupportsRoles {
  private static final String API_METALAKES_CATALOGS_PATH = "api/metalakes/%s/catalogs/%s";
  private static final String API_PERMISSION_PATH = "api/metalakes/%s/permissions/%s";
  private static final String API_METALAKES_USERS_PATH = "api/metalakes/%s/users/%s";
  private static final String API_METALAKES_GROUPS_PATH = "api/metalakes/%s/groups/%s";
  private static final String API_METALAKES_ROLES_PATH = "api/metalakes/%s/roles/%s";
  private static final String API_METALAKES_OWNERS_PATH = "api/metalakes/%s/owners/%s";

  private static final String API_METALAKES_TAGS_PATH = "api/metalakes/%s/tags";
  private static final String BLANK_PLACEHOLDER = "";

  private final RESTClient restClient;
  private final MetadataObjectRoleOperations metadataObjectRoleOperations;

  GravitinoMetalake(
      String name,
      String comment,
      Map<String, String> properties,
      AuditDTO auditDTO,
      RESTClient restClient) {
    super(name, comment, properties, auditDTO);
    this.restClient = restClient;
    this.metadataObjectRoleOperations =
        new MetadataObjectRoleOperations(
            name, MetadataObjects.of(null, name, MetadataObject.Type.METALAKE), restClient);
  }

  /**
   * List all the catalogs under this metalake.
   *
   * @return A list of the catalog names under the current metalake.
   * @throws NoSuchMetalakeException If the metalake does not exist.
   */
  @Override
  public String[] listCatalogs() throws NoSuchMetalakeException {

    EntityListResponse resp =
        restClient.get(
            String.format("api/metalakes/%s/catalogs", this.name()),
            EntityListResponse.class,
            Collections.emptyMap(),
            ErrorHandlers.catalogErrorHandler());
    resp.validate();

    return Arrays.stream(resp.identifiers()).map(NameIdentifier::name).toArray(String[]::new);
  }

  /**
   * List all the catalogs with their information under this metalake.
   *
   * @return A list of {@link Catalog} under the specified namespace.
   * @throws NoSuchMetalakeException if the metalake with specified namespace does not exist.
   */
  @Override
  public Catalog[] listCatalogsInfo() throws NoSuchMetalakeException {

    Map<String, String> params = new HashMap<>();
    params.put("details", "true");
    CatalogListResponse resp =
        restClient.get(
            String.format("api/metalakes/%s/catalogs", this.name()),
            params,
            CatalogListResponse.class,
            Collections.emptyMap(),
            ErrorHandlers.catalogErrorHandler());

    return Arrays.stream(resp.getCatalogs())
        .map(c -> DTOConverters.toCatalog(this.name(), c, restClient))
        .toArray(Catalog[]::new);
  }

  /**
   * Load the catalog with specified identifier.
   *
   * @param catalogName The identifier of the catalog to load.
   * @return The {@link Catalog} with specified identifier.
   * @throws NoSuchCatalogException if the catalog with specified identifier does not exist.
   */
  @Override
  public Catalog loadCatalog(String catalogName) throws NoSuchCatalogException {

    CatalogResponse resp =
        restClient.get(
            String.format(API_METALAKES_CATALOGS_PATH, this.name(), catalogName),
            CatalogResponse.class,
            Collections.emptyMap(),
            ErrorHandlers.catalogErrorHandler());
    resp.validate();

    return DTOConverters.toCatalog(this.name(), resp.getCatalog(), restClient);
  }

  /**
   * Create a new catalog with specified identifier, type, comment and properties.
   *
   * @param catalogName The identifier of the catalog.
   * @param type The type of the catalog.
   * @param provider The provider of the catalog.
   * @param comment The comment of the catalog.
   * @param properties The properties of the catalog.
   * @return The created {@link Catalog}.
   * @throws NoSuchMetalakeException if the metalake with specified namespace does not exist.
   * @throws CatalogAlreadyExistsException if the catalog with specified identifier already exists.
   */
  @Override
  public Catalog createCatalog(
      String catalogName,
      Catalog.Type type,
      String provider,
      String comment,
      Map<String, String> properties)
      throws NoSuchMetalakeException, CatalogAlreadyExistsException {
    CatalogCreateRequest req =
        new CatalogCreateRequest(catalogName, type, provider, comment, properties);
    req.validate();

    CatalogResponse resp =
        restClient.post(
            String.format("api/metalakes/%s/catalogs", this.name()),
            req,
            CatalogResponse.class,
            Collections.emptyMap(),
            ErrorHandlers.catalogErrorHandler());
    resp.validate();

    return DTOConverters.toCatalog(this.name(), resp.getCatalog(), restClient);
  }

  /**
   * Alter the catalog with specified identifier by applying the changes.
   *
   * @param catalogName the identifier of the catalog.
   * @param changes the changes to apply to the catalog.
   * @return the altered {@link Catalog}.
   * @throws NoSuchCatalogException if the catalog with specified identifier does not exist.
   * @throws IllegalArgumentException if the changes are invalid.
   */
  @Override
  public Catalog alterCatalog(String catalogName, CatalogChange... changes)
      throws NoSuchCatalogException, IllegalArgumentException {
    List<CatalogUpdateRequest> reqs =
        Arrays.stream(changes)
            .map(DTOConverters::toCatalogUpdateRequest)
            .collect(Collectors.toList());
    CatalogUpdatesRequest updatesRequest = new CatalogUpdatesRequest(reqs);
    updatesRequest.validate();

    CatalogResponse resp =
        restClient.put(
            String.format(
                API_METALAKES_CATALOGS_PATH,
                RESTUtils.encodeString(this.name()),
                RESTUtils.encodeString(catalogName)),
            updatesRequest,
            CatalogResponse.class,
            Collections.emptyMap(),
            ErrorHandlers.catalogErrorHandler());
    resp.validate();

    return DTOConverters.toCatalog(this.name(), resp.getCatalog(), restClient);
  }

  /**
   * Drop a catalog with specified name. If the force flag is true, it will:
   *
   * <ul>
   *   <li>Cascade drop all sub-entities (schemas, tables, etc.) of the catalog in Gravitino store.
   *   <li>Drop the catalog even if it is in use.
   *   <li>External resources (e.g. database, table, etc.) associated with sub-entities will not be
   *       dropped unless it is managed (such as managed fileset).
   * </ul>
   *
   * @param catalogName The identifier of the catalog.
   * @param force Whether to force the drop.
   * @return True if the catalog was dropped, false if the catalog does not exist.
   * @throws NonEmptyEntityException If the catalog is not empty and force is false.
   * @throws CatalogInUseException If the catalog is in use and force is false.
   */
  @Override
  public boolean dropCatalog(String catalogName, boolean force)
      throws NonEmptyEntityException, CatalogInUseException {
    Map<String, String> params = new HashMap<>();
    params.put("force", String.valueOf(force));

    DropResponse resp =
        restClient.delete(
            String.format(
                API_METALAKES_CATALOGS_PATH,
                RESTUtils.encodeString(this.name()),
                RESTUtils.encodeString(catalogName)),
            params,
            DropResponse.class,
            Collections.emptyMap(),
            ErrorHandlers.catalogErrorHandler());
    resp.validate();
    return resp.dropped();
  }

  @Override
  public void enableCatalog(String catalogName) throws NoSuchCatalogException {
    CatalogSetRequest req = new CatalogSetRequest(true);

    ErrorResponse resp =
        restClient.patch(
            String.format(
                API_METALAKES_CATALOGS_PATH,
                RESTUtils.encodeString(this.name()),
                RESTUtils.encodeString(catalogName)),
            req,
            ErrorResponse.class,
            Collections.emptyMap(),
            ErrorHandlers.catalogErrorHandler());

    if (resp.getCode() == 0) {
      return;
    }

    ErrorHandlers.catalogErrorHandler().accept(resp);
  }

  @Override
  public void disableCatalog(String catalogName) throws NoSuchCatalogException {
    CatalogSetRequest req = new CatalogSetRequest(false);

    ErrorResponse resp =
        restClient.patch(
            String.format(
                API_METALAKES_CATALOGS_PATH,
                RESTUtils.encodeString(this.name()),
                RESTUtils.encodeString(catalogName)),
            req,
            ErrorResponse.class,
            Collections.emptyMap(),
            ErrorHandlers.catalogErrorHandler());

    if (resp.getCode() == 0) {
      return;
    }

    ErrorHandlers.catalogErrorHandler().accept(resp);
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
    CatalogCreateRequest req =
        new CatalogCreateRequest(catalogName, type, provider, comment, properties);
    req.validate();

    // The response maybe a `BaseResponse` (test successfully)  or an `ErrorResponse` (test failed),
    // we use the `ErrorResponse` here because it contains all fields of `BaseResponse` (code field
    // only)
    ErrorResponse resp =
        restClient.post(
            String.format(
                "api/metalakes/%s/catalogs/testConnection", RESTUtils.encodeString(this.name())),
            req,
            ErrorResponse.class,
            Collections.emptyMap(),
            ErrorHandlers.catalogErrorHandler());

    if (resp.getCode() == 0) {
      return;
    }

    // Throw the corresponding exception
    ErrorHandlers.catalogErrorHandler().accept(resp);
  }

  @Override
  public SupportsRoles supportsRoles() {
    return this;
  }

  /**
   * List all the tag names under a metalake.
   *
   * @return A list of the tag names under the current metalake.
   * @throws NoSuchMetalakeException If the metalake does not exist.
   */
  @Override
  public String[] listTags() throws NoSuchMetalakeException {
    NameListResponse resp =
        restClient.get(
            String.format(API_METALAKES_TAGS_PATH, RESTUtils.encodeString(this.name())),
            NameListResponse.class,
            Collections.emptyMap(),
            ErrorHandlers.tagErrorHandler());
    resp.validate();
    return resp.getNames();
  }

  /**
   * List all the tags with detailed information under the current metalake.
   *
   * @return A list of {@link Tag} under the current metalake.
   * @throws NoSuchMetalakeException If the metalake does not exist.
   */
  @Override
  public Tag[] listTagsInfo() throws NoSuchMetalakeException {
    Map<String, String> params = ImmutableMap.of("details", "true");
    TagListResponse resp =
        restClient.get(
            String.format(API_METALAKES_TAGS_PATH, RESTUtils.encodeString(this.name())),
            params,
            TagListResponse.class,
            Collections.emptyMap(),
            ErrorHandlers.tagErrorHandler());
    resp.validate();

    return Arrays.stream(resp.getTags())
        .map(t -> new GenericTag(t, restClient, this.name()))
        .toArray(Tag[]::new);
  }

  /**
   * Get a tag by its name under the current metalake.
   *
   * @param name The name of the tag.
   * @return The tag.
   * @throws NoSuchTagException If the tag does not exist.
   */
  @Override
  public Tag getTag(String name) throws NoSuchTagException {
    Preconditions.checkArgument(StringUtils.isNotBlank(name), "tag name must not be null or empty");

    TagResponse resp =
        restClient.get(
            String.format(API_METALAKES_TAGS_PATH, RESTUtils.encodeString(this.name()))
                + "/"
                + RESTUtils.encodeString(name),
            TagResponse.class,
            Collections.emptyMap(),
            ErrorHandlers.tagErrorHandler());
    resp.validate();

    return new GenericTag(resp.getTag(), restClient, this.name());
  }

  /**
   * Create a tag under the current metalake.
   *
   * @param name The name of the tag.
   * @param comment The comment of the tag.
   * @param properties The properties of the tag.
   * @return The created tag.
   * @throws TagAlreadyExistsException If the tag already exists.
   */
  @Override
  public Tag createTag(String name, String comment, Map<String, String> properties)
      throws TagAlreadyExistsException {
    Preconditions.checkArgument(StringUtils.isNotBlank(name), "tag name must not be null or empty");
    TagCreateRequest req = new TagCreateRequest(name, comment, properties);
    req.validate();

    TagResponse resp =
        restClient.post(
            String.format(API_METALAKES_TAGS_PATH, RESTUtils.encodeString(this.name())),
            req,
            TagResponse.class,
            Collections.emptyMap(),
            ErrorHandlers.tagErrorHandler());
    resp.validate();

    return new GenericTag(resp.getTag(), restClient, this.name());
  }

  /**
   * Alter a tag under the current metalake.
   *
   * @param name The name of the tag.
   * @param changes The changes to apply to the tag.
   * @return The altered tag.
   * @throws NoSuchTagException If the tag does not exist.
   * @throws IllegalArgumentException If the changes cannot be applied to the tag.
   */
  @Override
  public Tag alterTag(String name, TagChange... changes)
      throws NoSuchTagException, IllegalArgumentException {
    Preconditions.checkArgument(StringUtils.isNotBlank(name), "tag name must not be null or empty");
    List<TagUpdateRequest> updates =
        Arrays.stream(changes).map(DTOConverters::toTagUpdateRequest).collect(Collectors.toList());
    TagUpdatesRequest req = new TagUpdatesRequest(updates);
    req.validate();

    TagResponse resp =
        restClient.put(
            String.format(API_METALAKES_TAGS_PATH, RESTUtils.encodeString(this.name()))
                + "/"
                + RESTUtils.encodeString(name),
            req,
            TagResponse.class,
            Collections.emptyMap(),
            ErrorHandlers.tagErrorHandler());
    resp.validate();

    return new GenericTag(resp.getTag(), restClient, this.name());
  }

  /**
   * Delete a tag under the current metalake.
   *
   * @param name The name of the tag.
   * @return True if the tag is deleted, false if the tag does not exist.
   */
  @Override
  public boolean deleteTag(String name) {
    Preconditions.checkArgument(StringUtils.isNotBlank(name), "tag name must not be null or empty");

    DropResponse resp =
        restClient.delete(
            String.format(API_METALAKES_TAGS_PATH, RESTUtils.encodeString(this.name()))
                + "/"
                + RESTUtils.encodeString(name),
            DropResponse.class,
            Collections.emptyMap(),
            ErrorHandlers.tagErrorHandler());
    resp.validate();
    return resp.dropped();
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
    UserAddRequest req = new UserAddRequest(user);
    req.validate();

    UserResponse resp =
        restClient.post(
            String.format(
                API_METALAKES_USERS_PATH, RESTUtils.encodeString(this.name()), BLANK_PLACEHOLDER),
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
   * @param user The name of the User.
   * @return True if the User was successfully removed, false only when there's no such user,
   *     otherwise it will throw an exception.
   * @throws NoSuchMetalakeException If the Metalake with the given name does not exist.
   * @throws RuntimeException If removing the User encounters storage issues.
   */
  public boolean removeUser(String user) throws NoSuchMetalakeException {
    RemoveResponse resp =
        restClient.delete(
            String.format(
                API_METALAKES_USERS_PATH,
                RESTUtils.encodeString(this.name()),
                RESTUtils.encodeString(user)),
            RemoveResponse.class,
            Collections.emptyMap(),
            ErrorHandlers.userErrorHandler());
    resp.validate();

    return resp.removed();
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
    UserResponse resp =
        restClient.get(
            String.format(
                API_METALAKES_USERS_PATH,
                RESTUtils.encodeString(this.name()),
                RESTUtils.encodeString(user)),
            UserResponse.class,
            Collections.emptyMap(),
            ErrorHandlers.userErrorHandler());
    resp.validate();

    return resp.getUser();
  }

  /**
   * Lists the users.
   *
   * @return The User list.
   * @throws NoSuchMetalakeException If the Metalake with the given name does not exist.
   */
  public User[] listUsers() throws NoSuchMetalakeException {
    Map<String, String> params = new HashMap<>();
    params.put("details", "true");

    UserListResponse resp =
        restClient.get(
            String.format(
                API_METALAKES_USERS_PATH, RESTUtils.encodeString(this.name()), BLANK_PLACEHOLDER),
            params,
            UserListResponse.class,
            Collections.emptyMap(),
            ErrorHandlers.userErrorHandler());
    resp.validate();

    return resp.getUsers();
  }

  /**
   * Lists the usernames.
   *
   * @return The username list.
   * @throws NoSuchMetalakeException If the Metalake with the given name does not exist.
   */
  public String[] listUserNames() throws NoSuchMetalakeException {
    NameListResponse resp =
        restClient.get(
            String.format(
                API_METALAKES_USERS_PATH, RESTUtils.encodeString(this.name()), BLANK_PLACEHOLDER),
            NameListResponse.class,
            Collections.emptyMap(),
            ErrorHandlers.userErrorHandler());
    resp.validate();

    return resp.getNames();
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
    GroupAddRequest req = new GroupAddRequest(group);
    req.validate();

    GroupResponse resp =
        restClient.post(
            String.format(
                API_METALAKES_GROUPS_PATH, RESTUtils.encodeString(this.name()), BLANK_PLACEHOLDER),
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
   * @param group THe name of the Group.
   * @return True if the Group was successfully removed, false only when there's no such group,
   *     otherwise it will throw an exception.
   * @throws NoSuchMetalakeException If the Metalake with the given name does not exist.
   * @throws RuntimeException If removing the Group encounters storage issues.
   */
  public boolean removeGroup(String group) throws NoSuchMetalakeException {
    RemoveResponse resp =
        restClient.delete(
            String.format(
                API_METALAKES_GROUPS_PATH,
                RESTUtils.encodeString(this.name()),
                RESTUtils.encodeString(group)),
            RemoveResponse.class,
            Collections.emptyMap(),
            ErrorHandlers.groupErrorHandler());
    resp.validate();

    return resp.removed();
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
    GroupResponse resp =
        restClient.get(
            String.format(
                API_METALAKES_GROUPS_PATH,
                RESTUtils.encodeString(this.name()),
                RESTUtils.encodeString(group)),
            GroupResponse.class,
            Collections.emptyMap(),
            ErrorHandlers.groupErrorHandler());
    resp.validate();

    return resp.getGroup();
  }

  /**
   * Lists the groups
   *
   * @return The Group list
   * @throws NoSuchMetalakeException If the Metalake with the given name does not exist.
   */
  public Group[] listGroups() throws NoSuchMetalakeException {
    Map<String, String> params = new HashMap<>();
    params.put("details", "true");

    GroupListResponse resp =
        restClient.get(
            String.format(
                API_METALAKES_GROUPS_PATH, RESTUtils.encodeString(this.name()), BLANK_PLACEHOLDER),
            params,
            GroupListResponse.class,
            Collections.emptyMap(),
            ErrorHandlers.groupErrorHandler());
    resp.validate();
    return resp.getGroups();
  }

  /**
   * Lists the group names
   *
   * @return The Group Name List
   * @throws NoSuchMetalakeException If the Metalake with the given name does not exist.
   */
  public String[] listGroupNames() throws NoSuchMetalakeException {
    NameListResponse resp =
        restClient.get(
            String.format(
                API_METALAKES_GROUPS_PATH, RESTUtils.encodeString(this.name()), BLANK_PLACEHOLDER),
            NameListResponse.class,
            Collections.emptyMap(),
            ErrorHandlers.groupErrorHandler());
    resp.validate();
    return resp.getNames();
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
    RoleResponse resp =
        restClient.get(
            String.format(
                API_METALAKES_ROLES_PATH,
                RESTUtils.encodeString(this.name()),
                RESTUtils.encodeString(role)),
            RoleResponse.class,
            Collections.emptyMap(),
            ErrorHandlers.roleErrorHandler());
    resp.validate();

    return resp.getRole();
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
    DeleteResponse resp =
        restClient.delete(
            String.format(
                API_METALAKES_ROLES_PATH,
                RESTUtils.encodeString(this.name()),
                RESTUtils.encodeString(role)),
            DeleteResponse.class,
            Collections.emptyMap(),
            ErrorHandlers.roleErrorHandler());
    resp.validate();

    return resp.deleted();
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
   * @throws IllegalMetadataObjectException If the securable object is invalid
   * @throws RuntimeException If creating the Role encounters storage issues.
   */
  public Role createRole(
      String role, Map<String, String> properties, List<SecurableObject> securableObjects)
      throws RoleAlreadyExistsException, NoSuchMetalakeException, IllegalMetadataObjectException {
    RoleCreateRequest req =
        new RoleCreateRequest(
            role,
            properties,
            securableObjects.stream()
                .map(DTOConverters::toSecurableObject)
                .toArray(SecurableObjectDTO[]::new));
    req.validate();

    RoleResponse resp =
        restClient.post(
            String.format(
                API_METALAKES_ROLES_PATH, RESTUtils.encodeString(this.name()), BLANK_PLACEHOLDER),
            req,
            RoleResponse.class,
            Collections.emptyMap(),
            ErrorHandlers.roleErrorHandler());
    resp.validate();

    return resp.getRole();
  }

  /**
   * Lists the role names.
   *
   * @return The role name list.
   * @throws NoSuchMetalakeException If the Metalake with the given name does not exist.
   */
  public String[] listRoleNames() {
    NameListResponse resp =
        restClient.get(
            String.format(
                API_METALAKES_ROLES_PATH, RESTUtils.encodeString(this.name()), BLANK_PLACEHOLDER),
            NameListResponse.class,
            Collections.emptyMap(),
            ErrorHandlers.roleErrorHandler());
    resp.validate();

    return resp.getNames();
  }

  /**
   * Grant roles to a user.
   *
   * @param user The name of the User.
   * @param roles The names of the Role.
   * @return The Group after granted.
   * @throws NoSuchUserException If the User with the given name does not exist.
   * @throws IllegalRoleException If the Role with the given name is invalid.
   * @throws NoSuchMetalakeException If the Metalake with the given name does not exist.
   * @throws RuntimeException If granting roles to a user encounters storage issues.
   */
  public User grantRolesToUser(List<String> roles, String user)
      throws NoSuchUserException, IllegalRoleException, NoSuchMetalakeException {
    RoleGrantRequest request = new RoleGrantRequest(roles);
    request.validate();

    UserResponse resp =
        restClient.put(
            String.format(
                API_PERMISSION_PATH,
                RESTUtils.encodeString(this.name()),
                String.format("users/%s/grant", RESTUtils.encodeString(user))),
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
   * @param group The name of the Group.
   * @param roles The names of the Role.
   * @return The Group after granted.
   * @throws NoSuchGroupException If the Group with the given name does not exist.
   * @throws IllegalRoleException If the Role with the given name is invalid.
   * @throws NoSuchMetalakeException If the Metalake with the given name does not exist.
   * @throws RuntimeException If granting roles to a group encounters storage issues.
   */
  public Group grantRolesToGroup(List<String> roles, String group)
      throws NoSuchGroupException, NoSuchRoleException, NoSuchMetalakeException {
    RoleGrantRequest request = new RoleGrantRequest(roles);
    request.validate();

    GroupResponse resp =
        restClient.put(
            String.format(
                API_PERMISSION_PATH,
                RESTUtils.encodeString(this.name()),
                String.format("groups/%s/grant", RESTUtils.encodeString(group))),
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
   * @param user The name of the User.
   * @param roles The names of the Role.
   * @return The User after revoked.
   * @throws NoSuchUserException If the User with the given name does not exist.
   * @throws IllegalRoleException If the Role with the given name is invalid.
   * @throws NoSuchMetalakeException If the Metalake with the given name does not exist.
   * @throws RuntimeException If revoking roles from a user encounters storage issues.
   */
  public User revokeRolesFromUser(List<String> roles, String user)
      throws NoSuchUserException, NoSuchRoleException, NoSuchMetalakeException {
    RoleRevokeRequest request = new RoleRevokeRequest(roles);
    request.validate();

    UserResponse resp =
        restClient.put(
            String.format(
                API_PERMISSION_PATH,
                RESTUtils.encodeString(this.name()),
                String.format("users/%s/revoke", RESTUtils.encodeString(user))),
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
   * @param group The name of the Group.
   * @param roles The names of the Role.
   * @return The Group after revoked.
   * @throws NoSuchGroupException If the Group with the given name does not exist.
   * @throws IllegalRoleException If the Role with the given name is invalid.
   * @throws NoSuchMetalakeException If the Metalake with the given name does not exist.
   * @throws RuntimeException If revoking roles from a group encounters storage issues.
   */
  public Group revokeRolesFromGroup(List<String> roles, String group)
      throws NoSuchGroupException, IllegalRoleException, NoSuchMetalakeException {
    RoleRevokeRequest request = new RoleRevokeRequest(roles);
    request.validate();

    GroupResponse resp =
        restClient.put(
            String.format(
                API_PERMISSION_PATH,
                RESTUtils.encodeString(this.name()),
                String.format("groups/%s/revoke", RESTUtils.encodeString(group))),
            request,
            GroupResponse.class,
            Collections.emptyMap(),
            ErrorHandlers.permissionOperationErrorHandler());
    resp.validate();

    return resp.getGroup();
  }

  /**
   * Grant privileges to a role.
   *
   * @param role The name of the role.
   * @param privileges The privileges to grant.
   * @param object The object is associated with privileges to grant.
   * @return The role after granted.
   * @throws NoSuchRoleException If the role with the given name does not exist.
   * @throws NoSuchMetadataObjectException If the metadata object with the given name does not
   *     exist.
   * @throws NoSuchMetalakeException If the Metalake with the given name does not exist.
   * @throws IllegalPrivilegeException If any privilege can't be bind to the metadata object.
   * @throws RuntimeException If granting privileges to a role encounters storage issues.
   */
  public Role grantPrivilegesToRole(String role, MetadataObject object, List<Privilege> privileges)
      throws NoSuchRoleException, NoSuchMetadataObjectException, NoSuchMetalakeException,
          IllegalPrivilegeException {
    Set<Privilege> privilegeSet = Sets.newHashSet(privileges);
    return grantPrivilegesToRole(role, object, privilegeSet);
  }

  /**
   * Grant privileges to a role.
   *
   * @param role The name of the role.
   * @param privileges The privileges to grant.
   * @param object The object is associated with privileges to grant.
   * @return The role after granted.
   * @throws NoSuchRoleException If the role with the given name does not exist.
   * @throws NoSuchMetadataObjectException If the metadata object with the given name does not
   *     exist.
   * @throws NoSuchMetalakeException If the Metalake with the given name does not exist.
   * @throws IllegalPrivilegeException If any privilege can't be bind to the metadata object.
   * @throws RuntimeException If granting privileges to a role encounters storage issues.
   */
  public Role grantPrivilegesToRole(String role, MetadataObject object, Set<Privilege> privileges)
      throws NoSuchRoleException, NoSuchMetadataObjectException, NoSuchMetalakeException,
          IllegalPrivilegeException {
    PrivilegeGrantRequest request =
        new PrivilegeGrantRequest(DTOConverters.toPrivileges(privileges));
    request.validate();

    RoleResponse resp =
        restClient.put(
            String.format(
                API_PERMISSION_PATH,
                RESTUtils.encodeString(this.name()),
                String.format(
                    "roles/%s/%s/%s/grant",
                    RESTUtils.encodeString(role),
                    object.type().name().toLowerCase(Locale.ROOT),
                    RESTUtils.encodeString(object.fullName()))),
            request,
            RoleResponse.class,
            Collections.emptyMap(),
            ErrorHandlers.permissionOperationErrorHandler());

    resp.validate();

    return resp.getRole();
  }

  /**
   * Revoke privileges from a role.
   *
   * @param role The name of the role.
   * @param privileges The privileges to revoke.
   * @param object The object is associated with privileges to revoke.
   * @return The role after revoked.
   * @throws NoSuchRoleException If the role with the given name does not exist.
   * @throws NoSuchMetadataObjectException If the metadata object with the given name does not
   *     exist.
   * @throws NoSuchMetalakeException If the Metalake with the given name does not exist.
   * @throws IllegalPrivilegeException If any privilege can't be bind to the metadata object.
   * @throws RuntimeException If revoking privileges from a role encounters storage issues.
   * @deprecated use {@link #revokePrivilegesFromRole(String, MetadataObject, Set)} instead.
   */
  @Deprecated
  public Role revokePrivilegesFromRole(
      String role, MetadataObject object, List<Privilege> privileges)
      throws NoSuchRoleException, NoSuchMetadataObjectException, NoSuchMetalakeException,
          IllegalPrivilegeException {
    Set<Privilege> privilegeSet = Sets.newHashSet(privileges);
    return revokePrivilegesFromRole(role, object, privilegeSet);
  }

  /**
   * Revoke privileges from a role.
   *
   * @param role The name of the role.
   * @param privileges The privileges to revoke.
   * @param object The object is associated with privileges to revoke.
   * @return The role after revoked.
   * @throws NoSuchRoleException If the role with the given name does not exist.
   * @throws NoSuchMetadataObjectException If the metadata object with the given name does not
   *     exist.
   * @throws NoSuchMetalakeException If the Metalake with the given name does not exist.
   * @throws IllegalPrivilegeException If any privilege can't be bind to the metadata object.
   * @throws RuntimeException If revoking privileges from a role encounters storage issues.
   */
  public Role revokePrivilegesFromRole(
      String role, MetadataObject object, Set<Privilege> privileges)
      throws NoSuchRoleException, NoSuchMetadataObjectException, NoSuchMetalakeException,
          IllegalPrivilegeException {
    PrivilegeRevokeRequest request =
        new PrivilegeRevokeRequest(DTOConverters.toPrivileges(privileges));
    request.validate();

    RoleResponse resp =
        restClient.put(
            String.format(
                API_PERMISSION_PATH,
                RESTUtils.encodeString(this.name()),
                String.format(
                    "roles/%s/%s/%s/revoke",
                    RESTUtils.encodeString(role),
                    object.type().name().toLowerCase(Locale.ROOT),
                    RESTUtils.encodeString(object.fullName()))),
            request,
            RoleResponse.class,
            Collections.emptyMap(),
            ErrorHandlers.permissionOperationErrorHandler());

    resp.validate();

    return resp.getRole();
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
    OwnerResponse resp =
        restClient.get(
            String.format(
                API_METALAKES_OWNERS_PATH,
                RESTUtils.encodeString(this.name()),
                String.format(
                    "%s/%s",
                    object.type().name().toLowerCase(Locale.ROOT),
                    RESTUtils.encodeString(object.fullName()))),
            OwnerResponse.class,
            Collections.emptyMap(),
            ErrorHandlers.ownerErrorHandler());
    resp.validate();
    return Optional.ofNullable(resp.getOwner());
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
    OwnerSetRequest request = new OwnerSetRequest(ownerName, ownerType);
    request.validate();
    SetResponse resp =
        restClient.put(
            String.format(
                API_METALAKES_OWNERS_PATH,
                RESTUtils.encodeString(this.name()),
                String.format(
                    "%s/%s",
                    object.type().name().toLowerCase(Locale.ROOT),
                    RESTUtils.encodeString(object.fullName()))),
            request,
            SetResponse.class,
            Collections.emptyMap(),
            ErrorHandlers.ownerErrorHandler());
    resp.validate();
  }

  @Override
  public String[] listBindingRoleNames() {
    return metadataObjectRoleOperations.listBindingRoleNames();
  }

  static class Builder extends MetalakeDTO.Builder<Builder> {
    private RESTClient restClient;

    private Builder() {
      super();
    }

    Builder withRestClient(RESTClient restClient) {
      this.restClient = restClient;
      return this;
    }

    @Override
    public GravitinoMetalake build() {
      Preconditions.checkNotNull(restClient, "restClient must be set");
      Preconditions.checkArgument(StringUtils.isNotBlank(name), "name must not be null or empty");
      Preconditions.checkArgument(audit != null, "audit must not be null");

      return new GravitinoMetalake(name, comment, properties, audit, restClient);
    }
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }

    if (!(o instanceof GravitinoMetalake)) {
      return false;
    }

    GravitinoMetalake that = (GravitinoMetalake) o;
    return super.equals(that);
  }

  /** @return the builder for creating a new instance of GravitinoMetaLake. */
  public static Builder builder() {
    return new Builder();
  }
}
