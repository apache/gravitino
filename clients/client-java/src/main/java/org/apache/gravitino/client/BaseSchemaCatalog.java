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

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.MetadataObjects;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.Schema;
import org.apache.gravitino.SchemaChange;
import org.apache.gravitino.SupportsSchemas;
import org.apache.gravitino.authorization.SupportsRoles;
import org.apache.gravitino.dto.AuditDTO;
import org.apache.gravitino.dto.CatalogDTO;
import org.apache.gravitino.dto.requests.SchemaCreateRequest;
import org.apache.gravitino.dto.requests.SchemaUpdateRequest;
import org.apache.gravitino.dto.requests.SchemaUpdatesRequest;
import org.apache.gravitino.dto.responses.DropResponse;
import org.apache.gravitino.dto.responses.EntityListResponse;
import org.apache.gravitino.dto.responses.SchemaResponse;
import org.apache.gravitino.exceptions.NoSuchCatalogException;
import org.apache.gravitino.exceptions.NoSuchPolicyException;
import org.apache.gravitino.exceptions.NoSuchSchemaException;
import org.apache.gravitino.exceptions.NonEmptySchemaException;
import org.apache.gravitino.exceptions.PolicyAlreadyAssociatedException;
import org.apache.gravitino.exceptions.SchemaAlreadyExistsException;
import org.apache.gravitino.policy.Policy;
import org.apache.gravitino.policy.SupportsPolicies;
import org.apache.gravitino.rest.RESTUtils;
import org.apache.gravitino.tag.SupportsTags;
import org.apache.gravitino.tag.Tag;

/**
 * BaseSchemaCatalog is the base abstract class for all the catalog with schema. It provides the
 * common methods for managing schemas in a catalog. With {@link BaseSchemaCatalog}, users can list,
 * create, load, alter and drop a schema with specified identifier.
 */
abstract class BaseSchemaCatalog extends CatalogDTO
    implements Catalog, SupportsSchemas, SupportsTags, SupportsRoles, SupportsPolicies {

  /** The REST client to send the requests. */
  protected final RESTClient restClient;

  /** The namespace of current catalog, which is the metalake name. */
  private final Namespace catalogNamespace;

  private final MetadataObjectTagOperations objectTagOperations;
  private final MetadataObjectPolicyOperations objectPolicyOperations;
  private final MetadataObjectRoleOperations objectRoleOperations;
  protected final MetadataObjectCredentialOperations objectCredentialOperations;

  BaseSchemaCatalog(
      Namespace catalogNamespace,
      String name,
      Catalog.Type type,
      String provider,
      String comment,
      Map<String, String> properties,
      AuditDTO auditDTO,
      RESTClient restClient) {
    super(name, type, provider, comment, properties, auditDTO);

    this.restClient = restClient;
    Namespace.check(
        catalogNamespace != null && catalogNamespace.length() == 1,
        "Catalog namespace must be non-null and have 1 level, the input namespace is %s",
        catalogNamespace);
    this.catalogNamespace = catalogNamespace;

    MetadataObject metadataObject =
        MetadataObjects.of(null, this.name(), MetadataObject.Type.CATALOG);
    this.objectTagOperations =
        new MetadataObjectTagOperations(catalogNamespace.level(0), metadataObject, restClient);
    this.objectPolicyOperations =
        new MetadataObjectPolicyOperations(catalogNamespace.level(0), metadataObject, restClient);
    this.objectRoleOperations =
        new MetadataObjectRoleOperations(catalogNamespace.level(0), metadataObject, restClient);
    this.objectCredentialOperations =
        new MetadataObjectCredentialOperations(
            catalogNamespace.level(0), metadataObject, restClient);
  }

  @Override
  public SupportsSchemas asSchemas() throws UnsupportedOperationException {
    return this;
  }

  @Override
  public SupportsTags supportsTags() throws UnsupportedOperationException {
    return this;
  }

  @Override
  public SupportsPolicies supportsPolicies() throws UnsupportedOperationException {
    return this;
  }

  @Override
  public SupportsRoles supportsRoles() throws UnsupportedOperationException {
    return this;
  }

  /**
   * List all the schemas under the given catalog namespace.
   *
   * @return A list of the schema names under the given catalog namespace.
   * @throws NoSuchCatalogException if the catalog with specified namespace does not exist.
   */
  @Override
  public String[] listSchemas() throws NoSuchCatalogException {

    EntityListResponse resp =
        restClient.get(
            formatSchemaRequestPath(schemaNamespace()),
            EntityListResponse.class,
            Collections.emptyMap(),
            ErrorHandlers.schemaErrorHandler());
    resp.validate();

    return Arrays.stream(resp.identifiers()).map(NameIdentifier::name).toArray(String[]::new);
  }

  /**
   * Create a new schema with specified identifier, comment and metadata.
   *
   * @param schemaName The name identifier of the schema.
   * @param comment The comment of the schema.
   * @param properties The properties of the schema.
   * @return The created {@link Schema}.
   * @throws NoSuchCatalogException if the catalog with specified namespace does not exist.
   * @throws SchemaAlreadyExistsException if the schema with specified identifier already exists.
   */
  @Override
  public Schema createSchema(String schemaName, String comment, Map<String, String> properties)
      throws NoSuchCatalogException, SchemaAlreadyExistsException {

    SchemaCreateRequest req = new SchemaCreateRequest(schemaName, comment, properties);
    req.validate();

    SchemaResponse resp =
        restClient.post(
            formatSchemaRequestPath(schemaNamespace()),
            req,
            SchemaResponse.class,
            Collections.emptyMap(),
            ErrorHandlers.schemaErrorHandler());
    resp.validate();

    return new GenericSchema(resp.getSchema(), restClient, catalogNamespace.level(0), this.name());
  }

  /**
   * Load the schema with specified identifier.
   *
   * @param schemaName The name identifier of the schema.
   * @return The {@link Schema} with specified identifier.
   * @throws NoSuchSchemaException if the schema with specified identifier does not exist.
   */
  @Override
  public Schema loadSchema(String schemaName) throws NoSuchSchemaException {

    SchemaResponse resp =
        restClient.get(
            formatSchemaRequestPath(schemaNamespace()) + "/" + RESTUtils.encodeString(schemaName),
            SchemaResponse.class,
            Collections.emptyMap(),
            ErrorHandlers.schemaErrorHandler());
    resp.validate();

    return new GenericSchema(resp.getSchema(), restClient, catalogNamespace.level(0), this.name());
  }

  /**
   * Alter the schema with specified identifier by applying the changes.
   *
   * @param schemaName The name identifier of the schema.
   * @param changes The metadata changes to apply.
   * @return The altered {@link Schema}.
   * @throws NoSuchSchemaException if the schema with specified identifier does not exist.
   */
  @Override
  public Schema alterSchema(String schemaName, SchemaChange... changes)
      throws NoSuchSchemaException {

    List<SchemaUpdateRequest> reqs =
        Arrays.stream(changes)
            .map(DTOConverters::toSchemaUpdateRequest)
            .collect(Collectors.toList());
    SchemaUpdatesRequest updatesRequest = new SchemaUpdatesRequest(reqs);
    updatesRequest.validate();

    SchemaResponse resp =
        restClient.put(
            formatSchemaRequestPath(schemaNamespace()) + "/" + RESTUtils.encodeString(schemaName),
            updatesRequest,
            SchemaResponse.class,
            Collections.emptyMap(),
            ErrorHandlers.schemaErrorHandler());
    resp.validate();

    return new GenericSchema(resp.getSchema(), restClient, catalogNamespace.level(0), this.name());
  }

  /**
   * Drop the schema with specified identifier.
   *
   * @param schemaName The name identifier of the schema.
   * @param cascade Whether to drop all the tables under the schema.
   * @return true if the schema is dropped successfully, false otherwise.
   * @throws NonEmptySchemaException if the schema is not empty and cascade is false.
   */
  @Override
  public boolean dropSchema(String schemaName, boolean cascade) throws NonEmptySchemaException {
    DropResponse resp =
        restClient.delete(
            formatSchemaRequestPath(schemaNamespace()) + "/" + RESTUtils.encodeString(schemaName),
            Collections.singletonMap("cascade", String.valueOf(cascade)),
            DropResponse.class,
            Collections.emptyMap(),
            ErrorHandlers.schemaErrorHandler());
    resp.validate();
    return resp.dropped();
  }

  @Override
  public String[] listTags() {
    return objectTagOperations.listTags();
  }

  @Override
  public Tag[] listTagsInfo() {
    return objectTagOperations.listTagsInfo();
  }

  @Override
  public Tag getTag(String name) {
    return objectTagOperations.getTag(name);
  }

  @Override
  public String[] associateTags(String[] tagsToAdd, String[] tagsToRemove) {
    return objectTagOperations.associateTags(tagsToAdd, tagsToRemove);
  }

  @Override
  public String[] listPolicies() {
    return objectPolicyOperations.listPolicies();
  }

  @Override
  public Policy[] listPolicyInfos() {
    return objectPolicyOperations.listPolicyInfos();
  }

  @Override
  public Policy getPolicy(String name) throws NoSuchPolicyException {
    return objectPolicyOperations.getPolicy(name);
  }

  @Override
  public String[] associatePolicies(String[] policiesToAdd, String[] policiesToRemove)
      throws PolicyAlreadyAssociatedException {
    return objectPolicyOperations.associatePolicies(policiesToAdd, policiesToRemove);
  }

  @Override
  public String[] listBindingRoleNames() {
    return objectRoleOperations.listBindingRoleNames();
  }

  /**
   * Get the namespace of the current catalog, which is "metalake".
   *
   * @return The namespace of the current catalog.
   */
  protected Namespace catalogNamespace() {
    return catalogNamespace;
  }

  /**
   * Get the namespace of the schemas, which is "metalake.catalog".
   *
   * @return The namespace of the schemas in this catalog.
   */
  protected Namespace schemaNamespace() {
    return Namespace.of(catalogNamespace.level(0), this.name());
  }

  static String formatSchemaRequestPath(Namespace ns) {
    return new StringBuilder()
        .append("api/metalakes/")
        .append(RESTUtils.encodeString(ns.level(0)))
        .append("/catalogs/")
        .append(RESTUtils.encodeString(ns.level(1)))
        .append("/schemas")
        .toString();
  }
}
