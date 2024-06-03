/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.client;

import com.datastrato.gravitino.Catalog;
import com.datastrato.gravitino.NameIdentifier;
import com.datastrato.gravitino.Namespace;
import com.datastrato.gravitino.Schema;
import com.datastrato.gravitino.SchemaChange;
import com.datastrato.gravitino.SupportsSchemas;
import com.datastrato.gravitino.dto.AuditDTO;
import com.datastrato.gravitino.dto.CatalogDTO;
import com.datastrato.gravitino.dto.requests.SchemaCreateRequest;
import com.datastrato.gravitino.dto.requests.SchemaUpdateRequest;
import com.datastrato.gravitino.dto.requests.SchemaUpdatesRequest;
import com.datastrato.gravitino.dto.responses.DropResponse;
import com.datastrato.gravitino.dto.responses.EntityListResponse;
import com.datastrato.gravitino.dto.responses.SchemaResponse;
import com.datastrato.gravitino.exceptions.NoSuchCatalogException;
import com.datastrato.gravitino.exceptions.NoSuchSchemaException;
import com.datastrato.gravitino.exceptions.NonEmptySchemaException;
import com.datastrato.gravitino.exceptions.SchemaAlreadyExistsException;
import com.datastrato.gravitino.rest.RESTUtils;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * BaseSchemaCatalog is the base abstract class for all the catalog with schema. It provides the
 * common methods for managing schemas in a catalog. With {@link BaseSchemaCatalog}, users can list,
 * create, load, alter and drop a schema with specified identifier.
 */
abstract class BaseSchemaCatalog extends CatalogDTO implements Catalog, SupportsSchemas {
  /** The REST client to send the requests. */
  protected final RESTClient restClient;
  /** The namespace of current catalog, which is the metalake name. */
  protected final Namespace namespace;

  /** The namespace of the schemas, which is the metalake name with catalog name. */
  protected final Namespace schemaNamespace;

  BaseSchemaCatalog(
      Namespace namespace,
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
        namespace != null && namespace.length() == 1,
        "Catalog namespace must be non-null and have 1 level, the input namespace is %s",
        namespace);
    this.namespace = namespace;
    this.schemaNamespace = Namespace.of(namespace.level(0), name);
  }

  @Override
  public SupportsSchemas asSchemas() throws UnsupportedOperationException {
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
            formatSchemaRequestPath(schemaNamespace),
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

    SchemaCreateRequest req =
        new SchemaCreateRequest(RESTUtils.encodeString(schemaName), comment, properties);
    req.validate();

    SchemaResponse resp =
        restClient.post(
            formatSchemaRequestPath(schemaNamespace),
            req,
            SchemaResponse.class,
            Collections.emptyMap(),
            ErrorHandlers.schemaErrorHandler());
    resp.validate();

    return resp.getSchema();
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
            formatSchemaRequestPath(schemaNamespace) + "/" + RESTUtils.encodeString(schemaName),
            SchemaResponse.class,
            Collections.emptyMap(),
            ErrorHandlers.schemaErrorHandler());
    resp.validate();

    return resp.getSchema();
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
            formatSchemaRequestPath(schemaNamespace) + "/" + RESTUtils.encodeString(schemaName),
            updatesRequest,
            SchemaResponse.class,
            Collections.emptyMap(),
            ErrorHandlers.schemaErrorHandler());
    resp.validate();

    return resp.getSchema();
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
            formatSchemaRequestPath(schemaNamespace) + "/" + RESTUtils.encodeString(schemaName),
            Collections.singletonMap("cascade", String.valueOf(cascade)),
            DropResponse.class,
            Collections.emptyMap(),
            ErrorHandlers.schemaErrorHandler());
    resp.validate();
    return resp.dropped();
  }

  static String formatSchemaRequestPath(Namespace ns) {
    return new StringBuilder()
        .append("api/metalakes/")
        .append(ns.level(0))
        .append("/catalogs/")
        .append(ns.level(1))
        .append("/schemas")
        .toString();
  }

  /**
   * Check whether the namespace is valid
   *
   * @param namespace The namespace to check
   */
  static void checkNamespace(Namespace namespace) {
    Namespace.check(
        namespace != null && namespace.length() == 3,
        "Table/fileset/topic namespace must be non-null and have 3 level, the input namespace is %s",
        namespace);
  }
}
