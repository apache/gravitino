/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.client;

import com.datastrato.gravitino.NameIdentifier;
import com.datastrato.gravitino.Namespace;
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
import com.datastrato.gravitino.rel.Schema;
import com.datastrato.gravitino.rel.SchemaChange;
import com.google.common.annotations.VisibleForTesting;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * BaseSchemaCatalog is the base abstract class for all the catalog with schema. It provides the
 * common methods for managing schemas in a catalog. With {@link BaseSchemaCatalog}, users can list,
 * create, load, alter and drop a schema with specified identifier.
 */
abstract class BaseSchemaCatalog extends CatalogDTO {
  private static final Logger LOG = LoggerFactory.getLogger(BaseSchemaCatalog.class);

  /** The REST client to send the requests. */
  protected final RESTClient restClient;
  /** The namespace of current catalog, which is the metalake name. */
  protected final Namespace namespace;

  BaseSchemaCatalog(
      Namespace namespace,
      String name,
      Type type,
      String provider,
      String comment,
      Map<String, String> properties,
      AuditDTO auditDTO,
      RESTClient restClient) {
    super(name, type, provider, comment, properties, auditDTO);
    this.restClient = restClient;
    Namespace.checkCatalog(namespace);
    this.namespace = namespace;
  }

  /**
   * List all the schemas under the current catalog.
   *
   * @return A list of {@link NameIdentifier} of the schemas under the given catalog namespace.
   * @throws NoSuchCatalogException if the catalog with specified namespace does not exist.
   */
  public NameIdentifier[] listSchemas() throws NoSuchCatalogException {

    Namespace namespaceForSchema = Namespace.ofSchema(namespace.level(0), this.name());
    EntityListResponse resp =
        restClient.get(
            formatSchemaRequestPath(namespaceForSchema),
            EntityListResponse.class,
            Collections.emptyMap(),
            ErrorHandlers.schemaErrorHandler());
    resp.validate();

    return resp.identifiers();
  }

  /**
   * Create a new schema with specified name, comment and metadata.
   *
   * @param schemaName The name of the schema.
   * @param comment The comment of the schema.
   * @param properties The properties of the schema.
   * @return The created {@link Schema}.
   * @throws NoSuchCatalogException if the catalog with specified namespace does not exist.
   * @throws SchemaAlreadyExistsException if the schema with specified identifier already exists.
   */
  public Schema createSchema(String schemaName, String comment, Map<String, String> properties)
      throws NoSuchCatalogException, SchemaAlreadyExistsException {
    NameIdentifier ident = NameIdentifier.ofSchema(namespace.level(0), this.name(), schemaName);
    NameIdentifier.checkSchema(ident);
    SchemaCreateRequest req = new SchemaCreateRequest(ident.name(), comment, properties);
    req.validate();

    SchemaResponse resp =
        restClient.post(
            formatSchemaRequestPath(ident.namespace()),
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
   * @param schemaName The name of the schema.
   * @return The {@link Schema} with specified identifier.
   * @throws NoSuchSchemaException if the schema with specified identifier does not exist.
   */
  public Schema loadSchema(String schemaName) throws NoSuchSchemaException {

    NameIdentifier ident = NameIdentifier.ofSchema(namespace.level(0), this.name(), schemaName);
    NameIdentifier.checkSchema(ident);

    SchemaResponse resp =
        restClient.get(
            formatSchemaRequestPath(ident.namespace()) + "/" + ident.name(),
            SchemaResponse.class,
            Collections.emptyMap(),
            ErrorHandlers.schemaErrorHandler());
    resp.validate();

    return resp.getSchema();
  }

  /**
   * Alter the schema with specified schema name by applying the changes.
   *
   * @param schemaName The name of the schema.
   * @param changes The metadata changes to apply.
   * @return The altered {@link Schema}.
   * @throws NoSuchSchemaException if the schema with specified identifier does not exist.
   */
  public Schema alterSchema(String schemaName, SchemaChange... changes)
      throws NoSuchSchemaException {
    NameIdentifier ident = NameIdentifier.ofSchema(namespace.level(0), this.name(), schemaName);
    NameIdentifier.checkSchema(ident);

    List<SchemaUpdateRequest> reqs =
        Arrays.stream(changes)
            .map(DTOConverters::toSchemaUpdateRequest)
            .collect(Collectors.toList());
    SchemaUpdatesRequest updatesRequest = new SchemaUpdatesRequest(reqs);
    updatesRequest.validate();

    SchemaResponse resp =
        restClient.put(
            formatSchemaRequestPath(ident.namespace()) + "/" + ident.name(),
            updatesRequest,
            SchemaResponse.class,
            Collections.emptyMap(),
            ErrorHandlers.schemaErrorHandler());
    resp.validate();

    return resp.getSchema();
  }

  /**
   * Drop the schema with specified name.
   *
   * @param schemaName The name of the schema.
   * @param cascade Whether to drop all the tables under the schema.
   * @return true if the schema is dropped successfully, false otherwise.
   * @throws NonEmptySchemaException if the schema is not empty and cascade is false.
   */
  public boolean dropSchema(String schemaName, boolean cascade) throws NonEmptySchemaException {
    NameIdentifier ident = NameIdentifier.ofSchema(namespace.level(0), this.name(), schemaName);
    NameIdentifier.checkSchema(ident);

    try {
      DropResponse resp =
          restClient.delete(
              formatSchemaRequestPath(ident.namespace()) + "/" + ident.name(),
              Collections.singletonMap("cascade", String.valueOf(cascade)),
              DropResponse.class,
              Collections.emptyMap(),
              ErrorHandlers.schemaErrorHandler());
      resp.validate();
      return resp.dropped();

    } catch (NonEmptySchemaException e) {
      throw e;
    } catch (Exception e) {
      LOG.warn("Failed to drop schema {}", ident, e);
      return false;
    }
  }

  @VisibleForTesting
  static String formatSchemaRequestPath(Namespace ns) {
    return new StringBuilder()
        .append("api/metalakes/")
        .append(ns.level(0))
        .append("/catalogs/")
        .append(ns.level(1))
        .append("/schemas")
        .toString();
  }
}
