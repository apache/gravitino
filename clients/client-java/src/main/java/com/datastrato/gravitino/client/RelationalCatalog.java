/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.client;

import static com.datastrato.gravitino.dto.util.DTOConverters.toDTO;
import static com.datastrato.gravitino.dto.util.DTOConverters.toDTOs;

import com.datastrato.gravitino.NameIdentifier;
import com.datastrato.gravitino.Namespace;
import com.datastrato.gravitino.dto.AuditDTO;
import com.datastrato.gravitino.dto.CatalogDTO;
import com.datastrato.gravitino.dto.requests.SchemaCreateRequest;
import com.datastrato.gravitino.dto.requests.SchemaUpdateRequest;
import com.datastrato.gravitino.dto.requests.SchemaUpdatesRequest;
import com.datastrato.gravitino.dto.requests.TableCreateRequest;
import com.datastrato.gravitino.dto.requests.TableUpdateRequest;
import com.datastrato.gravitino.dto.requests.TableUpdatesRequest;
import com.datastrato.gravitino.dto.responses.DropResponse;
import com.datastrato.gravitino.dto.responses.EntityListResponse;
import com.datastrato.gravitino.dto.responses.SchemaResponse;
import com.datastrato.gravitino.dto.responses.TableResponse;
import com.datastrato.gravitino.exceptions.NoSuchCatalogException;
import com.datastrato.gravitino.exceptions.NoSuchSchemaException;
import com.datastrato.gravitino.exceptions.NoSuchTableException;
import com.datastrato.gravitino.exceptions.NonEmptySchemaException;
import com.datastrato.gravitino.exceptions.SchemaAlreadyExistsException;
import com.datastrato.gravitino.exceptions.TableAlreadyExistsException;
import com.datastrato.gravitino.rel.Column;
import com.datastrato.gravitino.rel.Schema;
import com.datastrato.gravitino.rel.SchemaChange;
import com.datastrato.gravitino.rel.SupportsSchemas;
import com.datastrato.gravitino.rel.Table;
import com.datastrato.gravitino.rel.TableCatalog;
import com.datastrato.gravitino.rel.TableChange;
import com.datastrato.gravitino.rel.expressions.distributions.Distribution;
import com.datastrato.gravitino.rel.expressions.sorts.SortOrder;
import com.datastrato.gravitino.rel.expressions.transforms.Transform;
import com.datastrato.gravitino.rel.indexes.Index;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Relational catalog is a catalog implementation that supports relational database like metadata
 * operations, for example, schemas and tables list, creation, update and deletion. A Relational
 * catalog is under the metalake.
 */
public class RelationalCatalog extends CatalogDTO implements TableCatalog, SupportsSchemas {

  private static final Logger LOG = LoggerFactory.getLogger(RelationalCatalog.class);

  private final RESTClient restClient;

  RelationalCatalog(
      String name,
      Type type,
      String provider,
      String comment,
      Map<String, String> properties,
      AuditDTO auditDTO,
      RESTClient restClient) {
    super(name, type, provider, comment, properties, auditDTO);
    this.restClient = restClient;
  }

  @Override
  public SupportsSchemas asSchemas() {
    return this;
  }

  @Override
  public TableCatalog asTableCatalog() {
    return this;
  }

  /**
   * List all the tables under the given Schema namespace.
   *
   * @param namespace The namespace to list the tables under it.
   * @return A list of {@link NameIdentifier} of the tables under the given namespace.
   * @throws NoSuchSchemaException if the schema with specified namespace does not exist.
   */
  @Override
  public NameIdentifier[] listTables(Namespace namespace) throws NoSuchSchemaException {
    Namespace.checkTable(namespace);

    EntityListResponse resp =
        restClient.get(
            formatTableRequestPath(namespace),
            EntityListResponse.class,
            Collections.emptyMap(),
            ErrorHandlers.tableErrorHandler());
    resp.validate();

    return resp.identifiers();
  }

  /**
   * Load the table with specified identifier.
   *
   * @param ident The identifier of the table to load.
   * @return The {@link Table} with specified identifier.
   * @throws NoSuchTableException if the table with specified identifier does not exist.
   */
  @Override
  public Table loadTable(NameIdentifier ident) throws NoSuchTableException {
    NameIdentifier.checkTable(ident);

    TableResponse resp =
        restClient.get(
            formatTableRequestPath(ident.namespace()) + "/" + ident.name(),
            TableResponse.class,
            Collections.emptyMap(),
            ErrorHandlers.tableErrorHandler());
    resp.validate();

    return RelationalTable.from(ident.namespace(), resp.getTable(), restClient);
  }

  /**
   * Create a new table with specified identifier, columns, comment and properties.
   *
   * @param ident The identifier of the table.
   * @param columns The columns of the table.
   * @param comment The comment of the table.
   * @param properties The properties of the table.
   * @param partitioning The partitioning of the table.
   * @param indexes The indexes of the table.
   * @return The created {@link Table}.
   * @throws NoSuchSchemaException if the schema with specified namespace does not exist.
   * @throws TableAlreadyExistsException if the table with specified identifier already exists.
   */
  @Override
  public Table createTable(
      NameIdentifier ident,
      Column[] columns,
      String comment,
      Map<String, String> properties,
      Transform[] partitioning,
      Distribution distribution,
      SortOrder[] sortOrders,
      Index[] indexes)
      throws NoSuchSchemaException, TableAlreadyExistsException {
    NameIdentifier.checkTable(ident);

    TableCreateRequest req =
        new TableCreateRequest(
            ident.name(),
            comment,
            toDTOs(columns),
            properties,
            toDTOs(sortOrders),
            toDTO(distribution),
            toDTOs(partitioning),
            toDTOs(indexes));
    req.validate();

    TableResponse resp =
        restClient.post(
            formatTableRequestPath(ident.namespace()),
            req,
            TableResponse.class,
            Collections.emptyMap(),
            ErrorHandlers.tableErrorHandler());
    resp.validate();

    return RelationalTable.from(ident.namespace(), resp.getTable(), restClient);
  }

  /**
   * Alter the table with specified identifier by applying the changes.
   *
   * @param ident The identifier of the table.
   * @param changes Table changes to apply to the table.
   * @return The altered {@link Table}.
   * @throws NoSuchTableException if the table with specified identifier does not exist.
   * @throws IllegalArgumentException if the changes are invalid.
   */
  @Override
  public Table alterTable(NameIdentifier ident, TableChange... changes)
      throws NoSuchTableException, IllegalArgumentException {
    NameIdentifier.checkTable(ident);

    List<TableUpdateRequest> reqs =
        Arrays.stream(changes)
            .map(DTOConverters::toTableUpdateRequest)
            .collect(Collectors.toList());
    TableUpdatesRequest updatesRequest = new TableUpdatesRequest(reqs);
    updatesRequest.validate();

    TableResponse resp =
        restClient.put(
            formatTableRequestPath(ident.namespace()) + "/" + ident.name(),
            updatesRequest,
            TableResponse.class,
            Collections.emptyMap(),
            ErrorHandlers.tableErrorHandler());
    resp.validate();

    return RelationalTable.from(ident.namespace(), resp.getTable(), restClient);
  }

  /**
   * Drop the table with specified identifier.
   *
   * @param ident The identifier of the table.
   * @return true if the table is dropped successfully, false otherwise.
   */
  @Override
  public boolean dropTable(NameIdentifier ident) {
    NameIdentifier.checkTable(ident);

    try {
      DropResponse resp =
          restClient.delete(
              formatTableRequestPath(ident.namespace()) + "/" + ident.name(),
              DropResponse.class,
              Collections.emptyMap(),
              ErrorHandlers.tableErrorHandler());
      resp.validate();
      return resp.dropped();

    } catch (Exception e) {
      LOG.warn("Failed to drop table {}", ident, e);
      return false;
    }
  }

  /**
   * Purge the table with specified identifier.
   *
   * @param ident The identifier of the table.
   * @return true if the table is purged successfully, false otherwise.
   */
  @Override
  public boolean purgeTable(NameIdentifier ident) throws UnsupportedOperationException {
    NameIdentifier.checkTable(ident);

    Map<String, String> params = new HashMap<>();
    params.put("purge", "true");
    try {
      DropResponse resp =
          restClient.delete(
              formatTableRequestPath(ident.namespace()) + "/" + ident.name(),
              params,
              DropResponse.class,
              Collections.emptyMap(),
              ErrorHandlers.tableErrorHandler());
      resp.validate();
      return resp.dropped();

    } catch (UnsupportedOperationException e) {
      throw e;
    } catch (Exception e) {
      LOG.warn("Failed to purge table {}", ident, e);
      return false;
    }
  }

  /**
   * List all the schemas under the given catalog namespace.
   *
   * @param namespace The namespace of the catalog.
   * @return A list of {@link NameIdentifier} of the schemas under the given catalog namespace.
   * @throws NoSuchCatalogException if the catalog with specified namespace does not exist.
   */
  @Override
  public NameIdentifier[] listSchemas(Namespace namespace) throws NoSuchCatalogException {
    Namespace.checkSchema(namespace);

    EntityListResponse resp =
        restClient.get(
            formatSchemaRequestPath(namespace),
            EntityListResponse.class,
            Collections.emptyMap(),
            ErrorHandlers.schemaErrorHandler());
    resp.validate();

    return resp.identifiers();
  }

  /**
   * Create a new schema with specified identifier, comment and metadata.
   *
   * @param ident The name identifier of the schema.
   * @param comment The comment of the schema.
   * @param properties The properties of the schema.
   * @return The created {@link Schema}.
   * @throws NoSuchCatalogException if the catalog with specified namespace does not exist.
   * @throws SchemaAlreadyExistsException if the schema with specified identifier already exists.
   */
  @Override
  public Schema createSchema(NameIdentifier ident, String comment, Map<String, String> properties)
      throws NoSuchCatalogException, SchemaAlreadyExistsException {
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
   * @param ident The name identifier of the schema.
   * @return The {@link Schema} with specified identifier.
   * @throws NoSuchSchemaException if the schema with specified identifier does not exist.
   */
  @Override
  public Schema loadSchema(NameIdentifier ident) throws NoSuchSchemaException {
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
   * Alter the schema with specified identifier by applying the changes.
   *
   * @param ident The name identifier of the schema.
   * @param changes The metadata changes to apply.
   * @return The altered {@link Schema}.
   * @throws NoSuchSchemaException if the schema with specified identifier does not exist.
   */
  @Override
  public Schema alterSchema(NameIdentifier ident, SchemaChange... changes)
      throws NoSuchSchemaException {
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
   * Drop the schema with specified identifier.
   *
   * @param ident The name identifier of the schema.
   * @param cascade Whether to drop all the tables under the schema.
   * @return true if the schema is dropped successfully, false otherwise.
   * @throws NonEmptySchemaException if the schema is not empty and cascade is false.
   */
  @Override
  public boolean dropSchema(NameIdentifier ident, boolean cascade) throws NonEmptySchemaException {
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
  static String formatTableRequestPath(Namespace ns) {
    Namespace schemaNs = Namespace.of(ns.level(0), ns.level(1));
    return new StringBuilder()
        .append(formatSchemaRequestPath(schemaNs))
        .append("/")
        .append(ns.level(2))
        .append("/tables")
        .toString();
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

  static class Builder extends CatalogDTO.Builder<Builder> {
    private RESTClient restClient;

    protected Builder() {}

    Builder withRestClient(RESTClient restClient) {
      this.restClient = restClient;
      return this;
    }

    @Override
    public RelationalCatalog build() {
      Preconditions.checkArgument(restClient != null, "restClient must be set");
      Preconditions.checkArgument(StringUtils.isNotBlank(name), "name must not be null or empty");
      Preconditions.checkArgument(type != null, "type must not be null");
      Preconditions.checkArgument(
          StringUtils.isNotBlank(provider), "provider must not be null or empty");
      Preconditions.checkArgument(audit != null, "audit must not be null");

      return new RelationalCatalog(name, type, provider, comment, properties, audit, restClient);
    }
  }

  /** @return the builder for creating a new instance of RelationalCatalog. */
  public static Builder builder() {
    return new Builder();
  }
}
