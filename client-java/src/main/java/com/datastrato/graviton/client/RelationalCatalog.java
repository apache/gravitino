/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.graviton.client;

import com.datastrato.graviton.NameIdentifier;
import com.datastrato.graviton.Namespace;
import com.datastrato.graviton.dto.AuditDTO;
import com.datastrato.graviton.dto.CatalogDTO;
import com.datastrato.graviton.exceptions.NoSuchCatalogException;
import com.datastrato.graviton.exceptions.NoSuchSchemaException;
import com.datastrato.graviton.exceptions.NoSuchTableException;
import com.datastrato.graviton.exceptions.NonEmptySchemaException;
import com.datastrato.graviton.exceptions.SchemaAlreadyExistsException;
import com.datastrato.graviton.exceptions.TableAlreadyExistsException;
import com.datastrato.graviton.rel.Column;
import com.datastrato.graviton.rel.Schema;
import com.datastrato.graviton.rel.SchemaChange;
import com.datastrato.graviton.rel.SupportsSchemas;
import com.datastrato.graviton.rel.Table;
import com.datastrato.graviton.rel.TableCatalog;
import com.datastrato.graviton.rel.TableChange;
import com.google.common.base.Preconditions;
import java.util.Map;

public class RelationalCatalog extends CatalogDTO implements TableCatalog, SupportsSchemas {

  private final RESTClient restClient;

  RelationalCatalog(
      String name,
      Type type,
      String comment,
      Map<String, String> properties,
      AuditDTO auditDTO,
      RESTClient restClient) {
    super(name, type, comment, properties, auditDTO);
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

  @Override
  public NameIdentifier[] listTables(Namespace namespace) throws NoSuchCatalogException {
    throw new UnsupportedOperationException("Not implemented yet");
  }

  @Override
  public Table loadTable(NameIdentifier ident) throws NoSuchTableException {
    throw new UnsupportedOperationException("Not implemented yet");
  }

  @Override
  public Table createTable(
      NameIdentifier ident, Column[] columns, String comment, Map<String, String> properties)
      throws NoSuchSchemaException, TableAlreadyExistsException {
    throw new UnsupportedOperationException("Not implemented yet");
  }

  @Override
  public Table alterTable(NameIdentifier ident, TableChange... changes)
      throws NoSuchTableException, IllegalArgumentException {
    throw new UnsupportedOperationException("Not implemented yet");
  }

  @Override
  public boolean dropTable(NameIdentifier ident) {
    throw new UnsupportedOperationException("Not implemented yet");
  }

  @Override
  public NameIdentifier[] listSchemas(Namespace namespace) throws NoSuchCatalogException {
    throw new UnsupportedOperationException("Not implemented yet");
  }

  @Override
  public Schema createSchema(NameIdentifier ident, String comment, Map<String, String> metadata)
      throws NoSuchCatalogException, SchemaAlreadyExistsException {
    throw new UnsupportedOperationException("Not implemented yet");
  }

  @Override
  public Schema loadSchema(NameIdentifier ident) throws NoSuchSchemaException {
    throw new UnsupportedOperationException("Not implemented yet");
  }

  @Override
  public Schema alterSchema(NameIdentifier ident, SchemaChange... changes)
      throws NoSuchSchemaException {
    throw new UnsupportedOperationException("Not implemented yet");
  }

  @Override
  public boolean dropSchema(NameIdentifier ident, boolean cascade) throws NonEmptySchemaException {
    throw new UnsupportedOperationException("Not implemented yet");
  }

  static class Builder extends CatalogDTO.Builder<Builder> {
    private RESTClient restClient;

    Builder withRestClient(RESTClient restClient) {
      this.restClient = restClient;
      return this;
    }

    @Override
    public RelationalCatalog build() {
      Preconditions.checkArgument(restClient != null, "restClient must be set");
      Preconditions.checkArgument(
          name != null && !name.isEmpty(), "name must not be null or empty");
      Preconditions.checkArgument(type != null, "type must not be null");
      Preconditions.checkArgument(audit != null, "audit must not be null");

      return new RelationalCatalog(name, type, comment, properties, audit, restClient);
    }
  }
}
